package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

const serverAddress = "localhost:1234"

type JoinArgs struct {
	Username  string
	ClientURL string
}

type MessageArgs struct {
	Username string
	Message  string
}

type UserArgs struct {
	Username string
}

type EmptyArgs struct{}

type Reply struct {
	OK      bool
	Message string
}

type CallbackArgs struct {
	Message string
}

type ClientCallback struct{}

func (c *ClientCallback) Notify(args *CallbackArgs, reply *Reply) error {
	fmt.Printf("\n[%s]\n> ", args.Message)

	reply.OK = true
	reply.Message = "OK"

	return nil
}

var (
	activeUsersMu sync.Mutex
	activeUsers   = make(map[string]bool)
)

func addActiveUser(username string) {
	activeUsersMu.Lock()
	defer activeUsersMu.Unlock()
	activeUsers[username] = true
}

func removeActiveUser(username string) {
	activeUsersMu.Lock()
	defer activeUsersMu.Unlock()
	delete(activeUsers, username)
}

func leaveAllUsers(rpcClient *rpc.Client) {
	activeUsersMu.Lock()
	users := make([]string, 0, len(activeUsers))
	for username := range activeUsers {
		users = append(users, username)
	}
	activeUsers = make(map[string]bool)
	activeUsersMu.Unlock()

	for _, username := range users {
		var reply Reply
		_ = rpcClient.Call(
			"ChatServer.Leave",
			&UserArgs{Username: username},
			&reply,
		)
	}
}

func printMenu() {
	fmt.Println("===================================")
	fmt.Println("       Concurrent Chat Client")
	fmt.Println("===================================")
	fmt.Println("new <username>  - Create a new user")
	fmt.Println("list            - List connected users")
	fmt.Println("use <username>  - Select active user")
	fmt.Println("send <message>  - Send a message")
	fmt.Println("remove <user>   - Remove a user")
	fmt.Println("help            - Show this menu")
	fmt.Println("exit            - Exit the program")
	fmt.Println("===================================")
}

func getClientAddress() (net.Listener, string) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}

	port := listener.Addr().(*net.TCPAddr).Port
	address := "localhost:" + strconv.Itoa(port)

	return listener, address
}

func main() {
	rpcClient, err := rpc.Dial("tcp", serverAddress)
	if err != nil {
		log.Fatalf(
			"Could not connect to server at %s: %v",
			serverAddress,
			err,
		)
	}
	defer rpcClient.Close()

	callbackServer := &ClientCallback{}

	if err := rpc.RegisterName("ClientCallback", callbackServer); err != nil {
		log.Fatal(err)
	}

	listener, clientAddress := getClientAddress()
	defer listener.Close()

	go rpc.Accept(listener)

	fmt.Println("Connected to chat server.")
	fmt.Printf("Client callback listening on %s\n", clientAddress)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(
		signalChan,
		os.Interrupt,
		syscall.SIGTERM,
	)

	go func() {
		<-signalChan
		fmt.Println("\nSignal received, leaving chat...")
		leaveAllUsers(rpcClient)
		listener.Close()
		rpcClient.Close()
		os.Exit(0)
	}()

	scanner := bufio.NewScanner(os.Stdin)

	var selectedUser string

	printMenu()

	for {
		fmt.Print("\n> ")

		if !scanner.Scan() {
			leaveAllUsers(rpcClient)

			if err := scanner.Err(); err != nil {
				fmt.Println("Input error:", err)
			}

			return
		}

		input := strings.TrimSpace(scanner.Text())

		if input == "" {
			continue
		}

		parts := strings.Fields(input)

		switch parts[0] {
		case "new":
			if len(parts) != 2 {
				fmt.Println("Usage: new <username>")
				continue
			}

			username := parts[1]
			var reply Reply

			err := rpcClient.Call(
				"ChatServer.Join",
				&JoinArgs{
					Username:  username,
					ClientURL: clientAddress,
				},
				&reply,
			)

			if err != nil {
				fmt.Println("RPC error:", err)
				continue
			}

			if reply.OK {
				addActiveUser(username)

				fmt.Printf(
					"User %s joined successfully.\n",
					username,
				)
			} else {
				fmt.Println(reply.Message)
			}

		case "list":
			var reply Reply

			err := rpcClient.Call(
				"ChatServer.List",
				&EmptyArgs{},
				&reply,
			)

			if err != nil {
				fmt.Println("RPC error:", err)
				continue
			}

			fmt.Println(reply.Message)

		case "use":
			if len(parts) != 2 {
				fmt.Println("Usage: use <username>")
				continue
			}

			username := parts[1]

			var reply Reply

			err := rpcClient.Call(
				"ChatServer.List",
				&EmptyArgs{},
				&reply,
			)

			if err != nil {
				fmt.Println("RPC error:", err)
				continue
			}

			if reply.Message == "No users are currently connected." {
				fmt.Println(reply.Message)
				continue
			}

			found := false
			userList := strings.TrimPrefix(
				reply.Message,
				"Connected users: ",
			)

			for _, user := range strings.Split(userList, ", ") {
				if user == username {
					found = true
					break
				}
			}

			if !found {
				fmt.Printf(
					"ERROR: User %s does not exist.\n",
					username,
				)
				continue
			}

			selectedUser = username

			fmt.Printf(
				"Now acting as %s.\n",
				username,
			)

		case "send":
			if selectedUser == "" {
				fmt.Println(
					"ERROR: Select a user first using: use <username>",
				)
				continue
			}

			if len(parts) < 2 {
				fmt.Println("Usage: send <message>")
				continue
			}

			message := strings.TrimSpace(
				strings.TrimPrefix(input, "send"),
			)

			var reply Reply

			err := rpcClient.Call(
				"ChatServer.SendMessage",
				&MessageArgs{
					Username: selectedUser,
					Message:  message,
				},
				&reply,
			)

			if err != nil {
				fmt.Println("RPC error:", err)
				continue
			}

			if !reply.OK {
				fmt.Println(reply.Message)
			}

		case "remove":
			if len(parts) != 2 {
				fmt.Println("Usage: remove <username>")
				continue
			}

			username := parts[1]
			var reply Reply

			err := rpcClient.Call(
				"ChatServer.Leave",
				&UserArgs{Username: username},
				&reply,
			)

			if err != nil {
				fmt.Println("RPC error:", err)
				continue
			}

			if reply.OK {
				fmt.Printf(
					"User %s removed.\n",
					username,
				)

				removeActiveUser(username)

				if selectedUser == username {
					selectedUser = ""
				}
			} else {
				fmt.Println(reply.Message)
			}

		case "help":
			printMenu()

		case "exit":
			leaveAllUsers(rpcClient)
			fmt.Println("Client exited.")
			return

		default:
			fmt.Println(
				"Unknown command. Type 'help' for available commands.",
			)
		}
	}
}
