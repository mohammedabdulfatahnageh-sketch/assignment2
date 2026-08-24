package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

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

type ClientInfo struct {
	Username  string
	ClientURL string
	RPC       *rpc.Client
}

type CallbackArgs struct {
	Message string
}

type ChatServer struct {
	mu      sync.Mutex
	clients map[string]*ClientInfo
}

func NewChatServer() *ChatServer {
	return &ChatServer{
		clients: make(map[string]*ClientInfo),
	}
}

func (s *ChatServer) Join(args *JoinArgs, reply *Reply) error {
	// Fast check first so we don't dial out for an obviously duplicate name.
	s.mu.Lock()
	if _, exists := s.clients[args.Username]; exists {
		s.mu.Unlock()
		reply.OK = false
		reply.Message = "ERROR: Username already exists."
		return nil
	}
	s.mu.Unlock()

	clientRPC, err := rpc.Dial("tcp", args.ClientURL)
	if err != nil {
		reply.OK = false
		reply.Message = fmt.Sprintf("ERROR: Could not connect to client: %v", err)
		return nil
	}

	s.mu.Lock()
	if _, exists := s.clients[args.Username]; exists {
		s.mu.Unlock()
		_ = clientRPC.Close()
		reply.OK = false
		reply.Message = "ERROR: Username already exists."
		return nil
	}

	s.clients[args.Username] = &ClientInfo{
		Username:  args.Username,
		ClientURL: args.ClientURL,
		RPC:       clientRPC,
	}
	s.mu.Unlock()

	reply.OK = true
	reply.Message = "OK"

	s.broadcastExcept(
		args.Username,
		fmt.Sprintf("User %s joined the chat.", args.Username),
	)

	return nil
}

func (s *ChatServer) SendMessage(args *MessageArgs, reply *Reply) error {
	s.mu.Lock()
	_, exists := s.clients[args.Username]
	s.mu.Unlock()

	if !exists {
		reply.OK = false
		reply.Message = "ERROR: User is not connected."
		return nil
	}

	message := fmt.Sprintf("%s: %s", args.Username, args.Message)

	s.broadcastExcept(args.Username, message)

	reply.OK = true
	reply.Message = "OK"

	return nil
}

func (s *ChatServer) Leave(args *UserArgs, reply *Reply) error {
	s.mu.Lock()

	client, exists := s.clients[args.Username]

	if exists {
		delete(s.clients, args.Username)
	}

	s.mu.Unlock()

	if !exists {
		reply.OK = false
		reply.Message = "ERROR: User is not connected."
		return nil
	}

	if client.RPC != nil {
		_ = client.RPC.Close()
	}

	s.broadcastExcept(
		args.Username,
		fmt.Sprintf("User %s left the chat.", args.Username),
	)

	reply.OK = true
	reply.Message = "OK"

	return nil
}

func (s *ChatServer) List(_ *EmptyArgs, reply *Reply) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.clients) == 0 {
		reply.OK = true
		reply.Message = "No users are currently connected."
		return nil
	}

	users := make([]string, 0, len(s.clients))

	for username := range s.clients {
		users = append(users, username)
	}

	reply.OK = true
	reply.Message = "Connected users: " + joinStrings(users, ", ")

	return nil
}

func (s *ChatServer) broadcastExcept(except string, message string) {
	s.mu.Lock()

	targets := make([]*ClientInfo, 0, len(s.clients))

	for username, client := range s.clients {
		if username == except {
			continue
		}
		targets = append(targets, client)
	}

	s.mu.Unlock()

	var wg sync.WaitGroup

	for _, client := range targets {
		wg.Add(1)

		go func(c *ClientInfo) {
			defer wg.Done()

			var reply Reply
			err := c.RPC.Call(
				"ClientCallback.Notify",
				&CallbackArgs{Message: message},
				&reply,
			)

			if err != nil {
				log.Printf(
					"Could not push message to %s: %v",
					c.Username,
					err,
				)
			}
		}(client)
	}

	wg.Wait()
}

func (s *ChatServer) Shutdown() {
	s.mu.Lock()

	clients := make([]*ClientInfo, 0, len(s.clients))

	for _, client := range s.clients {
		clients = append(clients, client)
	}

	s.clients = make(map[string]*ClientInfo)

	s.mu.Unlock()

	for _, client := range clients {
		if client.RPC != nil {
			_ = client.RPC.Close()
		}
	}

	fmt.Println("Server shut down cleanly.")
}

func joinStrings(values []string, separator string) string {
	result := ""

	for i, value := range values {
		if i > 0 {
			result += separator
		}
		result += value
	}

	return result
}

func main() {
	server := NewChatServer()

	if err := rpc.RegisterName("ChatServer", server); err != nil {
		log.Fatal(err)
	}

	listener, err := net.Listen("tcp", ":1234")
	if err != nil {
		log.Fatal(err)
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-signalChan
		fmt.Println("\nShutdown signal received, closing listener...")
		_ = listener.Close()
	}()

	fmt.Println("===================================")
	fmt.Println("       Concurrent Chat Server")
	fmt.Println("===================================")
	fmt.Println("Server listening on :1234")
	fmt.Println("Waiting for clients...")
	fmt.Println()

	rpc.Accept(listener)

	server.Shutdown()
}
