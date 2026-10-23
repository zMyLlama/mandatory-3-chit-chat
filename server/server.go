package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"

	pb "mandatory-3-chit-chat/grpc"

	"google.golang.org/grpc"
)

const (
	outboundQueueSize  = 32
	broadcastQueueSize = 256
	maxMessageLength   = 128
	serverAddress      = ":50051"
)

type chatClient struct {
	username string
	outbound chan *pb.Payload
	done     chan struct{}
}

type registration struct {
	client *chatClient
	ready  chan struct{}
}

type unregistration struct {
	client *chatClient
	ready  chan struct{}
}

type Chat struct {
	register   chan registration
	unregister chan unregistration
	broadcast  chan *pb.Payload
	stop       chan struct{}
	done       chan struct{}
	stopOnce   sync.Once
	clients    map[*chatClient]struct{}
}

func newChat() *Chat {
	return &Chat{
		register:   make(chan registration),
		unregister: make(chan unregistration),
		broadcast:  make(chan *pb.Payload, broadcastQueueSize),
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
		clients:    make(map[*chatClient]struct{}),
	}
}

// only init carries the map of clients, everyone else has to use channels
func (h *Chat) init() {
	defer close(h.done)

	for {
		select {
		case request := <-h.register:
			h.clients[request.client] = struct{}{}
			log.Printf("client connected")
			h.broadcastSystemMessage(request.client.username + " joined the chat")
			close(request.ready)
		case request := <-h.unregister:
			if h.removeClient(request.client) {
				h.broadcastSystemMessage(request.client.username + " left the chat")
			}
			close(request.ready)
		case message := <-h.broadcast:
			h.processBroadcast(message)
		case <-h.stop:
			for client := range h.clients {
				h.removeClient(client)
			}
			return
		}
	}
}

func (h *Chat) registerClient(ctx context.Context, client *chatClient) bool {
	request := registration{client: client, ready: make(chan struct{})}
	select {
	case h.register <- request:
	case <-ctx.Done():
		return false
	case <-h.done:
		return false
	}

	select {
	case <-request.ready:
		return true
	case <-h.done:
		return false
	}
}

func (h *Chat) unregisterClient(client *chatClient) {
	request := unregistration{client: client, ready: make(chan struct{})}
	select {
	case h.unregister <- request:
		select {
		case <-request.ready:
		case <-h.done:
		}
	case <-h.done:
	}
}

func (h *Chat) enqueueMessage(message *pb.Payload) bool {
	select {
	case h.broadcast <- message:
		return true
	case <-h.done:
		return false
	}
}

func (h *Chat) broadcastSystemMessage(text string) {
	message := &pb.Payload{Username: "SERVER", Message: text}
	log.Printf("system message broadcast")
	h.processBroadcast(message)
}

func (h *Chat) processBroadcast(message *pb.Payload) {
	for client := range h.clients {
		client.outbound <- message
	}
}

func (h *Chat) removeClient(client *chatClient) bool {
	if _, exists := h.clients[client]; !exists {
		return false
	}
	delete(h.clients, client)
	close(client.done)
	close(client.outbound)
	log.Printf("client disconnected")
	return true
}

func (h *Chat) shutdown() {
	h.stopOnce.Do(func() { close(h.stop) })
}

type server struct {
	pb.UnimplementedChatServiceServer
	chat   *Chat
	nextID atomic.Uint64
}

func (s *server) Chat(stream pb.ChatService_ChatServer) error {
	id := s.nextID.Add(1)
	client := &chatClient{
		username: fmt.Sprintf("user %d", id),
		outbound: make(chan *pb.Payload, outboundQueueSize),
		done:     make(chan struct{}),
	}
	if !s.chat.registerClient(stream.Context(), client) {
		return nil
	}

	senderDone := make(chan struct{})
	go s.sendMessages(stream, client, senderDone)
	defer func() {
		s.chat.unregisterClient(client)
		<-senderDone
	}()

	type receiveResult struct {
		message *pb.Payload
		err     error
	}
	received := make(chan receiveResult, 1)
	go func() {
		for {
			message, err := stream.Recv()
			select {
			case received <- receiveResult{message: message, err: err}:
			case <-stream.Context().Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case result := <-received:
			if result.err != nil {
				return nil
			}

			if !utf8.ValidString(result.message.GetMessage()) {
				continue
			}
			if utf8.RuneCountInString(result.message.GetMessage()) > maxMessageLength {
				continue
			}

			message := &pb.Payload{
				Username: client.username,
				Message:  result.message.GetMessage(),
			}

			if message.Message == "" {
				continue
			}

			if !s.chat.enqueueMessage(message) {
				return nil
			}

		case <-senderDone:
			return nil

		case <-client.done:
			return nil

		case <-stream.Context().Done():
			return nil

		}
	}
}

func (s *server) sendMessages(stream pb.ChatService_ChatServer, client *chatClient, done chan<- struct{}) {
	defer close(done)
	for {
		select {
		case <-client.done:
			return
		default:
		}

		select {
		case <-client.done:
			return
		case message, ok := <-client.outbound:
			if !ok {
				return
			}
			if stream.Send(message) != nil {
				return
			}
		}
	}
}

func main() {
	listener, err := net.Listen("tcp", serverAddress)
	if err != nil {
		log.Fatal(err)
	}

	chat := newChat()
	go chat.init()

	grpcServer := grpc.NewServer()
	pb.RegisterChatServiceServer(grpcServer, &server{chat: chat})

	signals, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	go grpcServer.Serve(listener)

	log.Printf("server started address=%s", listener.Addr())
	<-signals.Done()
	log.Printf("server shutdown requested")

	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		log.Printf("graceful shutdown timed out; stopping active streams")
		grpcServer.Stop()
		<-stopped
	}

	chat.shutdown()
	<-chat.done
	log.Printf("server stopped")
}
