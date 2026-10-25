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
	register     chan registration
	unregister   chan unregistration
	broadcast    chan *pb.Payload
	stop         chan struct{}
	done         chan struct{}
	stopOnce     sync.Once
	clients      map[*chatClient]struct{}
	lamportClock chan int64
}

func newChat() *Chat {
	return &Chat{
		register:     make(chan registration),
		unregister:   make(chan unregistration),
		broadcast:    make(chan *pb.Payload, broadcastQueueSize),
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
		clients:      make(map[*chatClient]struct{}),
		lamportClock: make(chan int64, 1),
	}
}

// only init carries the map of clients, everyone else has to use channels
func (h *Chat) init() {
	defer close(h.done)

	h.lamportClock <- 0

	for {
		select {
		case request := <-h.register:
			h.clients[request.client] = struct{}{}

			currentLamport := <-h.lamportClock
			newLamport := currentLamport + 1
			h.lamportClock <- newLamport

			log.Printf("[SERVER] A client with id %s connected (L @ %d)", request.client.username, newLamport)
			h.broadcastSystemMessage("Participant "+request.client.username+" joined the chat at logical time "+fmt.Sprintf("%d", newLamport), newLamport)
			close(request.ready)
		case request := <-h.unregister:
			result := h.removeClient(request.client)
			if result.success {
				h.broadcastSystemMessage("Participant "+request.client.username+" left the chat at logical time "+fmt.Sprintf("%d", result.newLamport), result.newLamport)
			}
			close(request.ready)
		case message := <-h.broadcast:
			currentLamport := <-h.lamportClock
			newLamport := currentLamport + 1
			h.lamportClock <- newLamport

			log.Printf("[SERVER] Broadcasting message from %s with content %s and lamport %d", message.GetUsername(), message.GetMessage(), newLamport)

			message.Lamport = newLamport
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

func (h *Chat) broadcastSystemMessage(text string, lamport int64) {
	message := &pb.Payload{Username: "Server", Message: text, Lamport: lamport}
	log.Printf("[SERVER] System message broadcast with lamport timestamp %d: %s", lamport, text)
	h.processBroadcast(message)
}

func (h *Chat) processBroadcast(message *pb.Payload) {
	for client := range h.clients {
		client.outbound <- message
	}
}

type removeResult struct {
	success    bool
	newLamport int64
}

func (h *Chat) removeClient(client *chatClient) removeResult {
	if _, exists := h.clients[client]; !exists {
		return removeResult{success: false, newLamport: 0}
	}
	delete(h.clients, client)
	close(client.done)
	close(client.outbound)

	currentLamport := <-h.lamportClock
	newLamport := currentLamport + 1
	h.lamportClock <- newLamport

	log.Printf("[SERVER] A client with id %s disconnected (L @ %d)", client.username, newLamport)
	return removeResult{success: true, newLamport: newLamport}
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

	payload := &pb.Payload{
		Username: client.username,
		Message:  "",
		Flag:     1, // 1 = join
	}
	stream.Send(payload)

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

			log.Printf("[SERVER] Received message from %s: %s (L @ %d)", client.username, message.GetMessage(), message.GetLamport())

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

			receivedLamport := result.message.GetLamport()
			newLamport := max(<-s.chat.lamportClock, receivedLamport) + 1
			s.chat.lamportClock <- newLamport

			message := &pb.Payload{
				Username: client.username,
				Message:  result.message.GetMessage(),
				Lamport:  newLamport,
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
		log.Fatalf("[SERVER] Failed to listen on %s: %v", serverAddress, err)
		log.Fatal(err)
	}

	chat := newChat()
	go chat.init()

	grpcServer := grpc.NewServer()
	pb.RegisterChatServiceServer(grpcServer, &server{chat: chat})

	signals, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	go grpcServer.Serve(listener)

	log.Printf("[SERVER] Started on %s", listener.Addr())
	<-signals.Done()
	// Log always includes a timestamp
	log.Printf("[SERVER] Shutdown requested...")

	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		log.Printf("[SERVER] Graceful shutdown timed out; stopping active streams")
		grpcServer.Stop()
		<-stopped
	}

	chat.shutdown()
	<-chat.done
	log.Printf("[SERVER] Successfully stopped")
}
