package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"unicode/utf8"

	pb "mandatory-3-chit-chat/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const serverAddress = "localhost:50051"

const maxMessageLength = 128

func main() {
	conn, err := grpc.NewClient(serverAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[CLIENT] Failed to connect: %v", err)
	}
	defer conn.Close()

	baseCtx, cancelChat := context.WithCancel(context.Background())
	defer cancelChat()
	client := pb.NewChatServiceClient(conn)
	stream, err := client.Chat(baseCtx)
	if err != nil {
		log.Fatalf("[CLIENT] Failed to start chat: %v", err)
	}
	log.Printf("[CLIENT] Connected to %s. Type messages and press Enter; Ctrl-D to leave.", serverAddress)

	receiveDone := make(chan error, 1)
	go func() {
		for {
			message, err := stream.Recv()
			if err != nil {
				receiveDone <- err
				return
			}
			fmt.Printf("\r%s: %s\n", message.GetUsername(), message.GetMessage())
		}
	}()

	inputCtx, cancelInput := context.WithCancel(baseCtx)
	defer cancelInput()
	inputLines := make(chan string)
	inputDone := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			select {
			case inputLines <- scanner.Text():
			case <-inputCtx.Done():
				return
			}
		}
		inputDone <- scanner.Err()
		close(inputLines)
	}()

	sending := true
	receiveEnded := false
	for sending {
		select {
		case line, ok := <-inputLines:
			if !ok {
				if err := <-inputDone; err != nil {
					log.Printf("[CLIENT] Failed to read input: %v", err)
				}
				sending = false
				continue
			}
			if !utf8.ValidString(line) {
				log.Printf("[CLIENT] Message not sent: input must be valid UTF-8")
				continue
			}
			if length := utf8.RuneCountInString(line); length > maxMessageLength {
				log.Printf("[CLIENT] Message not sent: got %d characters, maximum is %d", length, maxMessageLength)
				continue
			}
			text := strings.TrimSpace(line)
			if text == "" {
				continue
			}
			if err := stream.Send(&pb.Payload{Message: text}); err != nil {
				log.Printf("[CLIENT] Failed to send message: %v", err)
				sending = false
			}
		case err := <-receiveDone:
			if err != io.EOF {
				log.Printf("[CLIENT] Receive ended: %v", err)
			}
			receiveEnded = true
			cancelInput()
			sending = false
		}
	}

	if err := stream.CloseSend(); err != nil {
		log.Printf("[CLIENT] Failed to close send side: %v", err)
	}

	if !receiveEnded {
		select {
		case err := <-receiveDone:
			if err != nil && err != io.EOF {
				log.Printf("[CLIENT] Receive ended: %v", err)
			}
		case <-baseCtx.Done():
		}
	}
	log.Printf("[CLIENT] Disconnected")
}
