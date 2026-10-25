package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"
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
		log.Fatalf("[CLIENT] Failed to connect: %v (%s)", err, time.Now().Format(time.RFC3339))
	}
	defer conn.Close()

	baseCtx, cancelChat := context.WithCancel(context.Background())
	defer cancelChat()
	client := pb.NewChatServiceClient(conn)
	stream, err := client.Chat(baseCtx)
	if err != nil {
		log.Fatalf("[CLIENT] Failed to start chat: %v (%s)", err, time.Now().Format(time.RFC3339))
	}
	log.Printf("[CLIENT] Connected to %s.\nType any messages and press Enter; Ctrl-D (or Ctrl-C) to leave.\nMax %d characters per message.", serverAddress, maxMessageLength)

	receiveDone := make(chan error, 1)
	go func() {
		var whois string

		for {
			message, err := stream.Recv()
			if err != nil {
				receiveDone <- err
				return
			}

			if message.GetFlag() == 1 {
				fmt.Printf("\nWelcome! You are %s\n", message.GetUsername())
				if whois == "" {
					whois = message.GetUsername()
				}
			} else if message.GetFlag() == 0 {
				displayUsername := message.GetUsername()
				if displayUsername == whois {
					displayUsername = "You"
				}

				fmt.Printf("\r%s: %s\n", displayUsername, message.GetMessage())
			}
		}
	}()

	inputCtx, cancelInput := context.WithCancel(baseCtx)
	defer cancelInput()
	inputLines := make(chan string)
	inputDone := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)

		for scanner.Scan() {
			line := scanner.Text()

			// This funny thing clears the last line of the terminal
			fmt.Print("\033[1A\033[2K\r")
			select {
			case inputLines <- line:
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
					log.Printf("[CLIENT] Failed to read input: %v (%s)", err, time.Now().Format(time.RFC3339))
				}
				sending = false
				continue
			}
			if !utf8.ValidString(line) {
				log.Printf("[CLIENT] Invalid message: input must be valid UTF-8 (%s)", time.Now().Format(time.RFC3339))
				continue
			}
			if length := utf8.RuneCountInString(line); length > maxMessageLength {
				log.Printf("[CLIENT] Invalid message: got %d characters, maximum is %d characters (%s)", length, maxMessageLength, time.Now().Format(time.RFC3339))
				continue
			}
			text := strings.TrimSpace(line)
			if text == "" {
				continue
			}
			if err := stream.Send(&pb.Payload{Message: text}); err != nil {
				log.Printf("[CLIENT] Failed to send message: %v (%s)", err, time.Now().Format(time.RFC3339))
				sending = false
			}
		case err := <-receiveDone:
			if err != io.EOF {
				log.Printf("[CLIENT] Receive ended: %v (%s)", err, time.Now().Format(time.RFC3339))
			}
			receiveEnded = true
			cancelInput()
			sending = false
		}
	}

	if err := stream.CloseSend(); err != nil {
		log.Printf("[CLIENT] Failed to close send side: %v (%s)", err, time.Now().Format(time.RFC3339))
	}

	if !receiveEnded {
		select {
		case err := <-receiveDone:
			if err != nil && err != io.EOF {
				log.Printf("[CLIENT] Receive ended: %v (%s)", err, time.Now().Format(time.RFC3339))
			}
		case <-baseCtx.Done():
		}
	}
	log.Printf("[CLIENT] Disconnected (%s)", time.Now().Format(time.RFC3339))
}
