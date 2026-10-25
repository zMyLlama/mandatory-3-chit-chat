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
	log.SetPrefix("[CLIENT] ")

	conn, err := grpc.NewClient(serverAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	baseCtx, cancelChat := context.WithCancel(context.Background())
	defer cancelChat()
	client := pb.NewChatServiceClient(conn)
	stream, err := client.Chat(baseCtx)
	if err != nil {
		log.Fatalf("Failed to start chat: %v", err)
	}
	log.Printf("Connected to %s.\nType any messages and press Enter; Ctrl-D (or Ctrl-C) to leave.\nMax %d characters per message.", serverAddress, maxMessageLength)

	receiveDone := make(chan error, 1)
	lamportClock := make(chan int64, 1)
	lamportClock <- 0

	go func() {
		var whois string

		for {
			message, err := stream.Recv()
			if err != nil {
				receiveDone <- err
				return
			}

			receivedLamport := message.GetLamport()
			lamportClock <- max(<-lamportClock, receivedLamport) + 1

			if message.GetFlag() == 1 {
				log.SetPrefix(fmt.Sprintf("[CLIENT] (%s) ", message.GetUsername()))
				log.Printf("Server confirmed join and assigned username %s (L @ %d)", message.GetUsername(), receivedLamport)
				fmt.Printf("\nWelcome! You are %s (L @ %d, %s)\n", message.GetUsername(), receivedLamport, time.Now().Format(time.RFC3339))
				if whois == "" {
					whois = message.GetUsername()
				}
			} else if message.GetFlag() == 0 {
				displayUsername := message.GetUsername()
				if displayUsername == whois {
					displayUsername = "You"
				}

				log.Printf("Received message from %s: %s (L @ %d)", message.GetUsername(), message.GetMessage(), receivedLamport)
				fmt.Printf("\r%s: %s (L @ %d, %s)\n", displayUsername, message.GetMessage(), receivedLamport, time.Now().Format(time.RFC3339))
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
					log.Printf("Failed to read input: %v", err)
				}
				sending = false
				continue
			}
			if !utf8.ValidString(line) {
				log.Printf("Invalid message: input must be valid UTF-8")
				continue
			}
			if length := utf8.RuneCountInString(line); length > maxMessageLength {
				log.Printf("Invalid message: got %d characters, maximum is %d characters", length, maxMessageLength)
				continue
			}
			text := strings.TrimSpace(line)
			if text == "" {
				continue
			}

			currentLamport := <-lamportClock
			newLamport := currentLamport + 1
			lamportClock <- newLamport

			log.Printf("Sending message: %s (L @ %d)", text, newLamport)

			if err := stream.Send(&pb.Payload{Message: text, Lamport: newLamport}); err != nil {
				log.Printf("Failed to send message: %v", err)
				sending = false
			}
		case err := <-receiveDone:
			if err != io.EOF {
				log.Printf("Receive ended: %v", err)
			}
			receiveEnded = true
			cancelInput()
			sending = false
		}
	}

	if err := stream.CloseSend(); err != nil {
		log.Printf("Failed to close send side: %v", err)
	}

	if !receiveEnded {
		select {
		case err := <-receiveDone:
			if err != nil && err != io.EOF {
				log.Printf("Receive ended: %v", err)
			}
		case <-baseCtx.Done():
		}
	}
	log.Printf("Disconnected")
}
