package main

import (
	"context"
	"flag"
	"io"
	"log"

	pb "mandatory-3-chit-chat/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	addr = flag.String("addr", "localhost:50051", "the address to connect to")
)

func main() {
	flag.Parse()

	var opts []grpc.DialOption = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	conn, err := grpc.NewClient(*addr, opts...)
	if err != nil {
		log.Fatalf("[CLIENT] Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewChatServiceClient(conn)

	stream, err := client.Chat(context.Background())
	waitc := make(chan struct{})
	go func() {
		for {
			in, err := stream.Recv()
			if err == io.EOF {
				close(waitc)
				return
			}
			if err != nil {
				log.Fatalf("[CLIENT] Failed to receive a note : %v", err)
			}

			log.Printf("[CLIENT] Got message: %s", in.Message)
		}
	}()

	if err := stream.Send(&pb.Payload{Message: "Hello, World!"}); err != nil {
		log.Fatalf("[CLIENT] Failed to send a note: %v", err)
	}

	stream.CloseSend()
	<-waitc
}
