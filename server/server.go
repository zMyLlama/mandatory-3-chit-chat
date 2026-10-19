package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	pb "mandatory-3-chit-chat/grpc"
	"net"

	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedChatServiceServer
}

var (
	port = flag.Int("port", 50051, "The server port")
)

func (s *server) Chat(stream pb.ChatService_ChatServer) error {
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			log.Fatalf("failed to receive a note : %v", err)
		}

		msg := in.GetMessage()
		log.Printf("Received message: %s", msg)
	}
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterChatServiceServer(grpcServer, &server{})
	grpcServer.Serve(lis)
}
