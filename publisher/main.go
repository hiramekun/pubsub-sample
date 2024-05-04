package main

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	"os"
	pb "pubsub-sample/github.com/hiramekun/pubsub-sample/proto"
)

func main() {
	projectID := os.Getenv("GCP_PROJECT_ID")
	topicID := os.Getenv("PUBSUB_TOPIC_ID")
	msg := "Hello World"
	if err := publish(projectID, topicID, msg); err != nil {
		fmt.Println(err)
	}
}

func publish(projectID, topicID, msg string) error {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("pubsub: NewClient: %w", err)
	}
	defer client.Close()

	t := client.Topic(topicID)
	pbMsg := pb.MyMessage{Content: msg}
	data, err := proto.Marshal(&pbMsg)
	if err != nil {
		return fmt.Errorf("proto: Marshal: %w", err)
	}
	result := t.Publish(ctx, &pubsub.Message{Data: data})
	// Block until the result is returned and a server-generated
	// ID is returned for the published message.
	id, err := result.Get(ctx)
	if err != nil {
		return fmt.Errorf("pubsub: result.Get: %w", err)
	}
	fmt.Printf("Published a message; msg ID: %v\n", id)
	return nil
}
