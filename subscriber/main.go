package main

import (
	"context"
	"fmt"
	"google.golang.org/protobuf/proto"
	"os"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	pb "pubsub-sample/github.com/hiramekun/pubsub-sample/proto"
)

func main() {
	projectID := os.Getenv("GCP_PROJECT_ID")
	subID := os.Getenv("PUBSUB_SUBSCRIPTION_ID")
	if err := pullMsgs(projectID, subID); err != nil {
		fmt.Println(err)
	}
}

func pullMsgs(projectID, subID string) error {
	// projectID := "my-project-id"
	// subID := "my-sub"
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("pubsub.NewClient: %w", err)
	}
	defer client.Close()

	sub := client.Subscription(subID)

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var received int32
	err = sub.Receive(ctx, func(_ context.Context, msg *pubsub.Message) {
		var m pb.Name
		if err := proto.Unmarshal(msg.Data, &m); err != nil {
			fmt.Printf("proto.Unmarshal: %v\n", err)
			return
		}
		fmt.Printf("Got message: %s, %s, %d\n", m.Name, m.AssignedSexAtBirth, m.Count)
		atomic.AddInt32(&received, 1)
		msg.Ack()
	})
	if err != nil {
		return fmt.Errorf("sub.Receive: %w", err)
	}
	fmt.Printf("Received %d messages\n", received)

	return nil
}
