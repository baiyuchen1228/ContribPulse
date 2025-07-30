package main

import (
	"context"
	"encoding/json"
	"log"
	"runtime"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/user/contrib-pulse/pkg/db"
	"github.com/user/contrib-pulse/pkg/queue"
	"go.mongodb.org/mongo-driver/mongo"
)

type AnalysisTask struct {
	TaskID  string `json:"task_id"`
	RepoURL string `json:"repo_url"`
}

func main() {
	// Connect to NATS
	nc, err := queue.Connect()
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	

	// Connect to MongoDB
	mongoClient, err := db.Connect()
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}

	// Subscribe to the topic
	_, err = nc.Subscribe("contrib.tasks", messageHandler(mongoClient))
	if err != nil {
		log.Fatalf("Failed to subscribe to Nats topic: %v", err)
	}

	log.Println("analyzer-service is ready and listening for tasks...")
	// Keep the service running
	runtime.Goexit()
}

func messageHandler(mongoClient *mongo.Client) nats.MsgHandler {
	return func(msg *nats.Msg) {
		log.Printf("Received task: %s", string(msg.Data))

		var task AnalysisTask
		if err := json.Unmarshal(msg.Data, &task); err != nil {
			log.Printf("Failed to unmarshal task: %v", err)
			return
		}

		// In a real scenario, you would fetch data from the GitHub API here.
		// For now, we'll just create a dummy result.
		log.Printf("Processing task %s for repo %s", task.TaskID, task.RepoURL)
		time.Sleep(5 * time.Second) // Simulate work

		result := map[string]interface{}{
			"task_id":      task.TaskID,
			"repo_url":     task.RepoURL,
			"processed_at": time.Now(),
			"summary": map[string]interface{}{
				"total_commits":  120,
				"total_issues":   25,
				"total_prs":      40,
				"top_contributor": "user-a",
			},
		}

		collection := mongoClient.Database("contrib_pulse").Collection("analysis_results")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, err := collection.InsertOne(ctx, result)
		if err != nil {
			log.Printf("Failed to store result in MongoDB: %v", err)
			return
		}

		log.Printf("Successfully processed and stored result for task %s", task.TaskID)
	}
}
