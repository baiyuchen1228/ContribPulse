package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/user/contrib-pulse/pkg/db"
	"github.com/user/contrib-pulse/pkg/queue"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type AnalysisRequest struct {
	RepoURL string `json:"repo_url"`
}

type AnalysisTask struct {
	TaskID  string `json:"task_id"`
	RepoURL string `json:"repo_url"`
}

var mongoClient *mongo.Client

func main() {
	// Connect to NATS
	nc, err := queue.Connect()
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	// Connect to MongoDB
	mongoClient, err = db.Connect()
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}

	log.Println("Waiting for analyzer-service to be ready...")
	time.Sleep(2 * time.Second)

	http.HandleFunc("/api/analyze", analyzeHandler(nc))
	http.HandleFunc("/api/results/", resultsHandler)

	log.Println("api-server starting on port 8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}

func analyzeHandler(nc *nats.Conn) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
			return
		}

		var req AnalysisRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		if req.RepoURL == "" {
			http.Error(w, "repo_url is required", http.StatusBadRequest)
			return
		}

		taskID := uuid.New().String()
		task := AnalysisTask{
			TaskID:  taskID,
			RepoURL: req.RepoURL,
		}

		taskJSON, err := json.Marshal(task)
		if err != nil {
			http.Error(w, "Failed to create task", http.StatusInternalServerError)
			return
		}

		if err := nc.Publish("contrib.tasks", taskJSON); err != nil {
			log.Printf("Failed to publish task to NATS: %v", err)
			http.Error(w, "Failed to publish task", http.StatusInternalServerError)
			return
		}

		log.Printf("Published task %s for repo %s", taskID, req.RepoURL)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"task_id": taskID})
	}
}

func resultsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET method is allowed", http.StatusMethodNotAllowed)
		return
	}

	taskID := strings.TrimPrefix(r.URL.Path, "/api/results/")
	if taskID == "" {
		http.Error(w, "Task ID is required", http.StatusBadRequest)
		return
	}

	collection := mongoClient.Database("contrib_pulse").Collection("analysis_results")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var result bson.M
	err := collection.FindOne(ctx, bson.M{"task_id": taskID}).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			http.Error(w, "Result not found", http.StatusNotFound)
			return
		}
		log.Printf("Failed to query MongoDB: %v", err)
		http.Error(w, "Failed to retrieve result", http.StatusInternalServerError)
		return
	}

	// Remove the internal MongoDB _id field before returning the result
	delete(result, "_id")

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}
