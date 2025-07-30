package main

import (
	"context"
	"encoding/json"
	"log"
	"net/url"
	"os"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/google/go-github/v59/github"
	"github.com/nats-io/nats.go"
	"github.com/user/contrib-pulse/pkg/db"
	"github.com/user/contrib-pulse/pkg/queue"
	"go.mongodb.org/mongo-driver/mongo"
	"golang.org/x/oauth2"
)

type AnalysisTask struct {
	TaskID  string `json:"task_id"`
	RepoURL string `json:"repo_url"`
}

type Contributor struct {
	Login string
	Count int
}

type ByCount []Contributor

func (a ByCount) Len() int           { return len(a) }
func (a ByCount) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByCount) Less(i, j int) bool { return a[i].Count > a[j].Count } // Descending order

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

		log.Printf("Processing task %s for repo %s", task.TaskID, task.RepoURL)

		// Initialize GitHub client
		githubToken := os.Getenv("GITHUB_TOKEN")
		if githubToken == "" {
			log.Printf("GITHUB_TOKEN environment variable not set. Cannot fetch real GitHub data.")
			return
		}
		ctx := context.Background()
		ts := oauth2.StaticTokenSource(
			&oauth2.Token{AccessToken: githubToken},
		)
		tc := oauth2.NewClient(ctx, ts)
		client := github.NewClient(tc)

		// Parse repo URL
		u, err := url.Parse(task.RepoURL)
		if err != nil {
			log.Printf("Failed to parse repo URL %s: %v", task.RepoURL, err)
			return
		}
		pathParts := strings.Split(u.Path, "/")
		if len(pathParts) < 3 {
			log.Printf("Invalid repo URL format: %s", task.RepoURL)
			return
		}
		owner := pathParts[1]
		repoName := strings.TrimSuffix(pathParts[2], ".git")

		contributorCounts := make(map[string]int)
		totalCommits := 0
		totalPRs := 0
		totalIssues := 0

		// Fetch all commits and count contributors
		optCommits := &github.CommitsListOptions{ListOptions: github.ListOptions{PerPage: 100}}
		for {
			commits, resp, err := client.Repositories.ListCommits(ctx, owner, repoName, optCommits)
			if err != nil {
				log.Printf("Failed to fetch commits for %s/%s: %v", owner, repoName, err)
				break
			}
			totalCommits += len(commits)
			for _, commit := range commits {
				if commit.Author != nil && commit.Author.Login != nil {
					contributorCounts[*commit.Author.Login]++
				} else if commit.Commit != nil && commit.Commit.Author != nil && commit.Commit.Author.Email != nil {
					// Fallback to email if login is not available (e.g., for old commits or deleted accounts)
					contributorCounts[*commit.Commit.Author.Email]++
				}
			}
			if resp.NextPage == 0 {
				break
			}
			optCommits.Page = resp.NextPage
		}

		// Fetch all pull requests and count contributors
		optPRs := &github.PullRequestListOptions{State: "all", ListOptions: github.ListOptions{PerPage: 100}}
		for {
			prs, resp, err := client.PullRequests.List(ctx, owner, repoName, optPRs)
			if err != nil {
				log.Printf("Failed to fetch pull requests for %s/%s: %v", owner, repoName, err)
				break
			}
			totalPRs += len(prs)
			for _, pr := range prs {
				if pr.User != nil && pr.User.Login != nil {
					contributorCounts[*pr.User.Login]++
				}
			}
			if resp.NextPage == 0 {
				break
			}
			optPRs.Page = resp.NextPage
		}

		// Fetch all issues and count contributors
		optIssues := &github.IssueListByRepoOptions{State: "all", ListOptions: github.ListOptions{PerPage: 100}}
		for {
			issues, resp, err := client.Issues.ListByRepo(ctx, owner, repoName, optIssues)
			if err != nil {
				log.Printf("Failed to fetch issues for %s/%s: %v", owner, repoName, err)
				break
			}
			totalIssues += len(issues)
			for _, issue := range issues {
				if issue.User != nil && issue.User.Login != nil {
					contributorCounts[*issue.User.Login]++
				}
			}
			if resp.NextPage == 0 {
				break
			}
			optIssues.Page = resp.NextPage
		}

		// Get top 10 contributors
		var contributors []Contributor
		for login, count := range contributorCounts {
			contributors = append(contributors, Contributor{Login: login, Count: count})
		}
		sort.Sort(ByCount(contributors))

		topContributors := []string{}
		for i, c := range contributors {
			if i >= 10 {
				break
			}
			topContributors = append(topContributors, c.Login)
		}

		result := map[string]interface{}{
			"task_id":      task.TaskID,
			"repo_url":     task.RepoURL,
			"processed_at": time.Now(),
			"summary": map[string]interface{}{
				"total_commits":   totalCommits,
				"total_issues":    totalIssues,
				"total_prs":       totalPRs,
				"top_contributors": topContributors,
			},
		}

		collection := mongoClient.Database("contrib_pulse").Collection("analysis_results")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, err = collection.InsertOne(ctx, result)
		if err != nil {
			log.Printf("Failed to store result in MongoDB: %v", err)
			return
		}

		log.Printf("Successfully processed and stored result for task %s", task.TaskID)
	}
}

