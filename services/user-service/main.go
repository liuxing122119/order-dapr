package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"order-dapr/pkg/db"

	"github.com/dapr/go-sdk/client"
	"github.com/dapr/go-sdk/service/common"
	daprhttp "github.com/dapr/go-sdk/service/http"
	"github.com/google/uuid"
)

type User struct {
	UserID    string `json:"userId" db:"user_id"`
	Name      string `json:"name" db:"name"`
	Email     string `json:"email" db:"email"`
	Phone     string `json:"phone" db:"phone"`
	Address   string `json:"address" db:"address"`
	CreatedAt string `json:"createdAt,omitempty" db:"created_at"`
}

type UserValidationRequest struct {
	UserID string `json:"userId"`
}

type UserValidationResponse struct {
	Valid   bool   `json:"valid"`
	Message string `json:"message"`
	User    *User  `json:"user,omitempty"`
}

const (
	serviceName    = "user-service"
	stateStoreName = "statestore"
	appPort        = ":5001"
)

var (
	daprClient     client.Client
	daprClientOnce sync.Once
	daprClientErr  error
)

func getDaprClient() (client.Client, error) {
	daprClientOnce.Do(func() {
		var err error
		maxRetries := 10
		for i := 0; i < maxRetries; i++ {
			daprClient, err = client.NewClientWithPort(os.Getenv("DAPR_GRPC_PORT"))
			if err == nil {
				return
			}
			if i < maxRetries-1 {
				time.Sleep(500 * time.Millisecond)
			}
		}
		daprClientErr = fmt.Errorf("failed to initialize Dapr client after %d attempts: %w", maxRetries, err)
	})
	return daprClient, daprClientErr
}

func main() {
	db.InitDB()
	defer db.CloseDB()

	var err error
	daprClient, err = getDaprClient()
	if err != nil {
		return
	}
	defer func() {
		if daprClient != nil {
			daprClient.Close()
		}
	}()

	s := daprhttp.NewService(appPort)

	s.AddServiceInvocationHandler("/user/create", handleCreateUser)
	s.AddServiceInvocationHandler("/user/get", handleGetUser)
	s.AddServiceInvocationHandler("/user/validate", handleValidateUser)
	s.AddServiceInvocationHandler("/health", handleHealth)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChan

		s.Stop()
	}()

	if err := s.Start(); err != nil && err != http.ErrServerClosed {
		return
	}
}

func handleHealth(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	health := map[string]interface{}{
		"service":   serviceName,
		"status":    "healthy",
		"timestamp": time.Now().Format(time.RFC3339),
		"features": []string{
			"dapr-sidecar",
			"service-invocation",
			"state-management",
			"etag-concurrency-control",
			"user-validation-api",
			"graceful-shutdown",
		},
	}
	data, _ := json.Marshal(health)
	return &common.Content{
		Data:        data,
		ContentType: "application/json",
	}, nil
}

func handleCreateUser(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	var user User
	if err := json.Unmarshal(in.Data, &user); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	user.UserID = uuid.New().String()
	user.CreatedAt = time.Now().Format(time.RFC3339)

	if db.DB != nil {
		db.DB.ExecContext(ctx,
			`INSERT INTO users (user_id, name, email, phone, address, created_at)
			 VALUES ($1, $2, $3, $4, $5, $6)`,
			user.UserID, user.Name, user.Email, user.Phone, user.Address, user.CreatedAt,
		)
	}

	userData, _ := json.Marshal(user)

	daprClient.SaveState(ctx, stateStoreName, "user-"+user.UserID, userData, map[string]string{
		"version": "1",
	})

	data, _ := json.Marshal(user)
	return &common.Content{
		Data:        data,
		ContentType: "application/json",
	}, nil
}

func handleGetUser(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	var req map[string]string
	if err := json.Unmarshal(in.Data, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	var user User

	if db.DB != nil {
		db.DB.QueryRowContext(ctx,
			"SELECT user_id, name, email, phone, address, created_at FROM users WHERE user_id = $1",
			req["userId"],
		).Scan(&user.UserID, &user.Name, &user.Email, &user.Phone, &user.Address, &user.CreatedAt)
	}

	if user.UserID == "" {
		item, err := daprClient.GetState(ctx, stateStoreName, "user-"+req["userId"], nil)
		if err != nil {
			return nil, fmt.Errorf("failed to get user from cache: %w", err)
		}
		if item == nil || len(item.Value) == 0 {
			return nil, fmt.Errorf("user not found: %s", req["userId"])
		}
		if err := json.Unmarshal(item.Value, &user); err != nil {
			return nil, fmt.Errorf("failed to unmarshal user: %w", err)
		}
	}

	response := map[string]interface{}{
		"user":   user,
		"source": "postgresql",
	}
	data, _ := json.Marshal(response)
	return &common.Content{
		Data:        data,
		ContentType: "application/json",
	}, nil
}

// handleValidateUser - 服务间调用接口：供Order Service验证用户有效性（模块1核心）
func handleValidateUser(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	var req UserValidationRequest
	if err := json.Unmarshal(in.Data, &req); err != nil {
		return nil, fmt.Errorf("invalid validation request: %w", err)
	}

	if req.UserID == "" {
		response := UserValidationResponse{
			Valid:   false,
			Message: "userID is required",
		}
		data, _ := json.Marshal(response)
		return &common.Content{Data: data, ContentType: "application/json"}, nil
	}

	var user User

	if db.DB != nil {
		db.DB.QueryRowContext(ctx,
			"SELECT user_id, name, email, phone, address, created_at FROM users WHERE user_id = $1",
			req.UserID,
		).Scan(&user.UserID, &user.Name, &user.Email, &user.Phone, &user.Address, &user.CreatedAt)
	}

	if user.UserID == "" {
		item, err := daprClient.GetState(ctx, stateStoreName, "user-"+req.UserID, nil)
		if err != nil {
			response := UserValidationResponse{
				Valid:   false,
				Message: fmt.Sprintf("error checking user: %v", err),
			}
			data, _ := json.Marshal(response)
			return &common.Content{Data: data, ContentType: "application/json"}, nil
		}

		if item == nil || len(item.Value) == 0 {
			response := UserValidationResponse{
				Valid:   false,
				Message: "user not found",
			}
			data, _ := json.Marshal(response)
			return &common.Content{Data: data, ContentType: "application/json"}, nil
		}

		if err := json.Unmarshal(item.Value, &user); err != nil {
			response := UserValidationResponse{
				Valid:   false,
				Message: "internal error",
			}
			data, _ := json.Marshal(response)
			return &common.Content{Data: data, ContentType: "application/json"}, nil
		}
	}

	response := UserValidationResponse{
		Valid:   true,
		Message: "user is valid",
		User:    &user,
	}
	data, _ := json.Marshal(response)
	return &common.Content{
		Data:        data,
		ContentType: "application/json",
	}, nil
}
