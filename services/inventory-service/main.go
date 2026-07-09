package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"order-dapr/db"

	"github.com/dapr/go-sdk/client"
	"github.com/dapr/go-sdk/service/common"
	daprhttp "github.com/dapr/go-sdk/service/http"
)

type Inventory struct {
	ProductID   string  `json:"productId"`
	ProductName string  `json:"productName"`
	Quantity    int     `json:"quantity"`
	Price       float64 `json:"price"`
	Category    string  `json:"category"`
}

const (
	serviceName    = "inventory-service"
	stateStoreName = "statestore"
	pubsubName     = "pubsub"
	appPort        = ":5004"
)

var daprClient client.Client

func main() {
	db.InitDB()
	defer db.CloseDB()

	var err error

	grpcPort := os.Getenv("DAPR_GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "3504"
		fmt.Printf("[WARN] DAPR_GRPC_PORT not set, using default: %s\n", grpcPort)
	}

	maxRetries := 50              // 增加到50次
	retryDelay := 2 * time.Second // 增加到2秒间隔
	for i := 0; i < maxRetries; i++ {
		fmt.Printf("[INFO] Attempting to connect to Dapr gRPC on port %s (%d/%d)...\n", grpcPort, i+1, maxRetries)
		daprClient, err = client.NewClientWithPort(grpcPort)
		if err == nil {
			fmt.Printf("[SUCCESS] Connected to Darp gRPC on port %s\n", grpcPort)
			break
		}
		if i < maxRetries-1 {
			time.Sleep(retryDelay)
		}
	}
	if err != nil {
		return
	}
	defer daprClient.Close()

	s := daprhttp.NewService(appPort)

	s.AddServiceInvocationHandler("/inventory/check", handleCheckInventory)
	s.AddServiceInvocationHandler("/inventory/update", handleUpdateInventory)
	s.AddServiceInvocationHandler("/health", handleHealth)

	subscription := &common.Subscription{
		PubsubName: pubsubName,
		Topic:      "order.created",
		Route:      "/events/order/created",
	}

	s.AddTopicEventHandler(subscription, handleOrderCreatedEvent)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChan

		http.Post("http://localhost:3504/v1.0/shutdown", "application/json", nil)
		s.Stop()
		os.Exit(0)
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
			"pubsub-subscriber",
			"graceful-shutdown",
		},
		"subscriptions": []string{
			"order.created",
		},
	}
	data, _ := json.Marshal(health)
	return &common.Content{
		Data:        data,
		ContentType: "application/json",
	}, nil
}

func handleOrderCreatedEvent(ctx context.Context, e *common.TopicEvent) (retry bool, err error) {
	data, ok := e.Data.([]byte)
	if !ok {
		return false, nil
	}

	var orderEvent struct {
		OrderID string `json:"orderId"`
		UserID  string `json:"userId"`
		Items   []struct {
			ProductID   string  `json:"productId"`
			ProductName string  `json:"productName"`
			Quantity    int     `json:"quantity"`
			Price       float64 `json:"price"`
		} `json:"items"`
	}

	if err := json.Unmarshal(data, &orderEvent); err != nil {
		return false, err
	}

	reservationRecord := map[string]interface{}{
		"orderId":    orderEvent.OrderID,
		"itemCount":  len(orderEvent.Items),
		"status":     "reservation_pending",
		"reservedAt": time.Now().Format(time.RFC3339),
		"message":    "Inventory reservation initiated from order event",
	}

	reservationData, _ := json.Marshal(reservationRecord)
	if err := daprClient.SaveState(ctx, stateStoreName, "inventory-reservation-"+orderEvent.OrderID, reservationData, nil); err != nil {
		return true, err
	}

	return false, nil
}

func handleCheckInventory(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	var items []struct {
		ProductID string `json:"productId"`
		Quantity  int    `json:"quantity"`
	}
	if err := json.Unmarshal(in.Data, &items); err != nil {
		return nil, err
	}

	result := make([]map[string]interface{}, len(items))
	allAvailable := true

	for i, item := range items {
		stateItem, err := daprClient.GetState(ctx, stateStoreName, "inventory-"+item.ProductID, nil)
		if err != nil {
			result[i] = map[string]interface{}{
				"productId": item.ProductID,
				"available": false,
				"error":     err.Error(),
			}
			allAvailable = false
			continue
		}

		var inventory Inventory
		if err := json.Unmarshal(stateItem.Value, &inventory); err != nil {
			result[i] = map[string]interface{}{
				"productId": item.ProductID,
				"available": false,
				"error":     "parse error",
			}
			allAvailable = false
			continue
		}

		available := inventory.Quantity >= item.Quantity
		if !available {
			allAvailable = false
		}

		result[i] = map[string]interface{}{
			"productId":   item.ProductID,
			"productName": inventory.ProductName,
			"requested":   item.Quantity,
			"available":   inventory.Quantity,
			"isAvailable": available,
		}
	}

	response := map[string]interface{}{
		"allAvailable": allAvailable,
		"items":        result,
	}

	data, _ := json.Marshal(response)
	return &common.Content{
		Data:        data,
		ContentType: "application/json",
	}, nil
}

func handleUpdateInventory(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	var req struct {
		ProductID string `json:"productId"`
		Quantity  int    `json:"quantity"`
		Operation string `json:"operation"`
	}
	if err := json.Unmarshal(in.Data, &req); err != nil {
		return nil, err
	}

	item, err := daprClient.GetState(ctx, stateStoreName, "inventory-"+req.ProductID, nil)
	if err != nil {
		return nil, err
	}

	var inventory Inventory
	if err := json.Unmarshal(item.Value, &inventory); err != nil {
		return nil, err
	}

	switch req.Operation {
	case "decrease":
		inventory.Quantity -= req.Quantity
	case "increase":
		inventory.Quantity += req.Quantity
	default:
		return nil, fmt.Errorf("invalid operation: %s", req.Operation)
	}

	if db.DB != nil {
		db.DB.ExecContext(ctx,
			`UPDATE inventory SET quantity = $2, updated_at = $3 WHERE product_id = $1`,
			req.ProductID, inventory.Quantity, time.Now(),
		)
	}

	itemData, _ := json.Marshal(inventory)
	daprClient.SaveState(ctx, stateStoreName, "inventory-"+req.ProductID, itemData, nil)

	response := map[string]interface{}{
		"success":     true,
		"message":     "Inventory updated successfully",
		"productId":   req.ProductID,
		"newQuantity": inventory.Quantity,
	}
	data, _ := json.Marshal(response)
	return &common.Content{
		Data:        data,
		ContentType: "application/json",
	}, nil
}
