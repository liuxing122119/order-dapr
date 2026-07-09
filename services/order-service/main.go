package main

import (
	"context"
	"database/sql"
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

type Order struct {
	OrderID          string      `json:"orderId"`
	UserID           string      `json:"userId"`
	Items            []OrderItem `json:"items"`
	TotalAmount      float64     `json:"totalAmount"`
	Status           string      `json:"status"`
	CreatedAt        string      `json:"createdAt"`
	UpdatedAt        string      `json:"updatedAt,omitempty"`
	Version          int64       `json:"version,omitempty"`
	UserValidated    bool        `json:"userValidated"`
	InventoryChecked bool        `json:"inventoryChecked"`
	PaymentProcessed bool        `json:"paymentProcessed"`
}

type OrderItem struct {
	ProductID   string  `json:"productId"`
	ProductName string  `json:"productName"`
	Quantity    int     `json:"quantity"`
	Price       float64 `json:"price"`
}

type CreateOrderRequest struct {
	UserID string      `json:"userId"`
	Items  []OrderItem `json:"items"`
}

const (
	serviceName        = "order-service"
	stateStoreName     = "statestore"
	pubsubName         = "pubsub"
	appPort            = ":5002"
	userServiceAppId   = "user-service"
	paymentAppId       = "payment-service"
	inventoryAppId     = "inventory-service"
	retryPolicyName    = "order-retry-policy"
	timeoutPolicyName  = "order-timeout-policy"
	circuitBreakerName = "order-circuit-breaker"
)

var daprClient client.Client

func main() {
	db.InitDB()
	defer db.CloseDB()

	var err error

	grpcPort := os.Getenv("DAPR_GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "3502"
		fmt.Printf("[WARN] DAPR_GRPC_PORT not set, using default: %s\n", grpcPort)
	}

	maxRetries := 50              // 增加到50次
	retryDelay := 2 * time.Second // 增加到2秒间隔
	for i := 0; i < maxRetries; i++ {
		fmt.Printf("[INFO] Attempting to connect to Dapr gRPC on port %s (%d/%d)...\n", grpcPort, i+1, maxRetries)
		daprClient, err = client.NewClientWithPort(grpcPort)
		if err == nil {
			fmt.Printf("[SUCCESS] Connected to Dapr gRPC on port %s\n", grpcPort)
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

	initializeWorkflowSystem()
	startWorkflowEngine()

	s := daprhttp.NewService(appPort)

	s.AddServiceInvocationHandler("/order/create", handleCreateOrderWithWorkflow)
	s.AddServiceInvocationHandler("/order/get", handleGetOrder)
	s.AddServiceInvocationHandler("/health", handleHealth)

	paymentSubscription := &common.Subscription{
		PubsubName: pubsubName,
		Topic:      "payment.completed",
		Route:      "/events/payment/completed",
	}

	s.AddTopicEventHandler(paymentSubscription, handlePaymentCompletedEvent)

	inventorySubscription := &common.Subscription{
		PubsubName: pubsubName,
		Topic:      "inventory.updated",
		Route:      "/events/inventory/updated",
	}

	s.AddTopicEventHandler(inventorySubscription, handleInventoryUpdatedEvent)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChan

		http.Post("http://localhost:3502/v1.0/shutdown", "application/json", nil)
		stopWorkflowEngineSystem()
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
			"service-invocation-with-resilience",
			"state-management-with-etag",
			"workflow-orchestration-engine",
			"opentelemetry-tracing",
			"prometheus-metrics",
			"retry-policy",
			"timeout-policy",
			"circuit-breaker",
		},
		"resiliencePolicies": map[string]string{
			"retry":          retryPolicyName,
			"timeout":        timeoutPolicyName,
			"circuitBreaker": circuitBreakerName,
		},
		"workflowInfo": map[string]string{
			"engine":          "dapr-workflow-engine",
			"stateStore":      "redis (event-sourcing pattern)",
			"tracing":         "opentelemetry+jaeger",
			"metrics":         "prometheus",
			"metricsEndpoint": ":9090/metrics",
		},
	}
	data, _ := json.Marshal(health)
	return &common.Content{
		Data:        data,
		ContentType: "application/json",
	}, nil
}

func saveOrderToState(ctx context.Context, order Order) error {
	order.UpdatedAt = time.Now().Format(time.RFC3339)

	if db.DB != nil {
		var existingVersion int
		err := db.DB.QueryRowContext(ctx,
			"SELECT version FROM orders WHERE order_id = $1 FOR UPDATE",
			order.OrderID,
		).Scan(&existingVersion)

		if err == nil {
			db.DB.ExecContext(ctx,
				`UPDATE orders SET status = $2, user_validated = $3, inventory_checked = $4,
				 payment_processed = $5, version = version + 1, updated_at = $6
				 WHERE order_id = $1`,
				order.OrderID, order.Status, order.UserValidated, order.InventoryChecked,
				order.PaymentProcessed, order.UpdatedAt,
			)
		} else if err == sql.ErrNoRows {
			db.DB.ExecContext(ctx,
				`INSERT INTO orders (order_id, user_id, total_amount, status, user_validated,
				 inventory_checked, payment_processed, version, created_at, updated_at)
				 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
				order.OrderID, order.UserID, order.TotalAmount, order.Status,
				order.UserValidated, order.InventoryChecked, order.PaymentProcessed,
				order.Version, order.CreatedAt, order.UpdatedAt,
			)
			for _, item := range order.Items {
				db.DB.ExecContext(ctx,
					`INSERT INTO order_items (order_id, product_id, product_name, quantity, price)
					 VALUES ($1, $2, $3, $4, $5)`,
					order.OrderID, item.ProductID, item.ProductName, item.Quantity, item.Price,
				)
			}
		}
	}

	orderData, _ := json.Marshal(order)
	key := "order-" + order.OrderID

	metadata := map[string]string{
		"version": fmt.Sprintf("%d", order.Version),
		"status":  order.Status,
	}

	daprClient.SaveState(ctx, stateStoreName, key, orderData, metadata)

	return nil
}

func handleGetOrder(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	var req map[string]string
	if err := json.Unmarshal(in.Data, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	item, err := daprClient.GetState(ctx, stateStoreName, "order-"+req["orderId"], nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get order: %w", err)
	}
	if item == nil || len(item.Value) == 0 {
		return nil, fmt.Errorf("order not found: %s", req["orderId"])
	}

	var order Order
	if err := json.Unmarshal(item.Value, &order); err != nil {
		return nil, fmt.Errorf("failed to parse order: %w", err)
	}

	response := map[string]interface{}{
		"order": order,
		"metadata": map[string]string{
			"etag":               item.Etag,
			"concurrencyControl": "first-write (via Dapr component config)",
		},
	}

	data, _ := json.Marshal(response)
	return &common.Content{
		Data:        data,
		ContentType: "application/json",
	}, nil
}

func calculateTotalAmount(items []OrderItem) float64 {
	var total float64
	for _, item := range items {
		total += item.Price * float64(item.Quantity)
	}
	return total
}

func handlePaymentCompletedEvent(ctx context.Context, e *common.TopicEvent) (retry bool, err error) {
	data, ok := e.Data.([]byte)
	if !ok {
		return false, nil
	}

	var paymentEvent struct {
		PaymentID string  `json:"paymentId"`
		OrderID   string  `json:"orderId"`
		Amount    float64 `json:"amount"`
		Status    string  `json:"status"`
	}

	if err := json.Unmarshal(data, &paymentEvent); err != nil {
		return false, err
	}

	item, err := daprClient.GetState(ctx, stateStoreName, "order-"+paymentEvent.OrderID, nil)
	if err != nil {
		return true, err
	}

	if item == nil || len(item.Value) == 0 {
		return false, nil
	}

	var order Order
	if err := json.Unmarshal(item.Value, &order); err != nil {
		return false, err
	}

	order.PaymentProcessed = true
	order.Status = "payment_processed"
	order.UpdatedAt = time.Now().Format(time.RFC3339)
	order.Version++

	saveOrderToState(ctx, order)
	return false, nil
}

func handleInventoryUpdatedEvent(ctx context.Context, e *common.TopicEvent) (retry bool, err error) {
	data, ok := e.Data.([]byte)
	if !ok {
		return false, nil
	}

	var inventoryEvent struct {
		OrderID     string `json:"orderId"`
		ProductID   string `json:"productId"`
		NewQuantity int    `json:"newQuantity"`
		Operation   string `json:"operation"`
		Status      string `json:"status"`
	}

	if err := json.Unmarshal(data, &inventoryEvent); err != nil {
		return false, err
	}

	if inventoryEvent.OrderID == "" || inventoryEvent.OrderID == "null" {
		return false, nil
	}

	item, err := daprClient.GetState(ctx, stateStoreName, "order-"+inventoryEvent.OrderID, nil)
	if err != nil {
		return true, err
	}

	if item == nil || len(item.Value) == 0 {
		return false, nil
	}

	var order Order
	if err := json.Unmarshal(item.Value, &order); err != nil {
		return false, err
	}

	order.InventoryChecked = true

	if order.PaymentProcessed {
		order.Status = "inventory_updated"
	} else {
		order.Status = "inventory_checked"
	}

	order.UpdatedAt = time.Now().Format(time.RFC3339)
	order.Version++

	saveOrderToState(ctx, order)
	return false, nil
}
