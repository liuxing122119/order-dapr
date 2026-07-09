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

	"order-dapr/pkg/db"

	"github.com/dapr/go-sdk/client"
	"github.com/dapr/go-sdk/service/common"
	daprhttp "github.com/dapr/go-sdk/service/http"
	"github.com/google/uuid"
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

type OrderStatusUpdate struct {
	OrderID      string `json:"orderId"`
	Status       string `json:"status"`
	UpdateReason string `json:"updateReason,omitempty"`
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
	maxRetries := 5
	for i := 0; i < maxRetries; i++ {
		daprClient, err = client.NewClientWithPort(os.Getenv("DAPR_GRPC_PORT"))
		if err == nil {
			break
		}
		if i < maxRetries-1 {
			time.Sleep(2 * time.Second)
		}
	}
	if err != nil {
		return
	}
	defer daprClient.Close()

	initWorkflowEngine()
	startWorkflowEngine()

	s := daprhttp.NewService(appPort)

	s.AddServiceInvocationHandler("/order/create", handleCreateOrderWithWorkflow)
	s.AddServiceInvocationHandler("/order/create/legacy", handleCreateOrder)
	s.AddServiceInvocationHandler("/order/get", handleGetOrder)
	s.AddServiceInvocationHandler("/order/status/update", handleUpdateOrderStatus)
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

		stopWorkflowEngine()
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

func handleCreateOrder(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	var req CreateOrderRequest
	if err := json.Unmarshal(in.Data, &req); err != nil {
		return nil, fmt.Errorf("invalid order request: %w", err)
	}

	if req.UserID == "" || len(req.Items) == 0 {
		return nil, fmt.Errorf("userId and items are required")
	}

	orderID := uuid.New().String()

	order := Order{
		OrderID:          orderID,
		UserID:           req.UserID,
		Items:            req.Items,
		Status:           "pending",
		CreatedAt:        time.Now().Format(time.RFC3339),
		Version:          1,
		UserValidated:    false,
		InventoryChecked: false,
		PaymentProcessed: false,
	}

	totalAmount := calculateTotalAmount(req.Items)
	order.TotalAmount = totalAmount

	if err := saveOrderToState(ctx, order); err != nil {
		return nil, fmt.Errorf("failed to persist initial order: %w", err)
	}

	orderEventData, _ := json.Marshal(order)
	daprClient.PublishEvent(ctx, pubsubName, "order.created", orderEventData)

	go func() {
		backgroundCtx := context.Background()
		processOrderWorkflow(backgroundCtx, order)
	}()

	response := map[string]interface{}{
		"orderId": orderID,
		"status":  "processing",
		"message": "Order created with pubsub event and legacy workflow started",
		"policies": map[string]string{
			"retry":          retryPolicyName,
			"timeout":        timeoutPolicyName,
			"circuitBreaker": circuitBreakerName,
		},
		"eventsPublished": []string{"order.created"},
	}

	data, _ := json.Marshal(response)
	return &common.Content{
		Data:        data,
		ContentType: "application/json",
	}, nil
}

func processOrderWorkflow(ctx context.Context, order Order) error {
	if err := validateUserViaServiceCall(ctx, &order); err != nil {
		return updateOrderFailed(ctx, order, "user_validation_failed", err.Error())
	}

	if err := checkInventoryViaServiceCall(ctx, &order); err != nil {
		return updateOrderFailed(ctx, order, "inventory_check_failed", err.Error())
	}

	if err := processPaymentViaServiceCall(ctx, &order); err != nil {
		return updateOrderFailed(ctx, order, "payment_failed", err.Error())
	}

	if err := completeOrder(ctx, &order); err != nil {
		return fmt.Errorf("failed to complete order: %w", err)
	}

	return nil
}

func validateUserViaServiceCall(ctx context.Context, order *Order) error {
	reqData := map[string]string{
		"userId": order.UserID,
	}
	content, _ := json.Marshal(reqData)

	resp, err := daprClient.InvokeMethodWithContent(ctx, userServiceAppId, "user/validate", "post",
		&client.DataContent{
			ContentType: "application/json",
			Data:        content,
		})
	if err != nil {
		return fmt.Errorf("service invocation failed (user validation): %w", err)
	}

	var validationResp struct {
		Valid   bool            `json:"valid"`
		Message string          `json:"message"`
		User    json.RawMessage `json:"user,omitempty"`
	}
	if err := json.Unmarshal(resp, &validationResp); err != nil {
		return fmt.Errorf("failed to parse user validation response: %w", err)
	}

	if !validationResp.Valid {
		return fmt.Errorf("user validation failed: %s", validationResp.Message)
	}

	order.UserValidated = true
	order.Status = "user_validated"

	if err := saveOrderToState(ctx, *order); err != nil {
		return fmt.Errorf("failed to save order after user validation: %w", err)
	}

	return nil
}

func checkInventoryViaServiceCall(ctx context.Context, order *Order) error {
	itemsForCheck := make([]map[string]interface{}, len(order.Items))
	for i, item := range order.Items {
		itemsForCheck[i] = map[string]interface{}{
			"productId": item.ProductID,
			"quantity":  item.Quantity,
		}
	}
	content, _ := json.Marshal(itemsForCheck)

	resp, err := daprClient.InvokeMethodWithContent(ctx, inventoryAppId, "inventory/check", "post",
		&client.DataContent{
			ContentType: "application/json",
			Data:        content,
		})
	if err != nil {
		return fmt.Errorf("service invocation failed (inventory check): %w", err)
	}

	var inventoryResp struct {
		AllAvailable bool                     `json:"allAvailable"`
		Items        []map[string]interface{} `json:"items"`
	}
	if err := json.Unmarshal(resp, &inventoryResp); err != nil {
		return fmt.Errorf("failed to parse inventory response: %w", err)
	}

	if !inventoryResp.AllAvailable {
		return fmt.Errorf("insufficient inventory for some items")
	}

	order.InventoryChecked = true
	order.Status = "inventory_checked"

	if err := saveOrderToState(ctx, *order); err != nil {
		return fmt.Errorf("failed to save order after inventory check: %w", err)
	}

	return nil
}

func processPaymentViaServiceCall(ctx context.Context, order *Order) error {
	paymentReq := map[string]interface{}{
		"orderId": order.OrderID,
		"amount":  order.TotalAmount,
	}
	content, _ := json.Marshal(paymentReq)

	resp, err := daprClient.InvokeMethodWithContent(ctx, paymentAppId, "payment/process", "post",
		&client.DataContent{
			ContentType: "application/json",
			Data:        content,
		})
	if err != nil {
		return fmt.Errorf("service invocation failed (payment): %w", err)
	}

	var paymentResp struct {
		PaymentID string `json:"paymentId"`
		Status    string `json:"status"`
	}
	if err := json.Unmarshal(resp, &paymentResp); err != nil {
		return fmt.Errorf("failed to parse payment response: %w", err)
	}

	if paymentResp.Status != "completed" {
		return fmt.Errorf("payment not completed, status: %s", paymentResp.Status)
	}

	order.PaymentProcessed = true
	order.Status = "payment_processed"

	if err := saveOrderToState(ctx, *order); err != nil {
		return fmt.Errorf("failed to save order after payment: %w", err)
	}

	return nil
}

func completeOrder(ctx context.Context, order *Order) error {
	now := time.Now().Format(time.RFC3339)
	order.Status = "completed"
	order.UpdatedAt = now
	order.Version++

	orderData, _ := json.Marshal(order)

	metadata := map[string]string{
		"version":       fmt.Sprintf("%d", order.Version),
		"completedAt":   now,
		"userValidated": fmt.Sprintf("%t", order.UserValidated),
		"inventoryOk":   fmt.Sprintf("%t", order.InventoryChecked),
		"paymentOk":     fmt.Sprintf("%t", order.PaymentProcessed),
	}

	err := daprClient.SaveState(ctx, stateStoreName, "order-"+order.OrderID, orderData, metadata)
	if err != nil {
		return fmt.Errorf("failed to save completed order state to Redis: %w", err)
	}

	daprClient.PublishEvent(ctx, pubsubName, "orders-completed", orderData)

	return nil
}

func updateOrderFailed(ctx context.Context, order Order, status, reason string) error {
	order.Status = status
	order.UpdatedAt = time.Now().Format(time.RFC3339)
	order.Version++

	if err := saveOrderToState(ctx, order); err != nil {
		return fmt.Errorf("failed to update failed order: %w", err)
	}

	return fmt.Errorf("order failed [%s]: %s", status, reason)
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

func handleUpdateOrderStatus(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	var req OrderStatusUpdate
	if err := json.Unmarshal(in.Data, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	item, err := daprClient.GetState(ctx, stateStoreName, "order-"+req.OrderID, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get current order: %w", err)
	}
	if item == nil {
		return nil, fmt.Errorf("order not found: %s", req.OrderID)
	}

	var order Order
	if err := json.Unmarshal(item.Value, &order); err != nil {
		return nil, fmt.Errorf("failed to parse order: %w", err)
	}

	order.Status = req.Status
	order.UpdatedAt = time.Now().Format(time.RFC3339)
	order.Version++

	if err := saveOrderToState(ctx, order); err != nil {
		return nil, fmt.Errorf("failed to update order (possible conflict): %w", err)
	}

	data, _ := json.Marshal(order)
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
