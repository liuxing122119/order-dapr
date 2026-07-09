package main

import (
	"context"
	"encoding/json"
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

type Payment struct {
	PaymentID     string  `json:"paymentId"`
	OrderID       string  `json:"orderId"`
	Amount        float64 `json:"amount"`
	Status        string  `json:"status"`
	PaymentMethod string  `json:"paymentMethod"`
	CreatedAt     string  `json:"createdAt"`
}

const (
	serviceName    = "payment-service"
	stateStoreName = "statestore"
	pubsubName     = "pubsub"
	appPort        = ":5003"
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

	s := daprhttp.NewService(appPort)

	s.AddServiceInvocationHandler("/payment/create", handleCreatePayment)
	s.AddServiceInvocationHandler("/payment/process", handleProcessPayment)
	s.AddServiceInvocationHandler("/payment/get", handleGetPayment)
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
		OrderID     string  `json:"orderId"`
		UserID      string  `json:"userId"`
		TotalAmount float64 `json:"totalAmount"`
		Status      string  `json:"status"`
	}

	if err := json.Unmarshal(data, &orderEvent); err != nil {
		return false, err
	}

	paymentRecord := map[string]interface{}{
		"orderId":    orderEvent.OrderID,
		"amount":     orderEvent.TotalAmount,
		"status":     "pending_preparation",
		"preparedAt": time.Now().Format(time.RFC3339),
		"message":    "Payment record pre-created from order event",
	}

	paymentData, _ := json.Marshal(paymentRecord)
	if err := daprClient.SaveState(ctx, stateStoreName, "payment-prep-"+orderEvent.OrderID, paymentData, nil); err != nil {
		return true, err
	}

	return false, nil
}

func handleCreatePayment(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	var payment Payment
	if err := json.Unmarshal(in.Data, &payment); err != nil {
		return nil, err
	}

	payment.PaymentID = uuid.New().String()
	payment.Status = "pending"
	payment.PaymentMethod = "credit_card"
	payment.CreatedAt = "2026-07-01T00:00:00Z"

	paymentData, _ := json.Marshal(payment)
	err := daprClient.SaveState(ctx, stateStoreName, "payment-"+payment.PaymentID, paymentData, nil)
	if err != nil {
		return nil, err
	}

	data, _ := json.Marshal(payment)
	return &common.Content{
		Data:        data,
		ContentType: "application/json",
	}, nil
}

func handleProcessPayment(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	var req map[string]interface{}
	if err := json.Unmarshal(in.Data, &req); err != nil {
		return nil, err
	}

	payment := Payment{
		PaymentID:     uuid.New().String(),
		OrderID:       req["orderId"].(string),
		Amount:        req["amount"].(float64),
		Status:        "completed",
		PaymentMethod: "credit_card",
		CreatedAt:     time.Now().Format(time.RFC3339),
	}

	if db.DB != nil {
		db.DB.ExecContext(ctx,
			`INSERT INTO payments (payment_id, order_id, amount, status, payment_method, created_at)
			 VALUES ($1, $2, $3, $4, $5, $6)`,
			payment.PaymentID, payment.OrderID, payment.Amount,
			payment.Status, payment.PaymentMethod, payment.CreatedAt,
		)
	}

	paymentData, _ := json.Marshal(payment)
	daprClient.SaveState(ctx, stateStoreName, "payment-"+payment.PaymentID, paymentData, nil)

	data, _ := json.Marshal(payment)
	return &common.Content{
		Data:        data,
		ContentType: "application/json",
	}, nil
}

func handleGetPayment(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	var req map[string]string
	if err := json.Unmarshal(in.Data, &req); err != nil {
		return nil, err
	}

	item, err := daprClient.GetState(ctx, stateStoreName, "payment-"+req["paymentId"], nil)
	if err != nil {
		return nil, err
	}

	var payment Payment
	if err := json.Unmarshal(item.Value, &payment); err != nil {
		return nil, err
	}

	data, _ := json.Marshal(payment)
	return &common.Content{
		Data:        data,
		ContentType: "application/json",
	}, nil
}
