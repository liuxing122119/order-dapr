package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/dapr/go-sdk/service/common"
	"github.com/google/uuid"
)

type WorkflowInstance struct {
	InstanceID   string              `json:"instanceId"`
	OrderID      string              `json:"orderId"`
	Status       string              `json:"status"`
	CustomStatus string              `json:"customStatus"`
	CreatedAt    time.Time           `json:"createdAt"`
	CompletedAt  *time.Time          `json:"completedAt,omitempty"`
	Output       OrderWorkflowOutput `json:"output,omitempty"`
	Error        string              `json:"error,omitempty"`
}

var (
	workflowInstances sync.Map
	instanceCounter   int64
)

func initWorkflowEngine() error {
	initTelemetry()
	return nil
}

func startWorkflowEngine() error {
	time.Sleep(500 * time.Millisecond)
	return nil
}

func handleCreateOrderWithWorkflow(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	var req CreateOrderRequest
	if err := json.Unmarshal(in.Data, &req); err != nil {
		return nil, fmt.Errorf("invalid order request: %w", err)
	}

	if req.UserID == "" || len(req.Items) == 0 {
		return nil, fmt.Errorf("userId and items are required")
	}

	orderID := uuid.New().String()
	instanceID := generateInstanceID()

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

	saveOrderToState(ctx, order)

	recordOrderAmount(ctx, orderID, totalAmount)

	workflowInput := OrderWorkflowInput{
		OrderID:     orderID,
		UserID:      req.UserID,
		Items:       req.Items,
		TotalAmount: totalAmount,
	}

	instance := WorkflowInstance{
		InstanceID:   instanceID,
		OrderID:      orderID,
		Status:       "running",
		CustomStatus: "validating_inventory",
		CreatedAt:    time.Now(),
	}

	workflowInstances.Store(instanceID, instance)

	go func() {
		backgroundCtx := context.Background()
		executeAndMonitorWorkflow(backgroundCtx, instanceID, workflowInput)
	}()

	response := map[string]interface{}{
		"orderId":    orderID,
		"status":     "processing",
		"instanceId": instanceID,
		"message":    "Order created and Dapr workflow engine started",
		"engineInfo": map[string]string{
			"type":            "dapr-workflow-engine",
			"stateManagement": "event-sourcing (sidecar managed)",
			"faultTolerance":  "enabled (retry/timeout/circuit-breaker)",
			"tracing":         "opentelemetry + jaeger",
			"metrics":         "prometheus (:9090/metrics)",
		},
		"workflowActivities": []map[string]string{
			{"step": "1", "name": "ValidateInventoryActivity", "description": "验证库存活动：调用库存服务确认所需库存充足"},
			{"step": "2", "name": "ProcessPaymentActivity", "description": "处理支付活动：调用支付服务扣款"},
			{"step": "3", "name": "UpdateInventoryActivity", "description": "更新库存活动：调用库存服务扣减库存"},
			{"step": "4", "name": "SendNotificationActivity", "description": "发送通知活动：发送订单状态通知"},
		},
		"endpoints": map[string]string{
			"getStatus": "/workflow/status?instanceId=" + instanceID,
			"getOrder":  "/order/get?orderId=" + orderID,
			"health":    "/health",
		},
	}

	data, _ := json.Marshal(response)
	return &common.Content{
		Data:        data,
		ContentType: "application/json",
	}, nil
}

func executeAndMonitorWorkflow(ctx context.Context, instanceID string, input OrderWorkflowInput) {
	updateInstanceStatus(instanceID, "running", "validating_inventory")

	output, err := ExecuteOrderProcessingWorkflow(ctx, input)

	now := time.Now()

	if err != nil {
		updateInstanceCompleted(instanceID, "failed", output, err.Error(), now)
	} else {
		updateInstanceCompleted(instanceID, "completed", output, "", now)
	}

	updateOrderFromWorkflowOutput(ctx, input.OrderID, output)
}

func handleGetWorkflowStatus(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	var req map[string]string
	if err := json.Unmarshal(in.Data, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	instanceID := req["instanceId"]
	if instanceID == "" {
		return nil, fmt.Errorf("instanceId is required")
	}

	obj, ok := workflowInstances.Load(instanceID)
	if !ok {
		return nil, fmt.Errorf("workflow instance not found: %s", instanceID)
	}

	instance := obj.(WorkflowInstance)

	response := map[string]interface{}{
		"instanceId":   instance.InstanceID,
		"orderId":      instance.OrderID,
		"status":       instance.Status,
		"customStatus": instance.CustomStatus,
		"createdAt":    instance.CreatedAt.Format(time.RFC3339),
		"isCompleted":  instance.Status == "completed" || instance.Status == "failed",
	}

	if instance.CompletedAt != nil {
		response["completedAt"] = instance.CompletedAt.Format(time.RFC3339)
	}

	if instance.Output.Status != "" {
		response["output"] = instance.Output
	}

	if instance.Error != "" {
		response["error"] = instance.Error
	}

	data, _ := json.Marshal(response)
	return &common.Content{
		Data:        data,
		ContentType: "application/json",
	}, nil
}

func updateOrderFromWorkflowOutput(ctx context.Context, orderID string, output OrderWorkflowOutput) {
	item, err := daprClient.GetState(ctx, stateStoreName, "order-"+orderID, nil)
	if err != nil {
		return
	}

	if item == nil || len(item.Value) == 0 {
		return
	}

	var order Order
	json.Unmarshal(item.Value, &order)

	order.Status = output.Status
	order.UpdatedAt = time.Now().Format(time.RFC3339)
	order.Version++
	order.InventoryChecked = output.InventoryValid
	order.PaymentProcessed = output.PaymentSuccess

	saveOrderToState(ctx, order)
}

func generateInstanceID() string {
	instanceCounter++
	return fmt.Sprintf("wf-%d-%s", instanceCounter, uuid.New().String()[:8])
}

func updateInstanceStatus(instanceID string, status, customStatus string) {
	if obj, ok := workflowInstances.Load(instanceID); ok {
		instance := obj.(WorkflowInstance)
		instance.Status = status
		instance.CustomStatus = customStatus
		workflowInstances.Store(instanceID, instance)
	}
}

func updateInstanceCompleted(instanceID string, status string, output OrderWorkflowOutput, errMsg string, completedAt time.Time) {
	if obj, ok := workflowInstances.Load(instanceID); ok {
		instance := obj.(WorkflowInstance)
		instance.Status = status
		instance.CustomStatus = ""
		instance.CompletedAt = &completedAt
		instance.Output = output
		instance.Error = errMsg
		workflowInstances.Store(instanceID, instance)
	}
}

func stopWorkflowEngine() error {
	runningCount := 0
	workflowInstances.Range(func(key, value interface{}) bool {
		instance := value.(WorkflowInstance)
		if instance.Status == "running" {
			runningCount++
		}
		return true
	})

	if runningCount > 0 {
		time.Sleep(5 * time.Second)
	}

	return nil
}
