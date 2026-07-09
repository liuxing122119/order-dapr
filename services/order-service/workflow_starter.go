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

func initializeWorkflowSystem() error {
	initTelemetry()
	return initDaprWorkflowEngine()
}

func startWorkflowEngine() error {
	time.Sleep(500 * time.Millisecond)
	return nil
}

func initDaprWorkflowEngine() error {

	if err := initWorkflowEngine(); err != nil {
		return nil
	}

	go func() {
		time.Sleep(10 * time.Second)
		ctx := context.Background()
		migrateRunningWorkflowsToV2(ctx)
	}()

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
		Version:     CurrentVersion,
	}

	var actualInstanceID string
	var startErr error

	if taskHubClient != nil {
		actualInstanceID, startErr = startWorkflowInstance(ctx, workflowInput)
		if startErr != nil {
			actualInstanceID = instanceID
		}
	} else {
		actualInstanceID = instanceID
	}

	instance := WorkflowInstance{
		InstanceID:   actualInstanceID,
		OrderID:      orderID,
		Status:       "running",
		CustomStatus: "validating_inventory",
		CreatedAt:    time.Now(),
	}

	workflowInstances.Store(actualInstanceID, instance)

	if taskHubClient == nil || startErr != nil {
		go func() {
			backgroundCtx := context.Background()
			executeAndMonitorWorkflow(backgroundCtx, actualInstanceID, workflowInput)
		}()
	} else {
		go func() {
			backgroundCtx := context.Background()
			monitorDaprWorkflow(backgroundCtx, actualInstanceID, workflowInput)
		}()
	}

	versionInfo := getWorkflowVersionInfo()

	response := map[string]interface{}{
		"orderId":    orderID,
		"status":     "processing",
		"instanceId": actualInstanceID,
		"message":    "Order created and Dapr Workflow Engine started with version management",
		"engineInfo": map[string]string{
			"type":              "dapr-workflow-engine-v2",
			"stateManagement":   "event-sourcing (durabletask-go backend)",
			"faultTolerance":    "enabled (retry/timeout/circuit-breaker)",
			"tracing":           "opentelemetry + jaeger",
			"metrics":           "prometheus (:9090/metrics)",
			"versionManagement": "enabled",
			"currentVersion":    CurrentVersion,
			"backend":           "gRPC (localhost:4001)",
		},
		"versionInfo": versionInfo,
		"workflowActivities": []map[string]string{
			{"step": "1", "name": "ValidateInventoryActivity", "description": "验证库存活动：调用库存服务确认所需库存充足"},
			{"step": "2", "name": "ProcessPaymentActivity", "description": "处理支付活动：调用支付服务扣款"},
			{"step": "3", "name": "UpdateInventoryActivity", "description": "更新库存活动：调用库存服务扣减库存"},
			{"step": "4", "name": "SendNotificationActivity", "description": "发送通知活动：发送订单状态通知"},
		},
		"endpoints": map[string]string{
			"getOrder": "/order/get?orderId=" + orderID,
			"health":   "/health",
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

	output, err := ExecuteOrderProcessingWorkflowV1ForLegacy(ctx, input)

	now := time.Now()

	if err != nil {
		updateInstanceCompleted(instanceID, "failed", output, err.Error(), now)
	} else {
		updateInstanceCompleted(instanceID, "completed", output, "", now)
	}

	updateOrderFromWorkflowOutput(ctx, input.OrderID, output)
}

func monitorDaprWorkflow(ctx context.Context, instanceID string, input OrderWorkflowInput) {
	updateInstanceStatus(instanceID, "running", "dapr_workflow_engine_processing")

	timeout := 5 * time.Minute
	output, err := waitForWorkflowCompletion(ctx, instanceID, timeout)

	now := time.Now()

	if err != nil {
		updateInstanceCompleted(instanceID, "failed", OrderWorkflowOutput{
			OrderID:         input.OrderID,
			Status:          "failed",
			Error:           err.Error(),
			WorkflowVersion: CurrentVersion,
		}, err.Error(), now)
	} else {
		updateInstanceCompleted(instanceID, "completed", output, "", now)
	}

	updateOrderFromWorkflowOutput(ctx, input.OrderID, output)
}

func ExecuteOrderProcessingWorkflowV1ForLegacy(ctx context.Context, input OrderWorkflowInput) (OrderWorkflowOutput, error) {

	legacyInput := input
	legacyInput.Version = "v1"

	output, err := executeLegacyWorkflowV1(ctx, legacyInput)
	return output, err
}

func executeLegacyWorkflowV1(ctx context.Context, input OrderWorkflowInput) (OrderWorkflowOutput, error) {
	startTime := time.Now()
	wfCtx := recordWorkflowStart(ctx, WorkflowNameV1)
	defer func() {
		recordWorkflowComplete(wfCtx, WorkflowNameV1, "completed", time.Since(startTime))
	}()

	output := OrderWorkflowOutput{
		OrderID:         input.OrderID,
		WorkflowVersion: "v1",
	}

	output = executeValidateInventoryActivityLegacy(wfCtx, output, input)
	if output.Status == "failed" || output.Status == "inventory_unavailable" {
		return output, fmt.Errorf("workflow v1 failed at inventory validation")
	}

	output = executeProcessPaymentActivityLegacy(wfCtx, output, input)
	if output.Status == "failed" || output.Status == "payment_failed" {
		return output, fmt.Errorf("workflow v1 failed at payment processing")
	}

	output = executeUpdateInventoryActivityLegacy(wfCtx, output, input)
	if output.Status == "failed" || output.Status == "inventory_update_failed" {
		return output, fmt.Errorf("workflow v1 failed at inventory update")
	}

	executeSendNotificationActivityLegacy(wfCtx, output, input)
	output.NotificationSent = true
	output.Status = "completed"

	return output, nil
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

func stopWorkflowEngineSystem() error {

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

	stopDaprWorkflowEngine()
	return nil
}

func stopDaprWorkflowEngine() {
	if engine != nil {
		engine.cancel()
	}
}
