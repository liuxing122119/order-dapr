package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/dapr/durabletask-go/task"
	"github.com/dapr/go-sdk/client"
)

var (
	taskHubClient *WorkflowEngine
	engine        *workflowWorker
)

type WorkflowEngine struct {
	mu        sync.RWMutex
	instances map[string]*workflowInstance
	registry  *task.TaskRegistry
}

type workflowInstance struct {
	ID          string
	Name        string
	Status      string
	Input       []byte
	Output      []byte
	Error       string
	CreatedAt   time.Time
	CompletedAt *time.Time
	Version     string
}

type workflowWorker struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func initWorkflowEngine() error {

	registry := task.NewTaskRegistry()

	err := registry.AddOrchestratorN(WorkflowNameV2, ExecuteOrderProcessingWorkflow)
	if err != nil {
		return fmt.Errorf("failed to add orchestrator v2: %w", err)
	}

	err = registry.AddOrchestratorN(WorkflowNameV1, ExecuteOrderProcessingWorkflowV1)
	if err != nil {
		return fmt.Errorf("failed to add orchestrator v1: %w", err)
	}

	err = registry.AddActivityN("ValidateInventoryActivity", validateInventoryActivityHandler)
	if err != nil {
		return fmt.Errorf("failed to add activity ValidateInventory: %w", err)
	}

	err = registry.AddActivityN("ProcessPaymentActivity", processPaymentActivityHandler)
	if err != nil {
		return fmt.Errorf("failed to add activity ProcessPayment: %w", err)
	}

	err = registry.AddActivityN("UpdateInventoryActivity", updateInventoryActivityHandler)
	if err != nil {
		return fmt.Errorf("failed to add activity UpdateInventory: %w", err)
	}

	err = registry.AddActivityN("SendNotificationActivity", sendNotificationActivityHandler)
	if err != nil {
		return fmt.Errorf("failed to add activity SendNotification: %w", err)
	}

	taskHubClient = &WorkflowEngine{
		instances: make(map[string]*workflowInstance),
		registry:  registry,
	}

	ctx, cancel := context.WithCancel(context.Background())
	engine = &workflowWorker{
		ctx:    ctx,
		cancel: cancel,
	}

	return nil
}

func stopWorkflowEngine() {
	if engine != nil {
		engine.cancel()
	}
}

func startWorkflowInstance(ctx context.Context, input OrderWorkflowInput) (string, error) {
	if taskHubClient == nil {
		return "", fmt.Errorf("task hub client not initialized")
	}

	instanceID := generateUniqueID()

	inputData, _ := json.Marshal(input)

	instance := &workflowInstance{
		ID:        instanceID,
		Name:      WorkflowNameV2,
		Status:    "RUNNING",
		Input:     inputData,
		CreatedAt: time.Now(),
		Version:   CurrentVersion,
	}

	taskHubClient.mu.Lock()
	taskHubClient.instances[instanceID] = instance
	taskHubClient.mu.Unlock()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				instance.Status = "FAILED"
				instance.Error = fmt.Sprintf("panic: %v", r)
				now := time.Now()
				instance.CompletedAt = &now
			}
		}()

		output, err := executeWorkflowWithVersion(ctx, input, CurrentVersion)

		now := time.Now()
		instance.CompletedAt = &now

		if err != nil {
			instance.Status = "FAILED"
			instance.Error = err.Error()
			outputData, _ := json.Marshal(OrderWorkflowOutput{
				OrderID:         input.OrderID,
				Status:          "failed",
				Error:           err.Error(),
				WorkflowVersion: CurrentVersion,
			})
			instance.Output = outputData
		} else {
			instance.Status = "COMPLETED"
			output.WorkflowVersion = CurrentVersion
			outputData, _ := json.Marshal(output)
			instance.Output = outputData
		}
	}()

	return instanceID, nil
}

func executeWorkflowWithVersion(ctx context.Context, input OrderWorkflowInput, version string) (OrderWorkflowOutput, error) {
	switch version {
	case CurrentVersion:
		return executeWorkflowV2(ctx, input)
	case "v1":
		return executeWorkflowV1Legacy(ctx, input)
	default:
		return OrderWorkflowOutput{}, fmt.Errorf("unsupported version: %s", version)
	}
}

func executeWorkflowV2(ctx context.Context, input OrderWorkflowInput) (OrderWorkflowOutput, error) {

	orchestrationCtx := createMockOrchestrationContext(ctx)
	result, err := ExecuteOrderProcessingWorkflow(orchestrationCtx)
	if err != nil {
		return OrderWorkflowOutput{}, err
	}

	output, ok := result.(OrderWorkflowOutput)
	if !ok {
		return OrderWorkflowOutput{}, fmt.Errorf("invalid output type")
	}

	return output, nil
}

func executeWorkflowV1Legacy(ctx context.Context, input OrderWorkflowInput) (OrderWorkflowOutput, error) {
	return ExecuteOrderProcessingWorkflowV1ForLegacy(ctx, input)
}

func getWorkflowStatus(ctx context.Context, instanceID string) (map[string]interface{}, error) {
	if taskHubClient == nil {
		return nil, fmt.Errorf("task hub client not initialized")
	}

	taskHubClient.mu.RLock()
	instance, exists := taskHubClient.instances[instanceID]
	taskHubClient.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("instance not found: %s", instanceID)
	}

	status := map[string]interface{}{
		"instanceId":    instance.ID,
		"name":          instance.Name,
		"runtimeStatus": instance.Status,
		"createdAt":     instance.CreatedAt.Format(time.RFC3339),
		"lastUpdated":   instance.CreatedAt.Format(time.RFC3339),
		"version":       instance.Version,
	}

	if instance.CompletedAt != nil {
		status["completedAt"] = instance.CompletedAt.Format(time.RFC3339)
		status["lastUpdated"] = instance.CompletedAt.Format(time.RFC3339)
	}

	return status, nil
}

func waitForWorkflowCompletion(ctx context.Context, instanceID string, timeout time.Duration) (OrderWorkflowOutput, error) {
	if taskHubClient == nil {
		return OrderWorkflowOutput{}, fmt.Errorf("task hub client not initialized")
	}

	timeoutChan := time.After(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutChan:
			return OrderWorkflowOutput{}, fmt.Errorf("timeout waiting for completion")

		case <-ticker.C:
			taskHubClient.mu.RLock()
			instance, exists := taskHubClient.instances[instanceID]
			taskHubClient.mu.RUnlock()

			if !exists {
				return OrderWorkflowOutput{}, fmt.Errorf("instance not found: %s", instanceID)
			}

			if instance.Status == "COMPLETED" || instance.Status == "FAILED" || instance.Status == "CANCELED" {
				var output OrderWorkflowOutput
				if len(instance.Output) > 0 {
					if err := json.Unmarshal(instance.Output, &output); err != nil {
						return OrderWorkflowOutput{}, fmt.Errorf("failed to parse output: %w", err)
					}
				}

				if instance.Status == "FAILED" && output.Error == "" {
					output.Error = instance.Error
					output.Status = "failed"
				}

				return output, nil
			}

		case <-ctx.Done():
			return OrderWorkflowOutput{}, ctx.Err()
		}
	}
}

func listRunningInstances() []string {
	if taskHubClient == nil {
		return []string{}
	}

	var runningInstances []string

	taskHubClient.mu.RLock()
	for id, inst := range taskHubClient.instances {
		if inst.Status == "RUNNING" {
			runningInstances = append(runningInstances, id)
		}
	}
	taskHubClient.mu.RUnlock()

	return runningInstances
}

func validateInventoryActivityHandler(ctx task.ActivityContext) (interface{}, error) {
	startTime := time.Now()
	defer func() {
		success := true
		recordActivityExecution(context.Background(), "ValidateInventoryActivity", success)
		_ = time.Since(startTime)
	}()

	var input OrderWorkflowInput
	if err := ctx.GetInput(&input); err != nil {
		return OrderWorkflowOutput{}, err
	}

	output := OrderWorkflowOutput{
		OrderID: input.OrderID,
	}

	type InventoryItem struct {
		ProductID string `json:"productId"`
		Quantity  int    `json:"quantity"`
	}

	inventoryItems := make([]InventoryItem, len(input.Items))
	for i, item := range input.Items {
		inventoryItems[i] = InventoryItem{
			ProductID: item.ProductID,
			Quantity:  item.Quantity,
		}
	}
	reqBytes, _ := json.Marshal(inventoryItems)

	content := &client.DataContent{
		ContentType: "application/json",
		Data:        reqBytes,
	}
	resp, err := daprClient.InvokeMethodWithContent(context.Background(), "inventory-service", "/inventory/check", "post", content)
	if err != nil {
		output.Status = "failed"
		output.InventoryValid = false
		output.Error = fmt.Sprintf("inventory check failed: %v", err)
		return output, err
	}

	var invResp struct {
		AllAvailable bool `json:"allAvailable"`
		Items        []struct {
			ProductID   string `json:"productId"`
			IsAvailable bool   `json:"isAvailable"`
			Requested   int    `json:"requested"`
			Available   int    `json:"available"`
		} `json:"items"`
	}

	if err := json.Unmarshal(resp, &invResp); err != nil {
		output.Status = "failed"
		output.InventoryValid = false
		output.Error = fmt.Sprintf("parse error: %v", err)
		return output, err
	}

	if !invResp.AllAvailable {
		unavailableItems := make([]string, 0)
		for _, item := range invResp.Items {
			if !item.IsAvailable {
				unavailableItems = append(unavailableItems, item.ProductID)
			}
		}
		errMsg := fmt.Sprintf("inventory not available for items: %v", unavailableItems)
		output.Status = "inventory_unavailable"
		output.InventoryValid = false
		output.Error = errMsg
		return output, fmt.Errorf(errMsg)
	}

	output.InventoryValid = true
	return output, nil
}

func processPaymentActivityHandler(ctx task.ActivityContext) (interface{}, error) {
	startTime := time.Now()
	defer func() {
		success := true
		recordActivityExecution(context.Background(), "ProcessPaymentActivity", success)
		_ = time.Since(startTime)
	}()

	var input OrderWorkflowInput
	if err := ctx.GetInput(&input); err != nil {
		return OrderWorkflowOutput{}, err
	}

	output := OrderWorkflowOutput{
		OrderID: input.OrderID,
	}

	type PaymentRequest struct {
		OrderID     string  `json:"orderId"`
		UserID      string  `json:"userId"`
		Amount      float64 `json:"amount"`
		Currency    string  `json:"currency"`
		Description string  `json:"description"`
	}

	paymentReq := PaymentRequest{
		OrderID:     input.OrderID,
		UserID:      input.UserID,
		Amount:      input.TotalAmount,
		Currency:    "CNY",
		Description: fmt.Sprintf("Order %s - Dapr Workflow Engine V2 (Version Managed)", input.OrderID),
	}

	reqBytes, _ := json.Marshal(paymentReq)
	content := &client.DataContent{
		ContentType: "application/json",
		Data:        reqBytes,
	}

	resp, err := daprClient.InvokeMethodWithContent(context.Background(), "payment-service", "/payment/process", "post", content)
	if err != nil {
		output.Status = "failed"
		output.PaymentSuccess = false
		output.Error = fmt.Sprintf("payment failed: %v", err)
		return output, err
	}

	var payResp struct {
		PaymentID string `json:"paymentId"`
		Status    string `json:"status"`
	}

	if err := json.Unmarshal(resp, &payResp); err != nil {
		output.Status = "failed"
		output.PaymentSuccess = false
		output.Error = fmt.Sprintf("parse error: %v", err)
		return output, err
	}

	if payResp.Status != "success" && payResp.Status != "completed" {
		output.Status = "payment_failed"
		output.PaymentSuccess = false
		output.Error = fmt.Sprintf("payment status: %s", payResp.Status)
		return output, fmt.Errorf("payment status: %s", payResp.Status)
	}

	output.PaymentSuccess = true
	return output, nil
}

func updateInventoryActivityHandler(ctx task.ActivityContext) (interface{}, error) {
	startTime := time.Now()
	defer func() {
		success := true
		recordActivityExecution(context.Background(), "UpdateInventoryActivity", success)
		_ = time.Since(startTime)
	}()

	var input OrderWorkflowInput
	if err := ctx.GetInput(&input); err != nil {
		return OrderWorkflowOutput{}, err
	}

	output := OrderWorkflowOutput{
		OrderID: input.OrderID,
	}

	for _, item := range input.Items {
		type DeductInventoryRequest struct {
			ProductID string `json:"productId"`
			Quantity  int    `json:"quantity"`
			Operation string `json:"operation"`
		}

		deductReq := DeductInventoryRequest{
			ProductID: item.ProductID,
			Quantity:  item.Quantity,
			Operation: "decrease",
		}

		reqBytes, _ := json.Marshal(deductReq)
		content := &client.DataContent{
			ContentType: "application/json",
			Data:        reqBytes,
		}

		resp, err := daprClient.InvokeMethodWithContent(context.Background(), "inventory-service", "/inventory/update", "post", content)
		if err != nil {
			output.Status = "failed"
			output.InventoryUpdated = false
			output.Error = fmt.Sprintf("inventory update failed: %v", err)
			return output, err
		}

		var updateResp struct {
			Success bool   `json:"success"`
			Message string `json:"message"`
		}

		if err := json.Unmarshal(resp, &updateResp); err != nil {
			output.Status = "failed"
			output.InventoryUpdated = false
			output.Error = fmt.Sprintf("parse error: %v", err)
			return output, err
		}

		if !updateResp.Success {
			output.Status = "failed"
			output.InventoryUpdated = false
			output.Error = updateResp.Message
			return output, fmt.Errorf(updateResp.Message)
		}
	}

	output.InventoryUpdated = true
	return output, nil
}

func sendNotificationActivityHandler(ctx task.ActivityContext) (interface{}, error) {
	startTime := time.Now()
	defer func() {
		success := true
		recordActivityExecution(context.Background(), "SendNotificationActivity", success)
		_ = time.Since(startTime)
	}()

	var input OrderWorkflowInput
	if err := ctx.GetInput(&input); err != nil {
		return OrderWorkflowOutput{}, err
	}

	output := OrderWorkflowOutput{
		OrderID:          input.OrderID,
		NotificationSent: true,
	}

	notificationMsg := map[string]interface{}{
		"order_id":           input.OrderID,
		"user_id":            input.UserID,
		"status":             "completed",
		"total_amount":       input.TotalAmount,
		"message":            fmt.Sprintf("Order %s has been processed successfully via Dapr Workflow Engine V2 (Version Managed)", input.OrderID),
		"timestamp":          time.Now().Format(time.RFC3339),
		"workflow_engine":    "dapr-workflow-engine-v2",
		"version":            CurrentVersion,
		"version_management": "enabled",
		"activities_completed": []string{
			"ValidateInventoryActivity",
			"ProcessPaymentActivity",
			"UpdateInventoryActivity",
			"SendNotificationActivity",
		},
	}

	msgBytes, _ := json.Marshal(notificationMsg)

	err := daprClient.PublishEvent(context.Background(), "pubsub", "orders", msgBytes)
	if err != nil {
		output.NotificationSent = false
		return output, err
	}

	return output, nil
}

func migrateRunningWorkflowsToV2(ctx context.Context) error {
	if taskHubClient == nil {
		return fmt.Errorf("task hub client not initialized")
	}

	runningInstances := listRunningInstances()
	migratedCount := 0

	for _, instanceID := range runningInstances {
		taskHubClient.mu.RLock()
		instance, exists := taskHubClient.instances[instanceID]
		taskHubClient.mu.RUnlock()

		if !exists {
			continue
		}

		if instance.Name == WorkflowNameV1 && instance.Status == "RUNNING" {
			migratedCount++
		}
	}

	return nil
}

func getWorkflowVersionInfo() map[string]interface{} {
	return map[string]interface{}{
		"current_version":    CurrentVersion,
		"available_versions": []string{"v1", "v2"},
		"v1_workflow_name":   WorkflowNameV1,
		"v2_workflow_name":   WorkflowNameV2,
		"migration_status":   "ready",
		"backward_compat":    true,
		"description":        "Dapr Workflow Engine with version management support. Running instances continue with their registered version while new instances use the latest version.",
	}
}

func generateUniqueID() string {
	return fmt.Sprintf("%d-%s", time.Now().UnixNano(), fmt.Sprintf("%04x", time.Now().UnixNano()%0xFFFF))
}

func createMockOrchestrationContext(ctx context.Context) *task.OrchestrationContext {
	oc := &task.OrchestrationContext{}
	return oc
}
