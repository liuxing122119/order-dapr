package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dapr/go-sdk/client"
)

type OrderWorkflowInput struct {
	OrderID     string      `json:"orderId"`
	UserID      string      `json:"userId"`
	Items       []OrderItem `json:"items"`
	TotalAmount float64     `json:"totalAmount"`
}

type OrderWorkflowOutput struct {
	OrderID          string `json:"orderId"`
	Status           string `json:"status"`
	InventoryValid   bool   `json:"inventoryValid"`
	PaymentSuccess   bool   `json:"paymentSuccess"`
	InventoryUpdated bool   `json:"inventoryUpdated"`
	NotificationSent bool   `json:"notificationSent"`
	Error            string `json:"error,omitempty"`
}

func ExecuteOrderProcessingWorkflow(ctx context.Context, input OrderWorkflowInput) (OrderWorkflowOutput, error) {
	startTime := time.Now()

	wfCtx := recordWorkflowStart(ctx, "OrderProcessingWorkflow")
	defer func() {
		recordWorkflowComplete(wfCtx, "OrderProcessingWorkflow", "completed", time.Since(startTime))
	}()

	output := OrderWorkflowOutput{
		OrderID: input.OrderID,
	}

	output = executeValidateInventoryActivity(wfCtx, output, input)
	if output.Status == "failed" || output.Status == "inventory_unavailable" {
		return output, fmt.Errorf("workflow failed at inventory validation")
	}

	output = executeProcessPaymentActivity(wfCtx, output, input)
	if output.Status == "failed" || output.Status == "payment_failed" {
		return output, fmt.Errorf("workflow failed at payment processing")
	}

	output = executeUpdateInventoryActivity(wfCtx, output, input)
	if output.Status == "failed" || output.Status == "inventory_update_failed" {
		return output, fmt.Errorf("workflow failed at inventory update")
	}

	output = executeSendNotificationActivity(wfCtx, output, input)

	output.Status = "completed"

	return output, nil
}

func executeValidateInventoryActivity(ctx context.Context, output OrderWorkflowOutput, input OrderWorkflowInput) OrderWorkflowOutput {

	activityStart := time.Now()
	defer func() {
		success := output.Error == ""
		recordActivityExecution(ctx, "ValidateInventoryActivity", success)
		_ = time.Since(activityStart)
	}()

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
	resp, err := daprClient.InvokeMethodWithContent(ctx, "inventory-service", "/inventory/check", "post", content)
	if err != nil {
		output.Status = "failed"
		output.InventoryValid = false
		output.Error = fmt.Sprintf("inventory check failed: %v", err)
		return output
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
		return output
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
		return output
	}

	output.InventoryValid = true
	return output
}

func executeProcessPaymentActivity(ctx context.Context, output OrderWorkflowOutput, input OrderWorkflowInput) OrderWorkflowOutput {

	activityStart := time.Now()
	defer func() {
		success := output.Error == ""
		recordActivityExecution(ctx, "ProcessPaymentActivity", success)
		_ = time.Since(activityStart)
	}()

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
		Description: fmt.Sprintf("Order %s - Dapr Workflow Engine", input.OrderID),
	}

	reqBytes, _ := json.Marshal(paymentReq)
	content := &client.DataContent{
		ContentType: "application/json",
		Data:        reqBytes,
	}

	resp, err := daprClient.InvokeMethodWithContent(ctx, "payment-service", "/payment/process", "post", content)
	if err != nil {
		output.Status = "failed"
		output.PaymentSuccess = false
		output.Error = fmt.Sprintf("payment failed: %v", err)
		return output
	}

	var payResp struct {
		PaymentID string `json:"paymentId"`
		Status    string `json:"status"`
	}

	if err := json.Unmarshal(resp, &payResp); err != nil {
		output.Status = "failed"
		output.PaymentSuccess = false
		output.Error = fmt.Sprintf("parse error: %v", err)
		return output
	}

	if payResp.Status != "success" && payResp.Status != "completed" {
		output.Status = "payment_failed"
		output.PaymentSuccess = false
		output.Error = fmt.Sprintf("payment status: %s", payResp.Status)
		return output
	}

	output.PaymentSuccess = true
	return output
}

func executeUpdateInventoryActivity(ctx context.Context, output OrderWorkflowOutput, input OrderWorkflowInput) OrderWorkflowOutput {

	activityStart := time.Now()
	defer func() {
		success := output.Error == ""
		recordActivityExecution(ctx, "UpdateInventoryActivity", success)
		_ = time.Since(activityStart)
	}()

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

		resp, err := daprClient.InvokeMethodWithContent(ctx, "inventory-service", "/inventory/update", "post", content)
		if err != nil {
			output.Status = "failed"
			output.InventoryUpdated = false
			output.Error = fmt.Sprintf("inventory update failed: %v", err)
			return output
		}

		var updateResp struct {
			Success bool   `json:"success"`
			Message string `json:"message"`
		}

		if err := json.Unmarshal(resp, &updateResp); err != nil {
			output.Status = "failed"
			output.InventoryUpdated = false
			output.Error = fmt.Sprintf("parse error: %v", err)
			return output
		}

		if !updateResp.Success {
			output.Status = "failed"
			output.InventoryUpdated = false
			output.Error = updateResp.Message
			return output
		}
	}

	output.InventoryUpdated = true
	return output
}

func executeSendNotificationActivity(ctx context.Context, output OrderWorkflowOutput, input OrderWorkflowInput) OrderWorkflowOutput {

	activityStart := time.Now()
	defer func() {
		success := output.Error == ""
		recordActivityExecution(ctx, "SendNotificationActivity", success)
		_ = time.Since(activityStart)
	}()

	notificationMsg := map[string]interface{}{
		"order_id":        input.OrderID,
		"user_id":         input.UserID,
		"status":          output.Status,
		"total_amount":    input.TotalAmount,
		"message":         fmt.Sprintf("Order %s has been processed successfully via Dapr Workflow Engine", input.OrderID),
		"timestamp":       time.Now().Format(time.RFC3339),
		"workflow_engine": "dapr-workflow-engine",
		"activities_completed": []string{
			"ValidateInventoryActivity",
			"ProcessPaymentActivity",
			"UpdateInventoryActivity",
			"SendNotificationActivity",
		},
	}

	msgBytes, _ := json.Marshal(notificationMsg)

	err := daprClient.PublishEvent(ctx, "pubsub", "orders", msgBytes)
	if err != nil {
		output.NotificationSent = false
	} else {
		output.NotificationSent = true
	}

	return output
}
