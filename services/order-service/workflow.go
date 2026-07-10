// 工作流定义包 - 定义订单处理工作流的编排逻辑和活动实现
// 包含V1（旧版）和V2（新版）两个版本的工作流定义
package main

import (
	"context"     // 上下文控制库
	"encoding/json"  // JSON编解码库
	"fmt"         // 格式化输出库
	"time"        // 时间处理库

	"github.com/dapr/durabletask-go/task"   // Dapr持久化任务框架
	"github.com/dapr/go-sdk/client"         // Dapr Go SDK客户端
)

// 常量定义区域 - 工作流名称和版本标识
const (
	WorkflowNameV1 = "OrderProcessingWorkflow-v1"  // V1版工作流名称（旧版）
	WorkflowNameV2 = "OrderProcessingWorkflow-v2"  // V2版工作流名称（新版，当前使用）
	CurrentVersion = "v2"                          // 当前工作流版本标识
)

// OrderWorkflowInput 订单工作流输入结构体
// 作为工作流启动时的输入参数
type OrderWorkflowInput struct {
	OrderID     string      `json:"orderId"`     // 订单ID
	UserID      string      `json:"userId"`      // 用户ID
	Items       []OrderItem `json:"items"`       // 订单商品列表（OrderItem切片）
	TotalAmount float64     `json:"totalAmount"` // 订单总金额
	Version     string      `json:"version,omitempty"` // 工作流版本（可选字段）
}

// OrderWorkflowOutput 订单工作流输出结构体
// 作为工作流完成时的输出结果
type OrderWorkflowOutput struct {
	OrderID          string `json:"orderId"`          // 关联的订单ID
	Status           string `json:"status"`           // 最终状态（completed/failed等）
	InventoryValid   bool   `json:"inventoryValid"`   // 库存验证是否通过
	PaymentSuccess   bool   `json:"paymentSuccess"`   // 支付是否成功
	InventoryUpdated bool   `json:"inventoryUpdated"` // 库存是否已更新
	NotificationSent bool   `json:"notificationSent"` // 通知是否已发送
	Error            string `json:"error,omitempty"`  // 错误信息（可选）
	WorkflowVersion  string `json:"workflowVersion"`  // 使用的工作流版本
}

// ExecuteOrderProcessingWorkflow V2版订单处理工作流执行函数
// 功能：编排完整的订单处理流程（验证库存→处理支付→更新库存→发送通知）
// 参数：
//   - ctx *task.OrchestrationContext - Dapr编排上下文对象
// 返回值：
//   - any - 工作流输出结果（通常为OrderWorkflowOutput类型）
//   - error - 错误信息
func ExecuteOrderProcessingWorkflow(ctx *task.OrchestrationContext) (any, error) {
	// input 声明工作流输入变量
	input := OrderWorkflowInput{}
	// ctx.GetInput 从上下文中获取工作流输入参数并反序列化到input
	if err := ctx.GetInput(&input); err != nil {
		return OrderWorkflowOutput{}, err  // 获取失败返回空输出和错误
	}

	// startTime 记录工作流开始时间
	startTime := time.Now()
	// wfCtx 创建后台上下文用于记录遥测数据
	wfCtx := context.Background()
	// defer延迟函数调用 - 在工作流结束时记录完成指标
	defer func() {
		recordWorkflowComplete(wfCtx, ctx.Name, "completed", time.Since(startTime))
		// time.Since计算从startTime到现在经过的时间Duration
	}()

	// output 初始化工作流输出对象
	output := OrderWorkflowOutput{
		OrderID:         input.OrderID,        // 设置订单ID
		WorkflowVersion: CurrentVersion,       // 设置工作流版本为当前版本
	}

	// validateResult 验证库存活动结果变量（interface{}类型可接收任意类型）
	var validateResult interface{}
	// ctx.CallActivity 调用库存验证活动（异步等待结果）
	// task.WithActivityInput 设置活动的输入参数
	err := ctx.CallActivity("ValidateInventoryActivity",
		task.WithActivityInput(input),
	).Await(&validateResult)  // Await方法阻塞等待活动完成并将结果写入validateResult

	if err != nil {
		// if判断 - 库存验证活动执行失败
		output.Status = "failed"
		output.Error = fmt.Sprintf("inventory validation failed: %v", err)
		return output, err  // return返回错误输出
	}

	// 类型断言 - 将validateResult断言为OrderWorkflowOutput类型
	if validateResultVal, ok := validateResult.(OrderWorkflowOutput); ok {
		output.InventoryValid = validateResultVal.InventoryValid  // 获取库存验证结果
		// if判断 - 如果库存不可用，提前终止工作流
		if !validateResultVal.InventoryValid {
			output.Status = "inventory_unavailable"  // 设置状态为库存不足
			output.Error = validateResultVal.Error    // 设置错误信息
			return output, fmt.Errorf("inventory not available")  // 返回错误
		}
	}

	// paymentResult 支付处理活动结果变量
	var paymentResult interface{}
	// 调用支付处理活动
	err = ctx.CallActivity("ProcessPaymentActivity",
		task.WithActivityInput(input),
	).Await(&paymentResult)  // Await等待支付活动完成

	if err != nil {
		output.Status = "failed"
		output.Error = fmt.Sprintf("payment processing failed: %v", err)
		return output, err
	}

	// 类型断言获取支付结果
	if paymentResultVal, ok := paymentResult.(OrderWorkflowOutput); ok {
		output.PaymentSuccess = paymentResultVal.PaymentSuccess  // 获取支付成功标志
		// if判断 - 支付失败则终止工作流
		if !paymentResultVal.PaymentSuccess {
			output.Status = "payment_failed"
			output.Error = paymentResultVal.Error
			return output, fmt.Errorf("payment not completed")
		}
	}

	// updateResult 更新库存活动结果变量
	var updateResult interface{}
	// 调用更新库存活动
	err = ctx.CallActivity("UpdateInventoryActivity",
		task.WithActivityInput(input),
	).Await(&updateResult)

	if err != nil {
		output.Status = "failed"
		output.Error = fmt.Sprintf("inventory update failed: %v", err)
		return output, err
	}

	// 类型断言获取库存更新结果
	if updateResultVal, ok := updateResult.(OrderWorkflowOutput); ok {
		output.InventoryUpdated = updateResultVal.InventoryUpdated  // 获取库存更新标志
		// if判断 - 库存更新失败
		if !updateResultVal.InventoryUpdated {
			output.Status = "failed"
			output.Error = updateResultVal.Error
			return output, fmt.Errorf("inventory update failed")
		}
	}

	// 调用发送通知活动（不等待结果，fire-and-forget模式）
	ctx.CallActivity("SendNotificationActivity",
		task.WithActivityInput(input),
	)
	output.NotificationSent = true  // 标记通知已发送

	// 设置最终状态为完成
	output.Status = "completed"
	return output, nil  // return返回成功结果
}

// ExecuteOrderProcessingWorkflowV1 V1版订单处理工作流执行函数（旧版兼容）
// 功能：使用旧的直接服务调用方式编排订单处理流程
func ExecuteOrderProcessingWorkflowV1(ctx *task.OrchestrationContext) (any, error) {
	// input 输入参数
	input := OrderWorkflowInput{}
	if err := ctx.GetInput(&input); err != nil {
		return OrderWorkflowOutput{}, err
	}

	startTime := time.Now()
	wfCtx := context.Background()
	// defer延迟函数 - 工作流结束时记录遥测
	defer func() {
		recordWorkflowComplete(wfCtx, ctx.Name, "completed", time.Since(startTime))
	}()

	// output 初始化输出
	output := OrderWorkflowOutput{
		OrderID:         input.OrderID,
		WorkflowVersion: "v1",  // 版本标记为v1
	}

	// 按顺序调用各步骤的遗留实现函数
	output = executeValidateInventoryActivityLegacy(wfCtx, output, input)
	// if判断 - 检查库存验证步骤是否失败
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

	// 发送通知（不检查结果）
	executeSendNotificationActivityLegacy(wfCtx, output, input)
	output.NotificationSent = true
	output.Status = "completed"

	return output, nil
}

// executeValidateInventoryActivityLegacy V1版库存验证活动实现（遗留代码）
// 功能：通过Dapr服务调用直接调用库存服务的检查接口
// 参数：
//   - ctx context.Context - 上下文
//   - output OrderWorkflowOutput - 当前工作流输出（会更新）
//   - input OrderWorkflowInput - 工作流输入
// 返回值：
//   - OrderWorkflowOutput - 更新后的工作流输出
func executeValidateInventoryActivityLegacy(ctx context.Context, output OrderWorkflowInput, input OrderWorkflowInput) OrderWorkflowOutput {
	// activityStart 记录活动开始时间
	activityStart := time.Now()
	// defer延迟函数 - 活动结束时记录执行指标
	defer func() {
		success := output.Error == ""  // 无错误表示成功
		recordActivityExecution(ctx, "ValidateInventoryActivity", success)
		_ = time.Since(activityStart)  // _空白标识符忽略返回值
	}()

	// InventoryItem 内部使用的库存项结构体（匿名结构体）
	type InventoryItem struct {
		ProductID string `json:"productId"`  // 商品ID
		Quantity  int    `json:"quantity"`   // 数量
	}

	// make创建切片 - 分配与input.Items相同长度的InventoryItem切片
	inventoryItems := make([]InventoryItem, len(input.Items))
	// for range循环 - 将OrderItem转换为InventoryItem格式
	for i, item := range input.Items {  // i是索引，item是当前元素
		inventoryItems[i] = InventoryItem{
			ProductID: item.ProductID,
			Quantity:  item.Quantity,
		}
	}
	// json.Marshal 序列化为JSON字节数组
	reqBytes, _ := json.Marshal(inventoryItems)

	// client.DataContent 创建Dapr数据内容对象
	content := &client.DataContent{
		ContentType: "application/json",  // 内容类型
		Data:        reqBytes,             // JSON数据
	}
	// daprClient.InvokeMethodWithContent 调用库存服务接口
	// 参数：上下文、目标服务名、路径、HTTP方法、内容对象
	resp, err := daprClient.InvokeMethodWithContent(ctx, "inventory-service", "/inventory/check", "post", content)
	if err != nil {
		output.Status = "failed"
		output.InventoryValid = false
		output.Error = fmt.Sprintf("inventory check failed: %v", err)
		return output  // return返回错误输出
	}

	// invResp 库存响应结构体（匿名结构体）
	var invResp struct {
		AllAvailable bool `json:"allAvailable"`  // 所有商品是否都可用
		Items        []struct {                 // 各商品检查结果数组
			ProductID   string `json:"productId"`
			IsAvailable bool   `json:"isAvailable"`
			Requested   int    `json:"requested"`
			Available   int    `json:"available"`
		} `json:"items"`
	}

	// 反序列化响应数据
	if err := json.Unmarshal(resp, &invResp); err != nil {
		output.Status = "failed"
		output.InventoryValid = false
		output.Error = fmt.Sprintf("parse error: %v", err)
		return output
	}

	// if判断 - 检查所有商品是否都可用
	if !invResp.AllAvailable {
		unavailableItems := make([]string, 0)  // 创建空字符串切片存储不可用的商品ID
		for _, item := range invResp.Items {  // 遍历各商品检查结果
			if !item.IsAvailable {
				unavailableItems = append(unavailableItems, item.ProductID)  // append追加元素到切片
			}
		}
		errMsg := fmt.Sprintf("inventory not available for items: %v", unavailableItems)
		output.Status = "inventory_unavailable"
		output.InventoryValid = false
		output.Error = errMsg
		return output
	}

	output.InventoryValid = true  // 设置库存验证通过
	return output
}

// executeProcessPaymentActivityLegacy V1版支付处理活动实现（遗留代码）
// 功能：通过Dapr服务调用直接调用支付服务的处理接口
func executeProcessPaymentActivityLegacy(ctx context.Context, output OrderWorkflowOutput, input OrderWorkflowInput) OrderWorkflowOutput {
	activityStart := time.Now()
	defer func() {
		success := output.Error == ""
		recordActivityExecution(ctx, "ProcessPaymentActivity", success)
		_ = time.Since(activityStart)
	}()

	// PaymentRequest 支付请求内部结构体
	type PaymentRequest struct {
		OrderID     string  `json:"orderId"`     // 订单ID
		UserID      string  `json:"userId"`      // 用户ID
		Amount      float64 `json:"amount"`      // 金额
		Currency    string  `json:"currency"`    // 货币单位
		Description string  `json:"description"` // 描述信息
	}

	paymentReq := PaymentRequest{
		OrderID:     input.OrderID,
		UserID:      input.UserID,
		Amount:      input.TotalAmount,
		Currency:    "CNY",  // 使用人民币
		Description: fmt.Sprintf("Order %s - Dapr Workflow Engine V1", input.OrderID),  // 描述包含引擎版本
	}

	reqBytes, _ := json.Marshal(paymentReq)
	content := &client.DataContent{
		ContentType: "application/json",
		Data:        reqBytes,
	}

	// 调用支付服务接口
	resp, err := daprClient.InvokeMethodWithContent(ctx, "payment-service", "/payment/process", "post", content)
	if err != nil {
		output.Status = "failed"
		output.PaymentSuccess = false
		output.Error = fmt.Sprintf("payment failed: %v", err)
		return output
	}

	var payResp struct {
		PaymentID string `json:"paymentId"`  // 支付ID
		Status    string `json:"status"`     // 支付状态
	}

	if err := json.Unmarshal(resp, &payResp); err != nil {
		output.Status = "failed"
		output.PaymentSuccess = false
		output.Error = fmt.Sprintf("parse error: %v", err)
		return output
	}

	// if判断 - 检查支付状态是否成功或已完成
	if payResp.Status != "success" && payResp.Status != "completed" {
		output.Status = "payment_failed"
		output.PaymentSuccess = false
		output.Error = fmt.Sprintf("payment status: %s", payResp.Status)
		return output
	}

	output.PaymentSuccess = true  // 设置支付成功标志
	return output
}

// executeUpdateInventoryActivityLegacy V1版库存更新活动实现（遗留代码）
// 功能：遍历订单商品，逐个调用库存服务扣减库存
func executeUpdateInventoryActivityLegacy(ctx context.Context, output OrderWorkflowOutput, input OrderWorkflowInput) OrderWorkflowOutput {
	activityStart := time.Now()
	defer func() {
		success := output.Error == ""
		recordActivityExecution(ctx, "UpdateInventoryActivity", success)
		_ = time.Since(activityStart)
	}()

	// for range循环 - 遍历每个订单商品进行库存扣减
	for _, item := range input.Items {
		DeductInventoryRequest struct {
			ProductID string `json:"productId"`  // 商品ID
			Quantity  int    `json:"quantity"`   // 扣减数量
			Operation string `json:"operation"`  // 操作类型
		}

		deductReq := DeductInventoryRequest{
			ProductID: item.ProductID,
			Quantity:  item.Quantity,
			Operation: "decrease",  // 操作类型设为减少
		}

		reqBytes, _ := json.Marshal(deductReq)
		content := &client.DataContent{
			ContentType: "application/json",
			Data:        reqBytes,
		}

		// 调用库存服务更新接口
		resp, err := daprClient.InvokeMethodWithContent(ctx, "inventory-service", "/inventory/update", "post", content)
		if err != nil {
			output.Status = "failed"
			output.InventoryUpdated = false
			output.Error = fmt.Sprintf("inventory update failed: %v", err)
			return output  // 任一商品扣减失败立即返回
		}

		var updateResp struct {
			Success bool   `json:"success"`  // 是否成功
			Message string `json:"message"`  // 消息
		}

		if err := json.Unmarshal(resp, &updateResp); err != nil {
			output.Status = "failed"
			output.InventoryUpdated = false
			output.Error = fmt.Sprintf("parse error: %v", err)
			return output
		}

		// if判断 - 检查操作是否成功
		if !updateResp.Success {
			output.Status = "failed"
			output.InventoryUpdated = false
			output.Error = updateResp.Message
			return output
		}
	}

	output.InventoryUpdated = true  // 所有商品扣减成功
	return output
}

// executeSendNotificationActivityLegacy V1版发送通知活动实现（遗留代码）
// 功能：发布订单完成通知到PubSub消息队列
func executeSendNotificationActivityLegacy(ctx context.Context, output OrderWorkflowOutput, input OrderWorkflowInput) OrderWorkflowOutput {
	activityStart := time.Now()
	defer func() {
		success := output.Error == ""
		recordActivityExecution(ctx, "SendNotificationActivity", success)
		_ = time.Since(activityStart)
	}()

	// notificationMsg 通知消息映射表
	notificationMsg := map[string]interface{}{
		"order_id":        input.OrderID,           // 订单ID
		"user_id":         input.UserID,            // 用户ID
		"status":          output.Status,           // 订单状态
		"total_amount":    input.TotalAmount,       // 总金额
		"message":         fmt.Sprintf("Order %s has been processed successfully via Dapr Workflow Engine V1", input.OrderID),
		"timestamp":       time.Now().Format(time.RFC3339),  // 时间戳
		"workflow_engine": "dapr-workflow-engine-v1",        // 引擎版本
		"version":         "v1",                             // 版本号
	}

	msgBytes, _ := json.Marshal(notificationMsg)

	// daprClient.PublishEvent 发布事件到PubSub
	// 参数：上下文、PubSub组件名、主题名、消息数据
	err := daprClient.PublishEvent(ctx, "pubsub", "orders", msgBytes)
	if err != nil {
		output.NotificationSent = false  // 发布失败
	} else {
		output.NotificationSent = true   // 发布成功
	}

	return output
}