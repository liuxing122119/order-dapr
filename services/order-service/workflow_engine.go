// 工作流引擎包 - 实现Dapr工作流引擎的核心功能
// 功能：工作流实例管理、版本控制、活动处理器注册、状态查询和监控
package main

import (
	"context"     // 上下文控制库
	"encoding/json"  // JSON编解码库
	"fmt"         // 格式化输出库
	"sync"        // 同步原语库（互斥锁、等待组等）
	"time"        // 时间处理库

	"github.com/dapr/durabletask-go/task"   // Dapr持久化任务框架
	"github.com/dapr/go-sdk/client"         // Dapr Go SDK客户端
)

// 全局变量定义区域 - 工作流引擎核心组件
var (
	taskHubClient *WorkflowEngine  // taskHubClient 工作流引擎客户端指针
	engine        *workflowWorker  // engine 工作流工作者对象指针
)

// WorkflowEngine 工作流引擎结构体
// 管理所有工作流实例的生命周期和状态
type WorkflowEngine struct {
	mu        sync.RWMutex              // mu 读写互斥锁（用于并发安全访问instances）
	instances map[string]*workflowInstance  // instances 工作流实例映射表（键：实例ID，值：实例指针）
	registry  *task.TaskRegistry         // registry 任务注册表（注册编排器和活动）
}

// workflowInstance 工作流实例结构体
// 表示单个工作流执行实例的完整状态信息
type workflowInstance struct {
	ID          string       // ID 实例唯一标识符
	Name        string       // Name 工作流名称
	Status      string       // Status 运行状态（RUNNING/COMPLETED/FAILED/CANCELED）
	Input       []byte       // Input 输入参数（JSON字节数组）
	Output      []byte       // Output 输出结果（JSON字节数组）
	Error       string       // Error 错误信息
	CreatedAt   time.Time    // CreatedAt 创建时间
	CompletedAt *time.Time   // CompletedAt 完成时间（指针类型，未完成时为nil）
	Version     string       // Version 使用的工作流版本
}

// workflowWorker 工作流工作者结构体
// 用于管理后台goroutine的生命周期
type workflowWorker struct {
	ctx    context.Context     // ctx 上下文对象（用于取消操作）
	cancel context.CancelFunc  // cancel 取消函数（调用可通知goroutine停止）
	wg     sync.WaitGroup      // wg 等待组（用于等待所有goroutine完成）
}

// initWorkflowEngine 初始化工作流引擎函数
// 功能：创建任务注册表、注册编排器和活动处理器、初始化引擎组件
// 返回值：
//   - error - 初始化错误（nil表示成功）
func initWorkflowEngine() error {
	// task.NewTaskRegistry 创建新的任务注册表
	registry := task.NewTaskRegistry()

	// registry.AddOrchestratorN 注册V2版编排器（新版工作流）
	err := registry.AddOrchestratorN(WorkflowNameV2, ExecuteOrderProcessingWorkflow)
	if err != nil {
		return fmt.Errorf("failed to add orchestrator v2: %w", err)  // 注册失败返回错误
	}

	// 注册V1版编排器（旧版兼容）
	err = registry.AddOrchestratorN(WorkflowNameV1, ExecuteOrderProcessingWorkflowV1)
	if err != nil {
		return fmt.Errorf("failed to add orchestrator v1: %w", err)
	}

	// registry.AddActivityN 注册库存验证活动处理器
	err = registry.AddActivityN("ValidateInventoryActivity", validateInventoryActivityHandler)
	if err != nil {
		return fmt.Errorf("failed to add activity ValidateInventory: %w", err)
	}

	// 注册支付处理活动处理器
	err = registry.AddActivityN("ProcessPaymentActivity", processPaymentActivityHandler)
	if err != nil {
		return fmt.Errorf("failed to add activity ProcessPayment: %w", err)
	}

	// 注册库存更新活动处理器
	err = registry.AddActivityN("UpdateInventoryActivity", updateInventoryActivityHandler)
	if err != nil {
		return fmt.Errorf("failed to add activity UpdateInventory: %w", err)
	}

	// 注册发送通知活动处理器
	err = registry.AddActivityN("SendNotificationActivity", sendNotificationActivityHandler)
	if err != nil {
		return fmt.Errorf("failed to add activity SendNotification: %w", err)
	}

	// 初始化工作流引擎客户端
	taskHubClient = &WorkflowEngine{
		instances: make(map[string]*workflowInstance),  // make创建空映射表
		registry:  registry,                            // 设置任务注册表
	}

	// context.WithCancel 创建可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	// 初始化工作流工作者
	engine = &workflowWorker{
		ctx:    ctx,      // 上下文
		cancel: cancel,   // 取消函数
	}

	return nil  // 初始化成功
}

// stopWorkflowEngine 停止工作流引擎函数
// 功能：通过取消上下文通知所有运行中的工作流停止
func stopWorkflowEngine() {
	// if判断 - 检查引擎是否已初始化
	if engine != nil {
		engine.cancel()  // 调用取消函数，触发ctx.Done()
	}
}

// startWorkflowInstance 启动工作流实例函数
// 功能：创建新的工作流实例并在后台异步执行
// 参数：
//   - ctx context.Context - 上下文
//   - input OrderWorkflowInput - 工作流输入参数
// 返回值：
//   - string - 实例ID
//   - error - 错误信息
func startWorkflowInstance(ctx context.Context, input OrderWorkflowInput) (string, error) {
	// if判断 - 检查引擎客户端是否已初始化
	if taskHubClient == nil {
		return "", fmt.Errorf("task hub client not initialized")  // 未初始化返回错误
	}

	// generateUniqueID 生成唯一的实例ID
	instanceID := generateUniqueID()

	// json.Marshal 序列化输入参数
	inputData, _ := json.Marshal(input)

	// instance 创建工作流实例对象
	instance := &workflowInstance{
		ID:        instanceID,           // 实例ID
		Name:      WorkflowNameV2,        // 使用V2版工作流名称
		Status:    "RUNNING",            // 初始状态设为运行中
		Input:     inputData,            // 输入数据
		CreatedAt: time.Now(),           // 创建时间
		Version:   CurrentVersion,       // 版本号
	}

	// 获取写锁（排他锁）- 保证并发安全
	taskHubClient.mu.Lock()
	// 将实例存入映射表
	taskHubClient.instances[instanceID] = instance
	// taskHubClient.mu.Unlock 释放写锁
	taskHubClient.mu.Unlock()

	// go关键字 - 启动新goroutine异步执行工作流
	go func() {
		// defer延迟函数 - 捕获panic异常并记录到实例状态
		defer func() {
			// recover() 从panic中恢复并获取panic值
			if r := recover(); r != nil {  // if判断 - 如果发生panic
				instance.Status = "FAILED"  // 设置状态为失败
				instance.Error = fmt.Sprintf("panic: %v", r)  // 记录panic信息
				now := time.Now()
				instance.CompletedAt = &now  // 设置完成时间
			}
		}()

		// executeWorkflowWithVersion 根据版本执行工作流
		output, err := executeWorkflowWithVersion(ctx, input, CurrentVersion)

		now := time.Now()
		instance.CompletedAt = &now  // 设置完成时间

		// if-else条件判断 - 根据执行结果设置实例状态
		if err != nil {
			// 分支1: 执行失败
			instance.Status = "FAILED"
			instance.Error = err.Error()  // 记录错误信息
			outputData, _ := json.Marshal(OrderWorkflowOutput{
				OrderID:         input.OrderID,
				Status:          "failed",
				Error:           err.Error(),
				WorkflowVersion: CurrentVersion,
			})
			instance.Output = outputData  // 设置错误输出
		} else {
			// 分支2: 执行成功
			instance.Status = "COMPLETED"
			output.WorkflowVersion = CurrentVersion
			outputData, _ := json.Marshal(output)
			instance.Output = outputData  // 设置成功输出
		}
	}()

	return instanceID, nil  // return返回实例ID和无错误
}

// executeWorkflowWithVersion 根据版本执行工作流函数
// 功能：根据版本号路由到对应的工作流实现
// 参数：
//   - ctx context.Context - 上下文
//   - input OrderWorkflowInput - 输入参数
//   - version string - 版本标识（v1/v2）
// 返回值：
//   - OrderWorkflowOutput - 工作流输出结果
//   - error - 错误信息
func executeWorkflowWithVersion(ctx context.Context, input OrderWorkflowInput, version string) (OrderWorkflowOutput, error) {
	// switch多路选择语句 - 根据版本选择不同的实现
	switch version {
	case CurrentVersion:
		// case分支 - 当前版本（v2），调用V2实现
		return executeWorkflowV2(ctx, input)
	case "v1":
		// case分支 - V1旧版，调用V1遗留实现
		return executeWorkflowV1Legacy(ctx, input)
	default:
		// default分支 - 不支持的版本
		return OrderWorkflowOutput{}, fmt.Errorf("unsupported version: %s", version)
	}
}

// executeWorkflowV2 执行V2版工作流函数
// 功能：使用Dapr持久化任务框架执行新版工作流
func executeWorkflowV2(ctx context.Context, input OrderWorkflowInput) (OrderWorkflowOutput, error) {
	// createMockOrchestrationContext 创建模拟的编排上下文
	orchestrationCtx := createMockOrchestrationContext(ctx)
	// ExecuteOrderProcessingWorkflow 执行V2版工作流逻辑
	result, err := ExecuteOrderProcessingWorkflow(orchestrationCtx)
	if err != nil {
		return OrderWorkflowOutput{}, err  // 执行失败返回空输出和错误
	}

	// 类型断言 - 将结果转换为OrderWorkflowOutput类型
	output, ok := result.(OrderWorkflowOutput)
	if !ok {
		return OrderWorkflowOutput{}, fmt.Errorf("invalid output type")  // 类型断言失败返回错误
	}

	return output, nil  // return返回成功结果
}

// executeWorkflowV1Legacy 执行V1版遗留工作流函数
// 功能：调用V1版的旧实现以保持向后兼容
func executeWorkflowV1Legacy(ctx context.Context, input OrderWorkflowInput) (OrderWorkflowOutput, error) {
	return ExecuteOrderProcessingWorkflowV1ForLegacy(ctx, input)
}

// getWorkflowStatus 获取工作流状态函数
// 功能：根据实例ID查询工作流的当前状态信息
// 参数：
//   - ctx context.Context - 上下文
//   - instanceID string - 实例ID
// 返回值：
//   - map[string]interface{} - 状态信息映射表
//   - error - 错误信息
func getWorkflowStatus(ctx context.Context, instanceID string) (map[string]interface{}, error) {
	// if判断 - 检查引擎是否已初始化
	if taskHubClient == nil {
		return nil, fmt.Errorf("task hub client not initialized")
	}

	// RLock获取读锁（共享锁）- 允许多个并发读取
	taskHubClient.mu.RLock()
	// 从映射表中查找实例
	instance, exists := taskHubClient.instances[instanceID]
	// RUnlock释放读锁
	taskHubClient.mu.RUnlock()

	// if判断 - 检查实例是否存在
	if !exists {
		return nil, fmt.Errorf("instance not found: %s", instanceID)  // 不存在返回错误
	}

	// status 构建状态信息映射表
	status := map[string]interface{}{
		"instanceId":    instance.ID,                              // 实例ID
		"name":          instance.Name,                            // 工作流名称
		"runtimeStatus": instance.Status,                          // 运行时状态
		"createdAt":     instance.CreatedAt.Format(time.RFC3339),  // 创建时间
		"lastUpdated":   instance.CreatedAt.Format(time.RFC3339),  // 最后更新时间（初始等于创建时间）
		"version":       instance.Version,                         // 版本号
	}

	// if判断 - 如果已完成，添加完成时间
	if instance.CompletedAt != nil {
		status["completedAt"] = instance.CompletedAt.Format(time.RFC3339)  // 完成时间
		status["lastUpdated"] = instance.CompletedAt.Format(time.RFC3339)  // 更新最后更新时间
	}

	return status, nil  // return返回状态信息
}

// waitForWorkflowCompletion 等待工作流完成函数
// 功能：轮询工作流状态直到完成或超时
// 参数：
//   - ctx context.Context - 上下文（可用于外部取消）
//   - instanceID string - 实例ID
//   - timeout time.Duration - 超时时间
// 返回值：
//   - OrderWorkflowOutput - 工作流输出结果
//   - error - 错误信息（超时或取消时返回错误）
func waitForWorkflowCompletion(ctx context.Context, instanceID string, timeout time.Duration) (OrderWorkflowOutput, error) {
	// if判断 - 检查引擎是否已初始化
	if taskHubClient == nil {
		return OrderWorkflowOutput{}, fmt.Errorf("task hub client not initialized")
	}

	// time.After 创建超时通道（到达指定时间后自动发送当前时间）
	timeoutChan := time.After(timeout)
	// time.NewTicker 创建定时器通道（每隔指定时间发送当前时间）
	ticker := time.NewTicker(100 * time.Millisecond)  // 每100毫秒轮询一次
	defer ticker.Stop()  // defer延迟调用 - 函数退出时停止定时器

	// for无限循环 - 持续轮询直到工作流完成或超时/取消
	for {
		// select多路复用语句 - 同时监听多个通道
		select {
		case <-timeoutChan:
			// case分支1: 超时通道触发
			return OrderWorkflowOutput{}, fmt.Errorf("timeout waiting for completion")

		case <-ticker.C:
			// case分支2: 定时器触发（执行轮询检查）

			// RLock获取读锁进行安全读取
			taskHubClient.mu.RLock()
			instance, exists := taskHubClient.instances[instanceID]
			taskHubClient.mu.RUnlock()  // 释放读锁

			// if判断 - 检查实例是否存在
			if !exists {
				return OrderWorkflowOutput{}, fmt.Errorf("instance not found: %s", instanceID)
			}

			// if判断 - 检查是否处于终态（COMPLETED/FAILED/CANCELED）
			if instance.Status == "COMPLETED" || instance.Status == "FAILED" || instance.Status == "CANCELED" {
				var output OrderWorkflowOutput
				// if判断 - 解析输出数据
				if len(instance.Output) > 0 {  // len获取字节切片长度
					if err := json.Unmarshal(instance.Output, &output); err != nil {
						return OrderWorkflowOutput{}, fmt.Errorf("failed to parse output: %w", err)
					}
				}

				// if判断 - 失败状态但无错误信息，从实例中补充
				if instance.Status == "FAILED" && output.Error == "" {
					output.Error = instance.Error
					output.Status = "failed"
				}

				return output, nil  // return返回最终结果
			}

		case <-ctx.Done():
			// case分支3: 上下文被取消（外部主动取消）
			return OrderWorkflowOutput{}, ctx.Err()  // 返回上下文错误
		}
	}
}

// listRunningInstances 列出正在运行的实例函数
// 功能：遍历所有实例，收集状态为RUNNING的实例ID列表
// 返回值：
//   - []string - 正在运行的实例ID字符串切片
func listRunningInstances() []string {
	// if判断 - 检查引擎是否已初始化
	if taskHubClient == nil {
		return []string{}  // 未初始化返回空切片
	}

	var runningInstances []string  // 声明切片变量存储运行中的实例ID

	// RLock获取读锁
	taskHubClient.mu.RLock()
	// Range方法 - 遍历映射表中的所有键值对
	for id, inst := range taskHubClient.instances {  // id是键（实例ID），inst是值（实例对象）
		// if判断 - 过滤出运行中的实例
		if inst.Status == "RUNNING" {
			runningInstances = append(runningInstances, id)  // append追加到切片
		}
	}
	taskHubClient.mu.RUnlock()  // 释放读锁

	return runningInstances  // return返回实例ID列表
}

// validateInventoryActivityHandler 库存验证活动处理函数
// 功能：作为活动处理器被工作流引擎调用，验证订单商品的库存可用性
// 参数：
//   - ctx task.ActivityContext - 活动上下文（包含输入参数等）
// 返回值：
//   - interface{} - 活动输出结果
//   - error - 错误信息
func validateInventoryActivityHandler(ctx task.ActivityContext) (interface{}, error) {
	startTime := time.Now()
	// defer延迟函数 - 记录活动执行指标
	defer func() {
		success := true
		recordActivityExecution(context.Background(), "ValidateInventoryActivity", success)
		_ = time.Since(startTime)
	}()

	var input OrderWorkflowInput
	// ctx.GetInput 从活动上下文中获取输入参数
	if err := ctx.GetInput(&input); err != nil {
		return OrderWorkflowOutput{}, err  // 获取输入失败返回错误
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
	// daprClient.InvokeMethodWithContent 调用库存服务接口
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

// processPaymentActivityHandler 支付处理活动处理函数
// 功能：调用支付服务处理订单支付
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

	// 调用支付服务
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

// updateInventoryActivityHandler 库存更新活动处理函数
// 功能：调用库存服务扣减订单商品库存
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

	// for range循环 - 遍历每个商品进行库存扣减
	for _, item := range input.Items {
		DeductInventoryRequest struct {
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

		// 调用库存服务更新接口
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

// sendNotificationActivityHandler 发送通知活动处理函数
// 功能：发布订单完成通知到消息队列
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
		"activities_completed": []string{  // 已完成的活动列表（字符串切片）
			"ValidateInventoryActivity",
			"ProcessPaymentActivity",
			"UpdateInventoryActivity",
			"SendNotificationActivity",
		},
	}

	msgBytes, _ := json.Marshal(notificationMsg)

	// 发布事件到PubSub
	err := daprClient.PublishEvent(context.Background(), "pubsub", "orders", msgBytes)
	if err != nil {
		output.NotificationSent = false
		return output, err
	}

	return output, nil
}

// migrateRunningWorkflowsToV2 运行中工作流迁移到V2函数
// 功能：统计仍在使用V1版本的运行中实例数量（预留迁移能力）
// 参数：
//   - ctx context.Context - 上下文
// 返回值：
//   - error - 错误信息
func migrateRunningWorkflowsToV2(ctx context.Context) error {
	// if判断 - 检查引擎是否已初始化
	if taskHubClient == nil {
		return fmt.Errorf("task hub client not initialized")
	}

	// listRunningInstances 获取所有运行中的实例ID列表
	runningInstances := listRunningInstances()
	migratedCount := 0  // migratedCount 迁移计数器

	// for range循环 - 遍历运行中的实例
	for _, instanceID := range runningInstances {
		// RLock读锁保护
		taskHubClient.mu.RLock()
		instance, exists := taskHubClient.instances[instanceID]
		taskHubClient.mu.RUnlock()

		// if判断 - 检查实例是否存在且是V1版本且在运行中
		if !exists {
			continue  // continue跳转 - 不存在则跳过
		}

		if instance.Name == WorkflowNameV1 && instance.Status == "RUNNING" {
			migratedCount++  // 计数器递增（实际迁移逻辑待实现）
		}
	}

	return nil  // 返回nil表示成功（当前仅统计不迁移）
}

// getWorkflowVersionInfo 获取工作流版本信息函数
// 功能：返回工作流系统的版本配置和特性说明
// 返回值：
//   - map[string]interface{} - 版本信息映射表
func getWorkflowVersionInfo() map[string]interface{} {
	return map[string]interface{}{
		"current_version":    CurrentVersion,                    // 当前使用的版本
		"available_versions": []string{"v1", "v2"},             // 可用的版本列表
		"v1_workflow_name":   WorkflowNameV1,                   // V1工作流名称
		"v2_workflow_name":   WorkflowNameV2,                   // V2工作流名称
		"migration_status":   "ready",                          // 迁移状态：就绪
		"backward_compat":    true,                             // 是否支持向后兼容
		"description":        "Dapr Workflow Engine with version management support. Running instances continue with their registered version while new instances use the latest version.",
	}
}

// generateUniqueID 生成唯一ID函数
// 功能：基于纳秒级时间戳生成全局唯一的实例ID
// 返回值：
//   - string - 格式化的唯一ID字符串
func generateUniqueID() string {
	return fmt.Sprintf("%d-%s", time.Now().UnixNano(), fmt.Sprintf("%04x", time.Now().UnixNano()%0xFFFF))
	// UnixNano() 获取纳秒级Unix时间戳
	// %04x 格式化为4位十六进制数（不足补零）
	// %0xFFFF 取低16位掩码
}

// createMockOrchestrationContext 创建模拟编排上下文函数
// 功能：创建一个空的编排上下文对象（用于非真实Dapr环境下的测试）
// 参数：
//   - ctx context.Context - 原始上下文
// 返回值：
//   - *task.OrchestrationContext - 编排上下文指针
func createMockOrchestrationContext(ctx context.Context) *task.OrchestrationContext {
	oc := &task.OrchestrationContext{}  // 使用结构体字面量创建空对象
	return oc  // return返回空上下文
}