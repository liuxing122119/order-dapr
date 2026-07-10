// 工作流启动器包 - 负责工作流系统的初始化、启动和生命周期管理
// 功能：创建订单时触发工作流执行、监控工作流状态、更新订单状态
package main

import (
	"context"       // 上下文控制库
	"encoding/json" // JSON编解码库
	"fmt"           // 格式化输出库
	"sync"          // 同步原语库
	"time"          // 时间处理库

	"github.com/dapr/go-sdk/service/common" // Dapr通用服务接口
	"github.com/google/uuid"                // UUID生成库（用于生成唯一ID）
)

// WorkflowInstance 工作流实例结构体（对外暴露的API模型）
// 用于向客户端返回工作流实例的详细信息
type WorkflowInstance struct {
	InstanceID   string              `json:"instanceId"`            // 实例唯一标识符
	OrderID      string              `json:"orderId"`               // 关联的订单ID
	Status       string              `json:"status"`                // 当前运行状态
	CustomStatus string              `json:"customStatus"`          // 自定义状态描述（如：validating_inventory）
	CreatedAt    time.Time           `json:"createdAt"`             // 创建时间
	CompletedAt  *time.Time          `json:"completedAt,omitempty"` // 完成时间（可选，未完成时为null）
	Output       OrderWorkflowOutput `json:"output,omitempty"`      // 输出结果（可选）
	Error        string              `json:"error,omitempty"`       // 错误信息（可选）
}

// 全局变量定义区域 - 工作流实例存储
var (
	workflowInstances sync.Map // workflowInstances 并发安全的映射表（存储所有工作流实例）
	instanceCounter   int64    // instanceCounter 实例计数器（用于生成递增ID）
)

// initializeWorkflowSystem 初始化工作流系统函数
// 功能：初始化遥测系统和工作流引擎
// 返回值：
//   - error - 初始化错误
func initializeWorkflowSystem() error {
	initTelemetry()                 // initTelemetry 初始化OpenTelemetry遥测监控
	return initDaprWorkflowEngine() // initDaprWorkflowEngine 初始化Dapr工作流引擎
}

// startWorkflowEngine 启动工作流引擎函数
// 功能：等待引擎完全就绪后返回
// 返回值：
//   - error - 启动错误
func startWorkflowEngine() error {
	time.Sleep(500 * time.Millisecond) // sleep等待500毫秒让引擎完全启动
	return nil                         // 返回nil表示成功
}

// initDaprWorkflowEngine 初始化Dapr工作流引擎函数
// 功能：调用引擎初始化并启动后台迁移任务
// 返回值：
//   - error - 错误信息
func initDaprWorkflowEngine() error {
	// if判断 - 调用initWorkflowEngine进行底层初始化
	if err := initWorkflowEngine(); err != nil {
		return nil // 即使初始化失败也返回nil（容错处理）
	}

	// go关键字 - 启动goroutine异步执行延迟迁移任务
	go func() {
		time.Sleep(10 * time.Second) // 等待10秒让系统稳定
		ctx := context.Background()
		migrateRunningWorkflowsToV2(ctx) // migrateRunningWorkflowsToV2 尝试将V1工作流迁移到V2
	}()

	return nil // 返回成功
}

// handleCreateOrderWithWorkflow 创建订单并启动工作流处理函数
// 功能：接收创建订单请求、保存订单数据、启动工作流编排、返回处理结果
// 参数：
//   - ctx context.Context - 请求上下文
//   - in *common.InvocationEvent - Dapr调用事件对象
//
// 返回值：
//   - *common.Content - 响应内容（包含订单和工作流信息）
//   - error - 错误信息
func handleCreateOrderWithWorkflow(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	var req CreateOrderRequest // req 创建订单请求对象
	if err := json.Unmarshal(in.Data, &req); err != nil {
		return nil, fmt.Errorf("invalid order request: %w", err) // 反序列化失败返回错误
	}

	// if判断 - 验证必填字段
	if req.UserID == "" || len(req.Items) == 0 { // len获取切片长度
		return nil, fmt.Errorf("userId and items are required") // 缺少必填字段返回错误
	}

	orderID := uuid.New().String()     // uuid.New生成UUID作为订单ID
	instanceID := generateInstanceID() // generateInstanceID 生成工作流实例ID

	order := Order{ // order 创建订单对象
		OrderID:          orderID,
		UserID:           req.UserID,
		Items:            req.Items,
		Status:           "pending", // 初始状态设为待处理
		CreatedAt:        time.Now().Format(time.RFC3339),
		Version:          1,     // 版本号从1开始
		UserValidated:    false, // 用户验证状态初始为false
		InventoryChecked: false, // 库存检查状态初始为false
		PaymentProcessed: false, // 支付处理状态初始为false
	}

	totalAmount := calculateTotalAmount(req.Items) // calculateTotalAmount 计算订单总金额
	order.TotalAmount = totalAmount

	saveOrderToState(ctx, order) // saveOrderToState 保存订单到状态存储

	recordOrderAmount(ctx, orderID, totalAmount) // recordOrderAmount 记录订单金额到遥测指标

	workflowInput := OrderWorkflowInput{ // workflowInput 构建工作流输入参数
		OrderID:     orderID,
		UserID:      req.UserID,
		Items:       req.Items,
		TotalAmount: totalAmount,
		Version:     CurrentVersion, // 使用当前版本
	}

	var actualInstanceID string // actualInstanceID 实际使用的实例ID
	var startErr error          // startErr 启动错误变量

	// if-else条件判断 - 根据引擎是否可用选择不同的启动方式
	if taskHubClient != nil {
		actualInstanceID, startErr = startWorkflowInstance(ctx, workflowInput)
		if startErr != nil {
			actualInstanceID = instanceID // 引擎启动失败则使用预生成的ID
		}
	} else {
		actualInstanceID = instanceID // 引擎未初始化直接使用预生成的ID
	}

	instance := WorkflowInstance{ // instance 创建对外暴露的工作流实例对象
		InstanceID:   actualInstanceID,
		OrderID:      orderID,
		Status:       "running",              // 状态设为运行中
		CustomStatus: "validating_inventory", // 自定义状态：正在验证库存
		CreatedAt:    time.Now(),
	}

	workflowInstances.Store(actualInstanceID, instance) // Store将实例存入并发映射表

	// if-else判断 - 选择工作流执行模式
	if taskHubClient == nil || startErr != nil {
		// 分支1: 使用遗留V1实现（引擎不可用时）
		go func() {
			backgroundCtx := context.Background()
			executeAndMonitorWorkflow(backgroundCtx, actualInstanceID, workflowInput)
		}()
	} else {
		// 分支2: 使用Dapr工作流引擎（正常情况）
		go func() {
			backgroundCtx := context.Background()
			monitorDaprWorkflow(backgroundCtx, actualInstanceID, workflowInput)
		}()
	}

	versionInfo := getWorkflowVersionInfo() // getWorkflowVersionInfo 获取版本信息

	response := map[string]interface{}{ // response 构建响应数据
		"orderId":    orderID,
		"status":     "processing",
		"instanceId": actualInstanceID,
		"message":    "Order created and Dapr Workflow Engine started with version management",
		"engineInfo": map[string]string{ // engineInfo 引擎详细信息子映射
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
		"workflowActivities": []map[string]string{ // workflowActivities 工作流活动步骤列表
			{"step": "1", "name": "ValidateInventoryActivity", "description": "验证库存活动：调用库存服务确认所需库存充足"},
			{"step": "2", "name": "ProcessPaymentActivity", "description": "处理支付活动：调用支付服务扣款"},
			{"step": "3", "name": "UpdateInventoryActivity", "description": "更新库存活动：调用库存服务扣减库存"},
			{"step": "4", "name": "SendNotificationActivity", "description": "发送通知活动：发送订单状态通知"},
		},
		"endpoints": map[string]string{ // endpoints 相关API端点
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

// executeAndMonitorWorkflow 执行并监控遗留工作流函数
// 功能：使用V1遗留实现执行工作流并在完成后更新状态
// 参数：
//   - ctx context.Context - 上下文
//   - instanceID string - 实例ID
//   - input OrderWorkflowInput - 工作流输入
func executeAndMonitorWorkflow(ctx context.Context, instanceID string, input OrderWorkflowInput) {
	updateInstanceStatus(instanceID, "running", "validating_inventory") // 更新实例状态

	output, err := ExecuteOrderProcessingWorkflowV1ForLegacy(ctx, input) // 执行V1遗留工作流

	now := time.Now()

	if err != nil {
		updateInstanceCompleted(instanceID, "failed", output, err.Error(), now) // 失败更新
	} else {
		updateInstanceCompleted(instanceID, "completed", output, "", now) // 成功更新
	}

	updateOrderFromWorkflowOutput(ctx, input.OrderID, output) // updateOrderFromWorkflowOutput 将结果回写到订单
}

// monitorDaprWorkflow 监控Dapr工作流函数
// 功能：轮询等待Dapr工作流引擎完成工作流并更新状态
func monitorDaprWorkflow(ctx context.Context, instanceID string, input OrderWorkflowInput) {
	updateInstanceStatus(instanceID, "running", "dapr_workflow_engine_processing")

	timeout := 5 * time.Minute                                         // timeout 设置超时时间为5分钟
	output, err := waitForWorkflowCompletion(ctx, instanceID, timeout) // waitForWorkflowCompletion 等待工作流完成

	now := time.Now()

	if err != nil {
		updateInstanceCompleted(instanceID, "failed", OrderWorkflowOutput{
			OrderID:         input.OrderID,
			Status:          "failed",
			Error:           err.Error(),
			WorkflowVersion: CurrentVersion,
		}, err.Error(), now) // 失败更新
	} else {
		updateInstanceCompleted(instanceID, "completed", output, "", now) // 成功更新
	}

	updateOrderFromWorkflowOutput(ctx, input.OrderID, output) // 回写订单状态
}

// ExecuteOrderProcessingWorkflowV1ForLegacy V1版工作流执行入口（供外部调用）
// 功能：设置版本标识并调用底层V1实现
func ExecuteOrderProcessingWorkflowV1ForLegacy(ctx context.Context, input OrderWorkflowInput) (OrderWorkflowOutput, error) {
	legacyInput := input
	legacyInput.Version = "v1" // 设置版本为v1

	output, err := executeLegacyWorkflowV1(ctx, legacyInput) // executeLegacyWorkflowV1 执行实际逻辑
	return output, err
}

// executeLegacyWorkflowV1 执行V1遗留工作流核心逻辑
// 功能：按顺序执行各活动步骤并记录遥测数据
func executeLegacyWorkflowV1(ctx context.Context, input OrderWorkflowInput) (OrderWorkflowOutput, error) {
	startTime := time.Now()
	wfCtx := recordWorkflowStart(ctx, WorkflowNameV1) // recordWorkflowStart 记录工作流开始
	defer func() {
		recordWorkflowComplete(wfCtx, WorkflowNameV1, "completed", time.Since(startTime)) // defer记录完成
	}()

	output := OrderWorkflowOutput{
		OrderID:         input.OrderID,
		WorkflowVersion: "v1",
	}

	// 按顺序调用各活动的遗留实现
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

// updateOrderFromWorkflowOutput 从工作流输出更新订单状态函数
// 功能：查询订单并将工作流执行结果回写到订单状态
// 参数：
//   - ctx context.Context - 上下文
//   - orderID string - 订单ID
//   - output OrderWorkflowOutput - 工作流输出
func updateOrderFromWorkflowOutput(ctx context.Context, orderID string, output OrderWorkflowOutput) {
	item, err := daprClient.GetState(ctx, stateStoreName, "order-"+orderID, nil)
	if err != nil {
		return // 获取失败直接返回
	}

	if item == nil || len(item.Value) == 0 {
		return // 订单不存在直接返回
	}

	var order Order
	json.Unmarshal(item.Value, &order) // 反序列化订单数据

	order.Status = output.Status                      // 更新订单状态
	order.UpdatedAt = time.Now().Format(time.RFC3339) // 更新时间戳
	order.Version++                                   // 版本号自增
	order.InventoryChecked = output.InventoryValid    // 更新库存检查结果
	order.PaymentProcessed = output.PaymentSuccess    // 更新支付处理结果

	saveOrderToState(ctx, order) // 保存更新后的订单
}

// generateInstanceID 生成实例ID函数
// 功能：基于计数器和UUID生成格式化的实例ID
// 返回值：
//   - string - 格式："wf-{计数器}-{8位UUID}"
func generateInstanceID() string {
	instanceCounter++ // 计数器递增
	return fmt.Sprintf("wf-%d-%s", instanceCounter, uuid.New().String()[:8])
	// uuid.New().String()[:8] 取UUID前8位字符
}

// updateInstanceStatus 更新实例运行状态函数
// 功能：修改并发映射表中指定实例的状态和自定义状态
// 参数：
//   - instanceID string - 实例ID
//   - status string - 运行状态
//   - customStatus string - 自定义状态描述
func updateInstanceStatus(instanceID string, status, customStatus string) {
	// Load从并发映射表中加载实例
	if obj, ok := workflowInstances.Load(instanceID); ok {
		instance := obj.(WorkflowInstance)            // 类型断言为WorkflowInstance
		instance.Status = status                      // 更新状态
		instance.CustomStatus = customStatus          // 更新自定义状态
		workflowInstances.Store(instanceID, instance) // Store写回更新的实例
	}
}

// updateInstanceCompleted 更新实例完成状态函数
// 功能：在工作流完成或失败时设置最终状态、输出和错误信息
// 参数：
//   - instanceID string - 实例ID
//   - status string - 最终状态
//   - output OrderWorkflowOutput - 工作流输出
//   - errMsg string - 错误消息
//   - completedAt time.Time - 完成时间
func updateInstanceCompleted(instanceID string, status string, output OrderWorkflowOutput, errMsg string, completedAt time.Time) {
	if obj, ok := workflowInstances.Load(instanceID); ok {
		instance := obj.(WorkflowInstance)
		instance.Status = status                      // 设置最终状态
		instance.CustomStatus = ""                    // 清空自定义状态
		instance.CompletedAt = &completedAt           // 设置完成时间指针
		instance.Output = output                      // 设置输出结果
		instance.Error = errMsg                       // 设置错误信息
		workflowInstances.Store(instanceID, instance) // 写回映射表
	}
}

// stopWorkflowEngineSystem 停止工作流引擎系统函数
// 功能：优雅关闭前等待运行中的工作流完成
// 返回值：
//   - error - 错误信息
func stopWorkflowEngineSystem() error {
	runningCount := 0 // runningCount 运行中的实例计数器

	// Range遍历并发映射表统计运行中实例数量
	workflowInstances.Range(func(key, value interface{}) bool {
		instance := value.(WorkflowInstance) // 类型断言
		if instance.Status == "running" {
			runningCount++ // 计数器递增
		}
		return true // return true继续遍历
	})

	// if判断 - 如果有运行中的实例，等待一段时间让其完成
	if runningCount > 0 {
		time.Sleep(5 * time.Second) // 等待5秒
	}

	stopDaprWorkflowEngine() // stopDaprWorkflowEngine 停止底层引擎
	return nil               // 返回成功
}

// stopDaprWorkflowEngine 停止Dapr工作流引擎函数
// 功能：通过取消函数停止引擎的所有goroutine
func stopDaprWorkflowEngine() {
	if engine != nil {
		engine.cancel() // 调用取消函数停止引擎
	}
}
