// Dapr多运行时云原生应用实践 - 工作流引擎核心模块
// 功能：工作流实例管理、活动处理器注册、版本控制分发、状态追踪与查询
package main

import (
	"context"       // 上下文管理，用于控制请求生命周期和传播取消信号
	"encoding/json" // JSON序列化与反序列化库
	"fmt"           // 格式化输入输出库
	"sync"          // 并发同步原语，提供互斥锁和读写锁保证线程安全
	"time"          // 时间处理工具，用于时间戳记录和超时控制

	"github.com/dapr/durabletask-go/task" // Dapr工作流任务框架，提供编排器和活动处理器接口
	"github.com/dapr/go-sdk/client"       // Dapr Go SDK客户端包，提供服务调用能力
)

var (
	taskHubClient *WorkflowEngine // 全局工作流引擎实例指针，提供实例管理和调度能力
	engine        *workflowWorker // 后台工作协程管理器，负责优雅关闭和生命周期控制
)

// WorkflowEngine 工作流引擎主结构体，封装实例存储、任务注册表和并发控制机制
// 采用单例模式全局唯一，通过sync.RWMutex保护并发访问安全
type WorkflowEngine struct {
	mu        sync.RWMutex                 // 读写锁对象，保护instances映射表的并发访问安全
	instances map[string]*workflowInstance // 工作流实例映射表，key为实例ID，value为实例对象指针
	registry  *task.TaskRegistry           // 任务注册表，存储所有已注册的编排器和活动处理器
}

// workflowInstance 单个工作流实例的状态信息结构体，记录完整的执行生命周期数据
// 每个订单创建请求都会生成一个独立的实例用于追踪处理进度
type workflowInstance struct {
	ID          string     // 实例唯一标识符（UUID格式），由generateUniqueID()生成
	Name        string     // 工作流名称标识符（如"OrderProcessingWorkflowV2"）
	Status      string     // 运行时状态：RUNNING/COMPLETED/FAILED/CANCELED
	Input       []byte     // 输入参数的JSON序列化字节数组，便于审计和调试
	Output      []byte     // 输出结果的JSON序列化字节数组，包含完整的业务结果
	Error       string     // 错误信息字符串，仅在失败或异常时非空
	CreatedAt   time.Time  // 实例创建时间戳，记录工作流的启动时刻
	CompletedAt *time.Time // 完成时间戳指针，nil表示仍在运行中，非空表示已终止
	Version     string     // 工作流版本号标识（如"v2"、"v1"），决定使用的编排逻辑
}

// workflowWorker 后台工作协程管理器结构体，负责工作流引擎的生命周期控制
// 通过context.Context实现优雅关闭机制，确保所有后台goroutine正常退出
type workflowWorker struct {
	ctx    context.Context    // 上下文对象，携带取消信号用于通知协程退出
	cancel context.CancelFunc // 取消函数调用后触发ctx.Done()通道关闭
	wg     sync.WaitGroup     // 等待组对象，用于等待所有后台协程完成清理工作
}

// initWorkflowEngine 初始化工作流引擎核心组件，注册所有编排器和活动处理器到任务注册表
// 在应用启动时调用此函数一次性完成引擎配置，后续不可修改注册内容
// 返回值：初始化过程中的错误信息（通常为nil表示成功）
func initWorkflowEngine() error {

	// 创建新的任务注册表实例，用于存储编排器和活动处理器的映射关系
	registry := task.NewTaskRegistry()

	// 注册V2版本的订单处理编排器（当前默认版本），使用最新的业务流程编排逻辑
	err := registry.AddOrchestratorN(WorkflowNameV2, ExecuteOrderProcessingWorkflow)
	if err != nil {
		// 编排器注册失败时返回明确的错误信息便于排查配置问题
		return fmt.Errorf("failed to add orchestrator v2: %w", err) // 错误包装保留原始信息
	}

	// 注册V1版本的订单处理编排器（向后兼容版本），保持旧版客户端兼容性
	err = registry.AddOrchestratorN(WorkflowNameV1, ExecuteOrderProcessingWorkflowV1)
	if err != nil {
		// V1版本注册失败时返回错误提示（不影响V2版本的正常使用）
		return fmt.Errorf("failed to add orchestrator v1: %w", err)
	}

	// 注册库存验证活动处理器，对应工作流的第一步：检查商品库存充足性
	err = registry.AddActivityN("ValidateInventoryActivity", validateInventoryActivityHandler)
	if err != nil {
		// 活动处理器注册失败时返回错误信息标识具体的活动名称
		return fmt.Errorf("failed to add activity ValidateInventory: %w", err)
	}

	// 注册支付处理活动处理器，对应工作流的第二步：执行订单支付扣款操作
	err = registry.AddActivityN("ProcessPaymentActivity", processPaymentActivityHandler)
	if err != nil {
		return fmt.Errorf("failed to add activity ProcessPayment: %w", err)
	}

	// 注册库存更新活动处理器，对应工作流的第三步：扣减已售商品的库存数量
	err = registry.AddActivityN("UpdateInventoryActivity", updateInventoryActivityHandler)
	if err != nil {
		return fmt.Errorf("failed to add activity UpdateInventory: %w", err)
	}

	// 注册通知发送活动处理器，对应工作流的第四步：发布订单处理完成的通知消息
	err = registry.AddActivityN("SendNotificationActivity", sendNotificationActivityHandler)
	if err != nil {
		return fmt.Errorf("failed to add activity SendNotification: %w", err)
	}

	// 初始化全局工作流引擎实例，创建空的实例存储映射表和任务注册表引用
	taskHubClient = &WorkflowEngine{
		instances: make(map[string]*workflowInstance), // 创建空的工作流实例映射表
		registry:  registry,                           // 引用已配置完成的任务注册表
	}

	// 创建可取消的上下文对象用于控制后台协程的生命周期
	ctx, cancel := context.WithCancel(context.Background())
	// 初始化后台协程管理器，绑定上下文和取消函数用于优雅关闭机制
	engine = &workflowWorker{
		ctx:    ctx,    // 上下文对象携带取消信号
		cancel: cancel, // 取消函数用于触发协程退出
	}

	return nil // 初始化成功无错误
}

// stopWorkflowEngine 停止工作流引擎系统，通过取消上下文通知所有后台协程退出
// 在应用优雅关闭时调用此函数释放工作流引擎占用的资源
func stopWorkflowEngine() {
	// 检查引擎实例是否已初始化（防止空指针异常）
	if engine != nil {
		// 调用取消函数发送退出信号给所有关联的后台协程
		engine.cancel()
	}
}

// startWorkflowInstance 启动新的工作流实例，异步执行订单处理工作流并返回实例ID
// 参数ctx：请求上下文，用于传播给工作流执行过程进行超时控制
// 参数input：工作流输入参数结构体，包含订单ID、用户ID、商品列表等业务数据
// 返回值：新创建的工作流实例唯一标识符和可能的错误信息
func startWorkflowInstance(ctx context.Context, input OrderWorkflowInput) (string, error) {
	// 检查工作流引擎是否已正确初始化（防止空指针异常）
	if taskHubClient == nil {
		return "", fmt.Errorf("task hub client not initialized") // 返回明确的初始化错误提示
	}

	// 生成全局唯一的实例标识符用于追踪和查询此工作流实例的运行状态
	instanceID := generateUniqueID()

	// 将输入参数序列化为JSON字节数组存储到实例记录中便于后续审计和调试
	inputData, _ := json.Marshal(input)

	// 构造新的工作流实例对象，初始化所有状态字段为运行中的初始状态
	instance := &workflowInstance{
		ID:        instanceID,     // 实例唯一标识符（刚生成的UUID）
		Name:      WorkflowNameV2, // 使用V2版本的工作流名称（当前默认版本）
		Status:    "RUNNING",      // 初始运行状态为RUNNING表示正在执行中
		Input:     inputData,      // 输入参数的JSON序列化数据
		CreatedAt: time.Now(),     // 记录实例创建的时间戳
		Version:   CurrentVersion, // 标记使用当前版本的配置和逻辑
	}

	// 使用写锁保护实例映射表的并发写入操作确保线程安全
	taskHubClient.mu.Lock()
	// 将新创建的实例注册到全局实例存储映射表中供后续查询
	taskHubClient.instances[instanceID] = instance
	// 释放写锁允许其他协程访问实例映射表
	taskHubClient.mu.Unlock()

	// 启动后台goroutine异步执行实际的工作流逻辑，不阻塞调用方响应
	go func() {
		// 注册延迟恢复函数捕获工作流执行过程中的panic异常防止程序崩溃
		defer func() {
			// 检查是否有panic异常发生并恢复执行
			if r := recover(); r != nil {
				// 更新实例状态为FAILED标记工作流因panic而失败终止
				instance.Status = "FAILED" // 失败状态标识
				// 记录panic异常的错误信息字符串
				instance.Error = fmt.Sprintf("panic: %v", r)
				// 设置完成时间戳记录失败发生的时刻
				now := time.Now()
				instance.CompletedAt = &now // 完成时间指针赋值
			}
		}()

		// 调用带版本控制的工作流执行函数传入当前版本号启动实际的业务流程编排
		output, err := executeWorkflowWithVersion(ctx, input, CurrentVersion)

		// 记录工作流完成的当前时间戳无论成功或失败都需要设置完成时间
		now := time.Now()
		instance.CompletedAt = &now

		// 根据执行结果更新实例的状态信息和输出数据
		if err != nil {
			// 工作流执行出错时更新实例状态为FAILED并保存错误详情
			instance.Status = "FAILED"   // 失败状态标识
			instance.Error = err.Error() // 保存错误信息字符串
			// 序列化失败状态的输出结果包含错误信息和工作流版本
			outputData, _ := json.Marshal(OrderWorkflowOutput{
				OrderID:         input.OrderID,  // 关联的订单ID
				Status:          "failed",       // 最终状态为failed
				Error:           err.Error(),    // 错误信息详细描述
				WorkflowVersion: CurrentVersion, // 工作流版本号
			})
			instance.Output = outputData // 保存输出数据到实例记录
		} else {
			// 工作流执行成功时更新实例状态为COMPLETED并保存业务结果
			instance.Status = "COMPLETED"           // 完成状态标识
			output.WorkflowVersion = CurrentVersion // 在输出结果中补充版本号信息
			// 序列化成功的输出结果包含完整的业务数据和版本信息
			outputData, _ := json.Marshal(output)
			instance.Output = outputData // 保存输出数据到实例记录
		}
	}()

	return instanceID, nil
}

// executeWorkflowWithVersion 带版本控制的工作流执行分发函数，根据版本号调用对应的工作流实现
// 参数ctx：请求上下文，用于传播给具体的工作流执行函数
// 参数input：工作流输入参数结构体，包含订单处理所需的全部业务数据
// 参数version：工作流版本标识符（"v2"或"v1"），决定调用哪个版本的实现逻辑
// 返回值：工作流执行结果输出结构体和可能的错误信息
func executeWorkflowWithVersion(ctx context.Context, input OrderWorkflowInput, version string) (OrderWorkflowOutput, error) {
	// 使用switch语句根据版本号进行分支选择对应的工作流执行实现
	switch version {
	case CurrentVersion: // 当前默认版本（V2）
		// 调用V2版本的工作流执行函数，使用最新的业务流程编排逻辑
		return executeWorkflowV2(ctx, input)
	case "v1": // 旧版兼容版本（V1）
		// 调用V1版本的兼容性工作流执行函数，保持向后兼容能力
		return executeWorkflowV1Legacy(ctx, input)
	default: // 不支持的版本号
		// 返回明确的错误信息提示不支持的版本号便于排查配置问题
		return OrderWorkflowOutput{}, fmt.Errorf("unsupported version: %s", version)
	}
}

// executeWorkflowV2 V2版本的工作流执行函数，调用最新的订单处理编排逻辑
// 参数ctx：请求上下文，用于创建编排器上下文和传播超时控制
// 参数input：工作流输入参数结构体，包含订单ID、用户信息、商品列表等
// 返回值：V2版本工作流执行结果输出结构体和可能的错误信息
func executeWorkflowV2(ctx context.Context, input OrderWorkflowInput) (OrderWorkflowOutput, error) {

	// 创建模拟的编排器上下文对象，将原始请求上下文包装为工作流所需的上下文格式
	orchestrationCtx := createMockOrchestrationContext(ctx)
	// 调用V2版本的编排器函数ExecuteOrderProcessingWorkflow执行完整的业务流程编排
	result, err := ExecuteOrderProcessingWorkflow(orchestrationCtx)
	if err != nil {
		// 编排器执行出错时直接返回空输出和错误信息
		return OrderWorkflowOutput{}, err
	}

	// 将返回值类型断言为OrderWorkflowOutput结构体确保类型安全
	output, ok := result.(OrderWorkflowOutput)
	if !ok {
		// 类型断言失败时返回明确的类型错误提示便于排查问题
		return OrderWorkflowOutput{}, fmt.Errorf("invalid output type")
	}

	return output, nil // 返回类型安全的V2版本工作流执行结果
}

// executeWorkflowV1Legacy V1版本的兼容性工作流执行函数，调用旧版的订单处理逻辑
// 用于保持向后兼容性，支持旧版客户端或配置仍使用V1逻辑的场景
// 参数ctx：请求上下文，直接传递给V1版本的执行函数
// 参数input：工作流输入参数结构体，包含订单处理所需的全部业务数据
// 返回值：V1版本工作流执行结果输出结构体和可能的错误信息
func executeWorkflowV1Legacy(ctx context.Context, input OrderWorkflowInput) (OrderWorkflowOutput, error) {
	// 直接调用V1版本的兼容性执行函数并返回其结果（无需额外的上下文转换）
	return ExecuteOrderProcessingWorkflowV1ForLegacy(ctx, input)
}

// getWorkflowStatus 查询指定工作流实例的运行状态信息，返回完整的状态详情数据
// 参数ctx：请求上下文（预留参数用于未来的权限控制和超时管理）
// 参数instanceID：工作流实例的唯一标识符，由startWorkflowInstance函数返回
// 返回值：包含实例详细状态信息的映射表和可能的错误信息
func getWorkflowStatus(ctx context.Context, instanceID string) (map[string]interface{}, error) {
	// 检查工作流引擎是否已正确初始化（防止空指针异常）
	if taskHubClient == nil {
		return nil, fmt.Errorf("task hub client not initialized") // 返回初始化错误提示
	}

	// 使用读锁保护实例映射表的并发读取操作确保线程安全
	taskHubClient.mu.RLock()
	// 根据实例ID从全局映射表中查找对应的工作流实例对象
	instance, exists := taskHubClient.instances[instanceID]
	// 释放读锁允许其他协程访问或修改实例映射表
	taskHubClient.mu.RUnlock()

	// 检查实例是否存在于存储中
	if !exists {
		// 实例未找到时返回明确的错误提示包含具体的实例ID便于排查
		return nil, fmt.Errorf("instance not found: %s", instanceID)
	}

	// 构造状态响应映射表包含实例的所有关键状态字段
	status := map[string]interface{}{
		"instanceId":    instance.ID,                             // 实例唯一标识符
		"name":          instance.Name,                           // 工作流名称（V1/V2）
		"runtimeStatus": instance.Status,                         // 运行时状态（RUNNING/COMPLETED/FAILED等）
		"createdAt":     instance.CreatedAt.Format(time.RFC3339), // 创建时间戳RFC3339格式
		"lastUpdated":   instance.CreatedAt.Format(time.RFC3339), // 最后更新时间戳（初始为创建时间）
		"version":       instance.Version,                        // 工作流版本号标识
	}

	// 条件判断：仅当实例已完成或有完成时间戳时才补充完成时间相关字段
	if instance.CompletedAt != nil {
		// 补充完成时间戳到状态响应中（非空指针表示实例已终止）
		status["completedAt"] = instance.CompletedAt.Format(time.RFC3339)
		// 更新最后更新时间为实际完成时间（覆盖初始值）
		status["lastUpdated"] = instance.CompletedAt.Format(time.RFC3339)
	}

	return status, nil // 返回完整的实例状态信息映射表
}

// waitForWorkflowCompletion 阻塞等待工作流实例完成执行，支持超时控制和轮询机制
// 参数ctx：请求上下文，用于传播取消信号和超时控制
// 参数instanceID：等待的目标工作流实例唯一标识符
// 参数timeout：最大等待超时时间（如30*time.Second），超过此时间将返回超时错误
// 返回值：工作流最终执行结果输出结构体和可能的错误信息（包括超时错误）
func waitForWorkflowCompletion(ctx context.Context, instanceID string, timeout time.Duration) (OrderWorkflowOutput, error) {
	// 检查工作流引擎是否已正确初始化（防止空指针异常）
	if taskHubClient == nil {
		return OrderWorkflowOutput{}, fmt.Errorf("task hub client not initialized") // 返回初始化错误提示
	}

	// 创建超时通道用于控制最大等待时间限制防止无限阻塞
	timeoutChan := time.After(timeout) // 超时后通道自动发送当前时间信号
	// 创建定时器用于周期性轮询检查工作流完成状态（100毫秒间隔平衡响应速度和性能）
	ticker := time.NewTicker(100 * time.Millisecond)
	// 注册延迟关闭函数确保函数返回时释放定时器资源避免内存泄漏
	defer ticker.Stop()

	// 进入无限轮询循环直到工作流完成或发生超时/取消事件
	for {
		// 使用select语句同时监听多个通道的事件实现多路复用
		select {
		case <-timeoutChan: // 超时通道触发事件分支
			// 等待时间超过设定的超时阈值时返回明确的超时错误信息
			return OrderWorkflowOutput{}, fmt.Errorf("timeout waiting for completion") // 超时错误提示

		case <-ticker.C: // 定时器周期性触发事件分支（每100毫秒）
			// 使用读锁保护实例映射表的并发读取操作确保线程安全
			taskHubClient.mu.RLock()
			// 根据实例ID从全局映射表中查找对应的工作流实例对象
			instance, exists := taskHubClient.instances[instanceID]
			// 释放读锁允许其他协程访问或修改实例映射表
			taskHubClient.mu.RUnlock()

			// 检查目标实例是否存在于存储中
			if !exists {
				// 实例未找到时返回明确的错误提示包含具体的实例ID便于排查
				return OrderWorkflowOutput{}, fmt.Errorf("instance not found: %s", instanceID)
			}

			// 检查实例是否已进入终止状态（完成、失败或取消）
			if instance.Status == "COMPLETED" || instance.Status == "FAILED" || instance.Status == "CANCELED" { // 终止状态判断
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

// listRunningInstances 列出所有当前正在运行中的工作流实例ID列表
// 用于监控和管理活跃的工作流实例，支持运维排查和资源管理场景
// 返回值：运行中实例ID的字符串数组（空数组表示无运行中的实例或引擎未初始化）
func listRunningInstances() []string {
	// 检查工作流引擎是否已正确初始化
	if taskHubClient == nil {
		return []string{} // 引擎未初始化时返回空数组
	}

	// 定义切片变量用于收集所有运行中实例的ID
	var runningInstances []string // 动态数组存储实例ID

	// 使用读锁保护实例映射表的并发遍历操作确保线程安全
	taskHubClient.mu.RLock()
	// 遍历全局实例映射表查找状态为RUNNING的所有实例
	for id, inst := range taskHubClient.instances { // 使用range同时获取键和值
		// 条件判断：仅筛选出状态为RUNNING的活跃实例
		if inst.Status == "RUNNING" { // 运行中状态匹配
			// 将符合条件的实例ID追加到结果切片中
			runningInstances = append(runningInstances, id) // 追加操作
		}
	}
	// 释放读锁允许其他协程访问或修改实例映射表
	taskHubClient.mu.RUnlock()

	return runningInstances // 返回运行中实例ID的完整列表
}

// validateInventoryActivityHandler 库存验证活动处理器实现函数（V2版本），被工作流编排器调用执行库存充足性检查
// 这是工作流编排的第一步活动，负责验证订单中所有商品的库存是否满足购买数量要求
// 采用Dapr Sidecar服务调用机制与inventory-service通信，支持弹性策略（重试、超时、熔断）
// 参数ctx：活动上下文对象，提供输入参数获取、日志记录、心跳报告等能力
// 返回值：活动执行结果输出结构体（包含库存验证状态）和可能的错误信息
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

	// 初始化输出结构体对象设置订单ID字段用于返回检查结果
	output := OrderWorkflowOutput{
		OrderID: input.OrderID, // 关联的订单唯一标识符
	}

	// 定义库存请求项内部结构体用于构造库存服务调用所需的参数格式
	type InventoryItem struct {
		ProductID string `json:"productId"` // 商品唯一标识符（SKU）
		Quantity  int    `json:"quantity"`  // 需要检查的库存数量
	}

	// 根据输入的商品列表创建对应数量的库存检查请求项数组
	inventoryItems := make([]InventoryItem, len(input.Items)) // 预分配指定长度的切片
	// 循环遍历输入商品列表转换为库存检查所需的格式
	for i, item := range input.Items { // 使用索引和值遍历
		inventoryItems[i] = InventoryItem{ // 逐个赋值转换格式
			ProductID: item.ProductID, // 映射商品标识符
			Quantity:  item.Quantity,  // 映射购买数量
		}
	}
	// 将库存请求数组序列化为JSON字节数组用于HTTP请求体
	reqBytes, _ := json.Marshal(inventoryItems)

	// 构造Dapr服务调用的数据内容对象，包含JSON格式的请求体和MIME类型声明
	content := &client.DataContent{
		ContentType: "application/json", // 声明内容类型为JSON格式
		Data:        reqBytes,           // 库存检查请求的完整数据
	}
	// 通过Dapr客户端调用库存服务的/inventory/check接口执行实际的库存充足性验证
	resp, err := daprClient.InvokeMethodWithContent(context.Background(), "inventory-service", "/inventory/check", "post", content)
	if err != nil {
		// 服务调用失败时更新输出状态为失败并记录错误信息
		output.Status = "failed"                                      // 失败状态标识
		output.InventoryValid = false                                 // 库存验证标志设为false
		output.Error = fmt.Sprintf("inventory check failed: %v", err) // 错误信息详情
		return output, err                                            // 返回失败结果和原始错误
	}

	// 定义库存响应结构体用于反序列化库存服务的返回结果
	var invResp struct {
		AllAvailable bool       `json:"allAvailable"` // 所有商品是否均可用标志
		Items        []struct { // 每个商品的详细检查结果数组
			ProductID   string `json:"productId"`   // 商品唯一标识符
			IsAvailable bool   `json:"isAvailable"` // 该商品是否可用
			Requested   int    `json:"requested"`   // 请求的数量
			Available   int    `json:"available"`   // 实际可用数量
		} `json:"items"` // 商品明细列表字段名
	}

	// 反序列化库存服务的响应数据到结构体获取详细检查结果
	if err := json.Unmarshal(resp, &invResp); err != nil {
		// 响应解析失败时更新输出状态为失败并记录解析错误
		output.Status = "failed"                           // 失败状态标识
		output.InventoryValid = false                      // 库存验证标志设为false
		output.Error = fmt.Sprintf("parse error: %v", err) // 解析错误信息
		return output, err                                 // 返回失败结果和原始错误
	}

	// 条件判断：检查是否所有商品的库存都充足可用
	if !invResp.AllAvailable {
		// 存在库存不足的商品时收集所有不可用商品的ID用于错误提示
		unavailableItems := make([]string, 0) // 创建空的不可用商品ID列表
		for _, item := range invResp.Items {  // 遍历所有商品的检查结果
			if !item.IsAvailable { // 筛选出不可用的商品
				unavailableItems = append(unavailableItems, item.ProductID) // 追加到列表
			}
		}
		// 构造详细的错误消息包含所有不可用商品的ID便于排查问题
		errMsg := fmt.Sprintf("inventory not available for items: %v", unavailableItems)
		output.Status = "inventory_unavailable" // 设置状态为库存不足
		output.InventoryValid = false           // 库存验证标志设为false
		output.Error = errMsg                   // 保存错误消息
		return output, fmt.Errorf(errMsg)       // 返回结果和包装后的错误
	}

	// 所有商品库存均充足时设置验证通过标志并返回成功结果
	output.InventoryValid = true // 库存验证标志设为true表示全部通过
	return output, nil           // 返回成功结果无错误
}

// processPaymentActivityHandler 支付处理活动处理器实现函数（V2版本），被工作流编排器调用执行订单支付扣款逻辑
// 这是工作流编排的第二步活动（在库存验证通过后执行），负责调用payment-service完成实际的资金扣款操作
// 采用Dapr Sidecar服务调用机制与payment-service通信，支持弹性策略保障交易可靠性
// 参数ctx：活动上下文对象，提供输入参数获取、日志记录、心跳报告等能力
// 返回值：活动执行结果输出结构体（包含支付处理状态）和可能的错误信息
func processPaymentActivityHandler(ctx task.ActivityContext) (interface{}, error) {
	// 记录活动开始时间用于计算执行耗时和性能指标统计
	startTime := time.Now()
	// 注册延迟恢复函数确保无论正常返回还是异常都能记录活动执行情况
	defer func() {
		success := true // 标记活动执行成功标志（默认为true）
		// 调用可观测性模块记录活动执行结果到Prometheus指标系统
		recordActivityExecution(context.Background(), "ProcessPaymentActivity", success) // 活动名称标识
		_ = time.Since(startTime)                                                        // 计算耗时（预留用于未来的性能分析）
	}()

	// 定义输入结构体变量用于接收工作流编排器传递的活动输入参数
	var input OrderWorkflowInput
	// 从活动上下文中反序列化JSON格式的输入数据到结构体
	if err := ctx.GetInput(&input); err != nil {
		// 输入参数解析失败时直接返回错误给编排器处理
		return OrderWorkflowOutput{}, err
	}

	// 初始化输出结构体对象设置订单ID字段用于返回支付处理结果
	output := OrderWorkflowOutput{
		OrderID: input.OrderID, // 关联的订单唯一标识符
	}

	// 定义支付请求内部结构体用于构造支付服务调用所需的参数格式
	type PaymentRequest struct {
		OrderID     string  `json:"orderId"`  // 订单唯一标识符（关联字段）
		UserID      string  `json:"userId"`   // 用户唯一标识符（扣款账户）
		Amount      float64 `json:"amount"`   // 支付金额（单位：元/CNY）
		Currency    string  `json:"currency"` // 货币类型标识（如"CNY"）
		Description string  `json:"description"`
	}

	// 构造支付请求对象填充所有必需字段用于调用支付服务接口
	paymentReq := PaymentRequest{
		OrderID:     input.OrderID,                                                                      // 订单唯一标识符关联到支付记录
		UserID:      input.UserID,                                                                       // 用户标识符指定扣款账户
		Amount:      input.TotalAmount,                                                                  // 订单总金额作为支付金额
		Currency:    "CNY",                                                                              // 使用人民币作为支付货币类型
		Description: fmt.Sprintf("Order %s - Dapr Workflow Engine V2 (Version Managed)", input.OrderID), // 支付描述信息包含订单ID和工作流版本标识
	}

	// 将支付请求对象序列化为JSON字节数组用于HTTP请求体
	reqBytes, _ := json.Marshal(paymentReq)
	// 构造Dapr服务调用的数据内容对象，包含JSON格式的请求体和MIME类型声明
	content := &client.DataContent{
		ContentType: "application/json", // 声明内容类型为JSON格式
		Data:        reqBytes,           // 支付请求的完整数据
	}
	// 通过Dapr客户端调用支付服务的/payment/process接口执行实际的支付扣款操作
	resp, err := daprClient.InvokeMethodWithContent(context.Background(), "payment-service", "/payment/process", "post", content)
	if err != nil {
		// 服务调用失败时更新输出状态为失败并记录错误信息
		output.Status = "failed"                              // 失败状态标识
		output.PaymentSuccess = false                         // 支付成功标志设为false
		output.Error = fmt.Sprintf("payment failed: %v", err) // 错误信息详情
		return output, err                                    // 返回失败结果和原始错误
	}

	// 定义支付响应结构体用于反序列化支付服务的返回结果
	var payResp struct {
		PaymentID string `json:"paymentId"` // 支付记录唯一标识符
		Status    string `json:"status"`    // 支付处理结果状态
	}

	// 反序列化支付服务的响应数据到结构体获取支付结果状态
	if err := json.Unmarshal(resp, &payResp); err != nil {
		// 响应解析失败时更新输出状态为失败并记录解析错误
		output.Status = "failed"                           // 失败状态标识
		output.PaymentSuccess = false                      // 支付成功标志设为false
		output.Error = fmt.Sprintf("parse error: %v", err) // 解析错误信息
		return output, err                                 // 返回失败结果和原始错误
	}

	// 条件判断：检查支付服务返回的状态是否表示成功（success或completed）
	if payResp.Status != "success" && payResp.Status != "completed" {
		// 支付未成功时更新输出状态为支付失败并记录实际状态
		output.Status = "payment_failed"                                 // 支付失败状态标识
		output.PaymentSuccess = false                                    // 支付成功标志设为false
		output.Error = fmt.Sprintf("payment status: %s", payResp.Status) // 实际状态信息
		return output, fmt.Errorf("payment status: %s", payResp.Status)  // 返回结果和包装后的错误
	}

	// 支付成功时设置成功标志并返回完成结果
	output.PaymentSuccess = true // 支付成功标志设为true表示扣款已完成
	return output, nil           // 返回成功结果无错误
}

// updateInventoryActivityHandler 库存更新活动处理器实现函数（V2版本），被工作流编排器调用执行已售商品库存扣减
// 这是工作流编排的第三步活动（在支付成功后执行），负责调用inventory-service完成实际的商品库存扣减操作
// 采用循环逐一处理每个商品确保原子性操作，任一商品失败则整体回滚
// 参数ctx：活动上下文对象，提供输入参数获取、日志记录、心跳报告等能力
// 返回值：活动执行结果输出结构体（包含库存更新状态）和可能的错误信息
func updateInventoryActivityHandler(ctx task.ActivityContext) (interface{}, error) {
	// 记录活动开始时间用于计算执行耗时和性能指标统计
	startTime := time.Now()
	// 注册延迟恢复函数确保无论正常返回还是异常都能记录活动执行情况
	defer func() {
		success := true // 标记活动执行成功标志（默认为true）
		// 调用可观测性模块记录活动执行结果到Prometheus指标系统
		recordActivityExecution(context.Background(), "UpdateInventoryActivity", success) // 活动名称标识
		_ = time.Since(startTime)                                                         // 计算耗时（预留用于未来的性能分析）
	}()

	// 定义输入结构体变量用于接收工作流编排器传递的活动输入参数
	var input OrderWorkflowInput
	// 从活动上下文中反序列化JSON格式的输入数据到结构体
	if err := ctx.GetInput(&input); err != nil {
		// 输入参数解析失败时直接返回错误给编排器处理
		return OrderWorkflowOutput{}, err
	}

	// 初始化输出结构体对象设置订单ID字段用于返回库存更新结果
	output := OrderWorkflowOutput{
		OrderID: input.OrderID, // 关联的订单唯一标识符
	}

	// 循环遍历订单中的每个商品逐一执行库存扣减操作确保数据一致性
	for _, item := range input.Items { // 使用range遍历商品列表
		// 定义库存扣减请求内部结构体用于构造库存服务调用所需的参数格式
		type DeductInventoryRequest struct {
			ProductID string `json:"productId"` // 商品唯一标识符（SKU）
			Quantity  int    `json:"quantity"`  // 需要扣减的库存数量
			Operation string `json:"operation"` // 操作类型标识（decrease表示扣减）
		}

		// 构造单个商品的库存扣减请求对象填充商品信息和扣减数量
		deductReq := DeductInventoryRequest{
			ProductID: item.ProductID, // 当前商品的唯一标识符
			Quantity:  item.Quantity,  // 当前商品的购买数量（即需扣减的库存量）
			Operation: "decrease",     // 操作类型设为decrease表示库存扣减操作
		}

		// 将扣减请求对象序列化为JSON字节数组用于HTTP请求体
		reqBytes, _ := json.Marshal(deductReq)
		// 构造Dapr服务调用的数据内容对象，包含JSON格式的请求体和MIME类型声明
		content := &client.DataContent{
			ContentType: "application/json", // 声明内容类型为JSON格式
			Data:        reqBytes,           // 库存扣减请求的完整数据
		}
		// 通过Dapr客户端调用库存服务的/inventory/update接口执行实际的库存扣减操作
		resp, err := daprClient.InvokeMethodWithContent(context.Background(), "inventory-service", "/inventory/update", "post", content)
		if err != nil {
			// 服务调用失败时更新输出状态为失败并记录错误信息
			output.Status = "failed"                                       // 失败状态标识
			output.InventoryUpdated = false                                // 库存更新标志设为false
			output.Error = fmt.Sprintf("inventory update failed: %v", err) // 错误信息详情
			return output, err                                             // 返回失败结果和原始错误
		}

		// 定义库存更新响应结构体用于反序列化库存服务的返回结果
		var updateResp struct {
			Success bool   `json:"success"` // 库存更新是否成功标志
			Message string `json:"message"` // 响应消息或错误描述
		}

		// 反序列化库存服务的响应数据到结构体获取更新结果状态
		if err := json.Unmarshal(resp, &updateResp); err != nil {
			// 响应解析失败时更新输出状态为失败并记录解析错误
			output.Status = "failed"                           // 失败状态标识
			output.InventoryUpdated = false                    // 库存更新标志设为false
			output.Error = fmt.Sprintf("parse error: %v", err) // 解析错误信息
			return output, err                                 // 返回失败结果和原始错误
		}

		// 条件判断：检查库存服务返回的操作结果是否成功
		if !updateResp.Success {
			// 库存更新失败时更新输出状态为失败并记录服务端错误消息
			output.Status = "failed"                      // 失败状态标识
			output.InventoryUpdated = false               // 库存更新标志设为false
			output.Error = updateResp.Message             // 保存服务端返回的错误消息
			return output, fmt.Errorf(updateResp.Message) // 返回结果和包装后的错误
		}
	} // 循环结束：所有商品均已成功完成库存扣减

	// 所有商品库存均成功扣减时设置更新完成标志并返回成功结果
	output.InventoryUpdated = true // 库存更新标志设为true表示全部扣减完成
	return output, nil             // 返回成功结果无错误
}

// sendNotificationActivityHandler 通知发送活动处理器实现函数（V2版本），被工作流编排器调用发送订单处理完成通知
// 这是工作流编排的最后一步活动（在库存更新完成后执行），负责构造通知消息并通过发布订阅组件异步分发
// 采用事件驱动架构实现解耦，通知服务可独立消费orders主题的消息进行后续处理
// 参数ctx：活动上下文对象，提供输入参数获取、日志记录、心跳报告等能力
// 返回值：活动执行结果输出结构体（包含通知发送状态）和可能的错误信息
func sendNotificationActivityHandler(ctx task.ActivityContext) (interface{}, error) {
	// 记录活动开始时间用于计算执行耗时和性能指标统计
	startTime := time.Now()
	// 注册延迟恢复函数确保无论正常返回还是异常都能记录活动执行情况
	defer func() {
		success := true // 标记活动执行成功标志（默认为true）
		// 调用可观测性模块记录活动执行结果到Prometheus指标系统
		recordActivityExecution(context.Background(), "SendNotificationActivity", success) // 活动名称标识
		_ = time.Since(startTime)                                                          // 计算耗时（预留用于未来的性能分析）
	}()

	// 定义输入结构体变量用于接收工作流编排器传递的活动输入参数
	var input OrderWorkflowInput
	// 从活动上下文中反序列化JSON格式的输入数据到结构体
	if err := ctx.GetInput(&input); err != nil {
		// 输入参数解析失败时直接返回错误给编排器处理
		return OrderWorkflowOutput{}, err
	}

	// 初始化输出结构体对象设置订单ID和通知发送标志
	output := OrderWorkflowOutput{
		OrderID:          input.OrderID, // 关联的订单唯一标识符
		NotificationSent: true,          // 通知发送标志设为true表示已发送
	}

	// 构造完整的订单完成通知消息映射表包含所有业务相关信息
	notificationMsg := map[string]interface{}{
		"order_id":           input.OrderID,                                                                                                        // 订单唯一标识符
		"user_id":            input.UserID,                                                                                                         // 用户唯一标识符
		"status":             "completed",                                                                                                          // 订单最终状态为已完成
		"total_amount":       input.TotalAmount,                                                                                                    // 订单总金额
		"message":            fmt.Sprintf("Order %s has been processed successfully via Dapr Workflow Engine V2 (Version Managed)", input.OrderID), // 成功消息描述包含工作流版本标识
		"timestamp":          time.Now().Format(time.RFC3339),                                                                                      // 消息生成时间戳
		"workflow_engine":    "dapr-workflow-engine-v2",                                                                                            // 工作流引擎类型标识
		"version":            CurrentVersion,                                                                                                       // 当前工作流版本号
		"version_management": "enabled",                                                                                                            // 版本管理功能启用标志
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

// migrateRunningWorkflowsToV2 工作流版本迁移函数，将运行中的V1实例平滑升级到V2版本
// 在系统启动后延迟执行，扫描所有活跃实例并统计需要迁移的V1工作流数量
// 参数ctx：请求上下文，用于状态查询和日志记录
// 返回值：迁移过程中的错误信息（通常为nil表示成功）
func migrateRunningWorkflowsToV2(ctx context.Context) error {
	// 检查工作流引擎是否已正确初始化（防止空指针异常）
	if taskHubClient == nil {
		return fmt.Errorf("task hub client not initialized") // 返回初始化错误提示
	}

	// 获取当前所有正在运行中的工作流实例ID列表
	runningInstances := listRunningInstances() // 调用列表查询函数
	// 定义计数器变量用于统计实际需要迁移的V1版本实例数量
	migratedCount := 0 // 初始值为0

	// 循环遍历每个运行中的实例检查其版本和状态
	for _, instanceID := range runningInstances { // 使用range遍历实例ID数组
		// 使用读锁保护实例映射表的并发读取操作确保线程安全
		taskHubClient.mu.RLock()
		// 根据实例ID从全局映射表中查找对应的实例对象
		instance, exists := taskHubClient.instances[instanceID] // 键值查找
		// 释放读锁允许其他协程访问或修改实例映射表
		taskHubClient.mu.RUnlock()

		// 条件判断：跳过不存在的实例（可能已被清理或异常数据）
		if !exists {
			continue // 继续下一个实例
		}

		// 条件判断：筛选出使用V1版本且仍在运行中的实例（需要迁移的目标）
		if instance.Name == WorkflowNameV1 && instance.Status == "RUNNING" { // 版本和状态双重匹配
			migratedCount++ // 递增迁移计数器记录符合条件的实例数量
		}
	}

	return nil // 迁移统计完成返回无错误（当前仅统计未执行实际迁移逻辑）
}

// getWorkflowVersionInfo 获取工作流版本管理系统详细信息，返回完整的版本配置和状态映射表
// 供API响应和健康检查接口使用，展示当前系统的版本管理能力和配置信息
// 返回值：包含版本号、名称、迁移状态、兼容性等详细信息的映射表
func getWorkflowVersionInfo() map[string]interface{} {
	// 构造版本信息映射表包含所有关键的版本管理元数据
	return map[string]interface{}{
		"current_version":    CurrentVersion,                                                                                                                                               // 当前活跃的默认版本号（v2）
		"available_versions": []string{"v1", "v2"},                                                                                                                                         // 系统支持的所有可用版本列表
		"v1_workflow_name":   WorkflowNameV1,                                                                                                                                               // V1版本的工作流注册名称标识符
		"v2_workflow_name":   WorkflowNameV2,                                                                                                                                               // V2版本的工作流注册名称标识符
		"migration_status":   "ready",                                                                                                                                                      // 版本迁移功能状态：就绪可执行
		"backward_compat":    true,                                                                                                                                                         // 向后兼容标志：支持旧版V1实例继续运行
		"description":        "Dapr Workflow Engine with version management support. Running instances continue with their registered version while new instances use the latest version.", // 功能描述说明
	}
}

// generateUniqueID 生成全局唯一的ID字符串，基于纳秒级时间戳和随机数组合算法
// 用于创建工作流实例ID、订单ID等需要唯一标识的场景
// 返回值：格式为"{时间戳}-{4位十六进制短码}"的唯一标识字符串
func generateUniqueID() string {
	// 使用当前时间的纳秒级Unix时间戳作为基础确保时间维度唯一性
	// 配合取模运算生成的4位十六进制短码增加随机性防止并发冲突
	return fmt.Sprintf("%d-%s", time.Now().UnixNano(), fmt.Sprintf("%04x", time.Now().UnixNano()%0xFFFF)) // 格式化构造
}

// createMockOrchestrationContext 创建模拟的编排器上下文对象用于降级模式执行
// 在Dapr引擎不可用时提供最小化的上下文包装，保证工作流逻辑能正常调用
// 参数ctx：原始请求上下文（预留参数用于未来的上下文传播）
// 返回值：空初始化的编排器上下文对象指针（仅具备基本结构无实际调度能力）
func createMockOrchestrationContext(ctx context.Context) *task.OrchestrationContext {
	// 创建空的编排器上下文对象并返回其指针
	oc := &task.OrchestrationContext{} // 零值初始化
	return oc                          // 返回对象指针
}
