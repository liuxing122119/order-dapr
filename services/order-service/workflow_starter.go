// Dapr多运行时云原生应用实践 - 工作流系统启动与管理模块
// 功能：工作流初始化、订单创建入口、实例生命周期管理、版本迁移、监控协调
package main

import (
	"context"       // 上下文管理，用于控制请求生命周期和超时
	"encoding/json" // JSON序列化与反序列化库
	"fmt"           // 格式化输入输出库用于错误消息构造
	"sync"          // 同步原语（sync.Map并发安全映射表）
	"time"          // 时间处理和格式化工具

	"github.com/dapr/go-sdk/service/common" // Dapr通用服务接口定义
	"github.com/google/uuid"                // UUID生成库用于生成全局唯一标识符
)

// WorkflowInstance 工作流运行实例结构体，记录单个工作流从创建到完成的完整生命周期信息
// 用于对外暴露工作流状态查询接口和内部的状态追踪管理
type WorkflowInstance struct {
	InstanceID   string              `json:"instanceId"`            // 实例唯一标识符，全局唯一用于查询和追踪
	OrderID      string              `json:"orderId"`               // 关联的订单唯一标识符（外键关系）
	Status       string              `json:"status"`                // 运行状态：running/completed/failed/canceled
	CustomStatus string              `json:"customStatus"`          // 自定义细粒度状态：validating_inventory/payment_processing等
	CreatedAt    time.Time           `json:"createdAt"`             // 实例创建时间戳，记录工作流启动时刻
	CompletedAt  *time.Time          `json:"completedAt,omitempty"` // 完成时间戳指针，未完成时为nil
	Output       OrderWorkflowOutput `json:"output,omitempty"`      // 工作流最终执行结果输出数据
	Error        string              `json:"error,omitempty"`       // 错误信息字符串，仅在失败时有值
}

var (
	// workflowInstances 全局并发安全的工作流实例存储映射表
	// 使用sync.Map保证高并发场景下的线程安全读写操作
	workflowInstances sync.Map // 键为instanceID，值为WorkflowInstance对象
	// instanceCounter 全局原子计数器用于生成递增的实例序列号
	// 配合UUID短码确保实例ID的全局唯一性和可读性
	instanceCounter int64 // 每次生成新实例时自增1
)

// initializeWorkflowSystem 初始化工作流系统的完整环境，包括可观测性和引擎核心组件
// 这是应用启动时必须调用的第一个初始化函数，建立工作流运行所需的全部基础设施
// 返回值：初始化过程中的错误信息（通常为nil表示成功）
func initializeWorkflowSystem() error {
	// 第一步：初始化OpenTelemetry可观测性系统，配置Prometheus指标导出和自定义监控指标
	initTelemetry() // 建立完整的监控能力用于后续的性能追踪和故障诊断
	// 第二步：初始化Dapr工作流引擎核心组件，注册编排器和活动处理器到任务注册表
	return initDaprWorkflowEngine() // 返回引擎初始化结果（可能包含错误）
}

// startWorkflowEngine 启动工作流引擎的后台服务协程，准备接收和处理工作流实例
// 提供短暂的启动延迟确保所有依赖组件已完全就绪后再开始处理请求
// 返回值：启动过程中的错误信息（通常为nil表示成功）
func startWorkflowEngine() error {
	// 等待500毫秒让Dapr Sidecar和数据库连接等依赖服务完成初始化握手
	time.Sleep(500 * time.Millisecond) // 启动缓冲时间防止资源竞争
	return nil                         // 启动完成返回无错误
}

// initDaprWorkflowEngine 初始化Dapr持久化任务框架的工作流引擎并配置版本迁移机制
// 建立任务注册表、编排器映射、活动处理器绑定，并启动后台版本迁移协程
// 返回值：初始化过程中的错误信息（通常为nil表示成功）
func initDaprWorkflowEngine() error {

	// 调用底层引擎初始化函数创建任务注册表并注册所有V1/V2版本的编排器和活动处理器
	if err := initWorkflowEngine(); err != nil {
		// 引擎核心初始化失败时忽略错误继续运行（降级模式支持）
		return nil // 优雅降级：即使引擎失败也不阻塞主流程启动
	}

	// 启动后台协程延迟执行版本迁移逻辑，将旧版V1工作流实例迁移到V2版本
	go func() { // 异步执行不阻塞主流程启动
		// 等待10秒确保系统完全稳定后再开始迁移操作（避免启动期间的并发冲突）
		time.Sleep(10 * time.Second) // 迁移延迟时间
		ctx := context.Background()  // 创建独立上下文用于迁移操作
		// 调用版本迁移函数将仍在运行的V1实例平滑升级到V2版本逻辑
		migrateRunningWorkflowsToV2(ctx) // 执行实际的迁移逻辑
	}()

	return nil // 初始化完成返回无错误
}

// handleCreateOrderWithWorkflow 订单创建接口处理器（带工作流编排），系统的核心业务入口
// 接收前端或API调用方提交的创建订单请求，初始化订单数据并启动异步工作流处理流程
// 支持Dapr工作流引擎和降级模式两种执行路径，确保高可用性和容错能力
// 参数ctx：请求上下文，用于状态管理操作和超时控制
// 参数in：Dapr调用事件对象，包含CreateOrderRequest格式的JSON请求数据（用户ID和商品列表）
// 返回值：包含订单ID、实例ID、工作流引擎信息的详细响应对象或错误信息
func handleCreateOrderWithWorkflow(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	// 定义请求结构体变量用于接收反序列化的下单请求数据
	var req CreateOrderRequest
	// 反序列化输入的JSON请求数据到请求结构体
	if err := json.Unmarshal(in.Data, &req); err != nil {
		// JSON解析失败时返回格式错误异常提示调用方检查请求格式
		return nil, fmt.Errorf("invalid order request: %w", err) // 错误包装保留原始信息
	}

	// 校验必填字段的有效性确保业务数据的完整性
	if req.UserID == "" || len(req.Items) == 0 { // 用户ID为空或商品列表为空
		return nil, fmt.Errorf("userId and items are required") // 返回明确的必填字段缺失错误
	}

	// 生成全局唯一的订单标识符使用UUID v4算法保证不重复
	orderID := uuid.New().String() // 36字符的标准UUID格式字符串
	// 生成全局唯一的工作流实例标识符用于追踪和管理此订单的处理过程
	instanceID := generateInstanceID() // 调用自定义生成函数

	// 构造完整的订单对象初始化所有字段为初始状态值
	order := Order{
		OrderID:          orderID,                         // 刚生成的订单唯一标识符
		UserID:           req.UserID,                      // 下单用户的标识符
		Items:            req.Items,                       // 购买的商品明细列表数组
		Status:           "pending",                       // 初始状态为待处理（等待工作流启动）
		CreatedAt:        time.Now().Format(time.RFC3339), // 订单创建时间戳RFC3339格式
		Version:          1,                               // 乐观锁初始版本号
		UserValidated:    false,                           // 用户验证标志初始为false
		InventoryChecked: false,                           // 库存检查标志初始为false
		PaymentProcessed: false,                           // 支付处理标志初始为false
	}

	// 调用总金额计算函数遍历商品明细累加计算订单应付金额
	totalAmount := calculateTotalAmount(req.Items) // 单价×数量的总和
	order.TotalAmount = totalAmount                // 将计算结果赋值到订单对象

	// 调用双写持久化函数将新创建的订单保存到PostgreSQL数据库和Redis状态存储
	saveOrderToState(ctx, order) // 确保订单数据立即持久化防止丢失

	// 调用可观测性模块记录当前处理的订单金额到Prometheus指标系统用于实时监控
	recordOrderAmount(ctx, orderID, totalAmount) // 指标名称：order_total_amount

	// 构造工作流输入参数对象包含订单处理所需的全部业务数据
	workflowInput := OrderWorkflowInput{
		OrderID:     orderID,        // 关联的订单唯一标识符
		UserID:      req.UserID,     // 用户唯一标识符
		Items:       req.Items,      // 商品明细列表（原样传递）
		TotalAmount: totalAmount,    // 订单总金额（已计算）
		Version:     CurrentVersion, // 使用当前默认的工作流版本号（V2）
	}

	// 定义变量存储实际使用的工作流实例ID和可能的启动错误
	var actualInstanceID string // 最终确定的实例ID（可能因降级而改变）
	var startErr error          // 工作流启动过程中的错误信息

	// 条件判断：根据Dapr工作流引擎是否可用选择不同的启动路径
	if taskHubClient != nil { // 引擎正常可用的情况
		// 通过引擎启动正式的Dapr工作流实例获取引擎分配的真实实例ID
		actualInstanceID, startErr = startWorkflowInstance(ctx, workflowInput) // 异步启动工作流
		if startErr != nil {                                                   // 工作流启动失败时的降级处理
			// 使用本地生成的备用实例ID替代引擎返回的ID
			actualInstanceID = instanceID // 回退到预生成的ID
		}
	} else { // 引擎不可用的情况（未初始化或已销毁）
		// 直接使用本地生成的实例ID走降级执行路径
		actualInstanceID = instanceID // 使用预生成的ID
	}

	// 构造工作流运行实例对象记录此订单的完整生命周期信息
	instance := WorkflowInstance{
		InstanceID:   actualInstanceID,       // 实际使用的实例标识符
		OrderID:      orderID,                // 关联的订单标识符
		Status:       "running",              // 运行状态设为running表示正在执行中
		CustomStatus: "validating_inventory", // 细粒度状态：正在验证库存（第一步）
		CreatedAt:    time.Now(),             // 实例创建时间戳
	}

	// 将新创建的实例注册到全局并发安全映射表中供后续查询和监控
	workflowInstances.Store(actualInstanceID, instance) // 键值对存储

	// 根据引擎可用性选择不同的后台执行策略启动异步工作流处理
	if taskHubClient == nil || startErr != nil { // 引擎不可用或启动失败的情况
		// 启动后台协程执行降级模式的工作流逻辑（V1兼容版本直接执行）
		go func() { // 异步协程不阻塞响应返回
			backgroundCtx := context.Background() // 创建独立的后台上下文
			// 调用降级执行函数直接在当前进程内运行完整的工作流逻辑
			executeAndMonitorWorkflow(backgroundCtx, actualInstanceID, workflowInput) // V1兼容路径
		}()
	} else { // 引擎正常可用且工作流成功启动的情况
		// 启动后台协程监控Dapr引擎管理的正式工作流实例执行进度
		go func() { // 异步协程不阻塞响应返回
			backgroundCtx := context.Background() // 创建独立的后台上下文
			// 调用监控函数轮询检查Dapr引擎中的工作流完成状态
			monitorDaprWorkflow(backgroundCtx, actualInstanceID, workflowInput) // V2标准路径
		}()
	}

	// 调用版本信息查询函数获取当前工作流系统的完整版本配置详情
	versionInfo := getWorkflowVersionInfo() // 包含V1/V2版本的配置和迁移状态

	// 构造完整的订单创建成功响应对象包含所有关键信息和辅助链接
	response := map[string]interface{}{
		"orderId":    orderID,                                                                  // 新创建的订单唯一标识符（供前端展示和后续查询使用）
		"status":     "processing",                                                             // 订单当前状态：正在异步处理中（非最终状态）
		"instanceId": actualInstanceID,                                                         // 工作流实例标识符（用于追踪处理进度）
		"message":    "Order created and Dapr Workflow Engine started with version management", // 成功提示消息
		"engineInfo": map[string]string{ // 工作流引擎详细技术信息映射表
			"type":              "dapr-workflow-engine-v2",                 // 引擎类型标识：V2版本
			"stateManagement":   "event-sourcing (durabletask-go backend)", // 状态管理方案
			"faultTolerance":    "enabled (retry/timeout/circuit-breaker)", // 容错机制说明
			"tracing":           "opentelemetry + jaeger",                  // 分布式追踪集成方案
			"metrics":           "prometheus (:9090/metrics)",              // 指标暴露端点地址
			"versionManagement": "enabled",                                 // 版本管理功能启用标志
			"currentVersion":    CurrentVersion,                            // 当前活跃的工作流版本号
			"backend":           "gRPC (localhost:4001)",                   // 后端通信协议和地址
		},
		"versionInfo": versionInfo, // 版本管理系统详细信息（V1/V2配置对比）
		"workflowActivities": []map[string]string{ // 工作流步骤清单（4个活动的执行顺序和说明）
			{"step": "1", "name": "ValidateInventoryActivity", "description": "验证库存活动：调用库存服务确认所需库存充足"},
			{"step": "2", "name": "ProcessPaymentActivity", "description": "处理支付活动：调用支付服务扣款"},
			{"step": "3", "name": "UpdateInventoryActivity", "description": "更新库存活动：调用库存服务扣减库存"},
			{"step": "4", "name": "SendNotificationActivity", "description": "发送通知活动：发送订单状态通知"},
		},
		"endpoints": map[string]string{ // 相关API端点地址映射表供客户端快速访问
			"getOrder": "/order/get?orderId=" + orderID, // 订单查询端点URL（已填充订单ID参数）
			"health":   "/health",                       // 健康检查端点URL
		},
	}

	// 将完整的响应对象序列化为JSON格式字节数组用于HTTP响应体
	data, _ := json.Marshal(response)
	// 返回标准的Dapr内容对象包含JSON数据和MIME类型声明
	return &common.Content{
		Data:        data,               // 完整的响应数据JSON字节流
		ContentType: "application/json", // 声明内容类型为JSON格式
	}, nil // 处理成功返回无错误
}

// executeAndMonitorWorkflow 降级模式的工作流执行与监控函数（V1兼容路径）
// 在Dapr工作流引擎不可用或启动失败时直接在当前进程内执行完整的工作流逻辑
// 同步执行所有活动步骤并实时更新实例状态，确保业务流程不中断
// 参数ctx：后台协程上下文，用于状态更新操作和指标记录
// 参数instanceID：工作流实例的唯一标识符（用于状态追踪）
// 参数input：工作流输入参数结构体，包含订单处理所需的全部业务数据
func executeAndMonitorWorkflow(ctx context.Context, instanceID string, input OrderWorkflowInput) {
	// 更新实例状态为运行中并设置细粒度状态为正在验证库存（第一步开始）
	updateInstanceStatus(instanceID, "running", "validating_inventory") // 状态同步到映射表

	// 调用V1兼容版本的执行函数直接在当前进程内顺序执行完整的4步工作流逻辑
	output, err := ExecuteOrderProcessingWorkflowV1ForLegacy(ctx, input) // 同步阻塞等待完成

	// 记录当前时间戳作为工作流完成的时刻（无论成功或失败都需要记录）
	now := time.Now() // 获取当前系统时间

	// 根据执行结果更新实例的最终状态信息（成功或失败两种分支）
	if err != nil { // 工作流执行出错的情况
		// 更新实例状态为失败并保存错误详情和输出结果
		updateInstanceCompleted(instanceID, "failed", output, err.Error(), now) // 失败状态+错误消息
	} else { // 工作流执行成功的情况
		// 更新实例状态为已完成并保存成功的输出结果
		updateInstanceCompleted(instanceID, "completed", output, "", now) // 完成状态无错误
	}

	// 调用订单状态同步函数将工作流最终结果回写到订单数据中保持一致性
	updateOrderFromWorkflowOutput(ctx, input.OrderID, output) // 更新关联的订单状态
}

// monitorDaprWorkflow Dapr引擎模式的工作流监控函数（V2标准路径）
// 异步轮询监控由Dapr工作流引擎管理的正式工作流实例执行进度直到完成或超时
// 不直接参与业务逻辑执行，仅负责状态追踪和结果收集
// 参数ctx：后台协程上下文，用于超时控制和状态查询操作
// 参数instanceID：被监控的工作流实例唯一标识符（由Dapr引擎分配）
// 参数input：原始工作流输入参数结构体（用于构造错误响应时的回填数据）
func monitorDaprWorkflow(ctx context.Context, instanceID string, input OrderWorkflowInput) {
	// 更新实例状态为运行中并设置细粒度状态为Dapr引擎处理中（区别于降级模式的描述）
	updateInstanceStatus(instanceID, "running", "dapr_workflow_engine_processing") // 引擎托管标识

	// 设置最大等待超时时间为5分钟防止无限期阻塞（可根据业务需求调整）
	timeout := 5 * time.Minute // 超时阈值配置
	// 调用等待完成函数阻塞轮询检查Dapr引擎中的工作流是否已终止
	output, err := waitForWorkflowCompletion(ctx, instanceID, timeout) // 带超时的异步等待

	// 记录当前时间戳作为监控结束的时刻（无论正常完成还是超时都需要记录）
	now := time.Now() // 获取当前系统时间

	// 根据等待结果更新实例的最终状态信息（成功、失败或超时三种情况）
	if err != nil { // 等待过程中发生错误或超时的情况
		updateInstanceCompleted(instanceID, "failed", OrderWorkflowOutput{
			OrderID:         input.OrderID,  // 回填订单ID便于关联
			Status:          "failed",       // 失败状态标识
			Error:           err.Error(),    // 保存实际的错误消息
			WorkflowVersion: CurrentVersion, // 标记使用当前版本
		}, err.Error(), now)
	} else { // 工作流正常完成的情况
		// 更新实例状态为已完成并保存引擎返回的实际输出结果
		updateInstanceCompleted(instanceID, "completed", output, "", now) // 完成状态无错误
	}

	// 调用订单状态同步函数将工作流最终结果回写到订单数据中保持一致性
	updateOrderFromWorkflowOutput(ctx, input.OrderID, output) // 更新关联的订单状态
}

// ExecuteOrderProcessingWorkflowV1ForLegacy V1版本工作流执行入口包装函数（降级兼容专用）
// 为降级模式提供统一的调用接口，强制使用V1版本的逻辑确保向后兼容性
// 参数ctx：请求上下文，用于传播给实际的工作流执行函数
// 参数input：工作流输入参数结构体（可能来自V2格式的请求）
// 返回值：V1版本工作流执行结果输出结构和可能的错误信息
func ExecuteOrderProcessingWorkflowV1ForLegacy(ctx context.Context, input OrderWorkflowInput) (OrderWorkflowOutput, error) {

	// 创建输入参数的副本避免修改原始数据影响其他逻辑
	legacyInput := input // 值拷贝创建独立副本
	// 强制覆盖版本号为V1确保使用旧版逻辑执行（忽略输入中的版本设置）
	legacyInput.Version = "v1" // 版本号硬编码为v1

	// 调用实际的V1版本工作流执行函数传入修改后的参数并返回结果
	output, err := executeLegacyWorkflowV1(ctx, legacyInput) // 执行V1逻辑
	return output, err                                       // 直接透传返回值给调用方
}

// executeLegacyWorkflowV1 V1版本的工作流核心实现函数（降级模式直接执行路径）
// 在当前进程内同步顺序执行4个活动步骤：库存验证→支付处理→库存更新→通知发送
// 与Dapr引擎管理的编排器不同，此函数直接调用各活动的处理逻辑无需中间调度层
// 参数ctx：请求上下文，用于可观测性指标记录和状态传播
// 参数input：工作流输入参数结构体（已强制设为V1版本）
// 返回值：V1版本工作流执行结果输出结构和可能的错误信息
func executeLegacyWorkflowV1(ctx context.Context, input OrderWorkflowInput) (OrderWorkflowOutput, error) {
	// 记录工作流开始时间用于计算总执行耗时和性能指标统计
	startTime := time.Now() // 起始时间戳
	// 调用可观测性模块记录工作流启动事件到Prometheus指标系统获取监控上下文
	wfCtx := recordWorkflowStart(ctx, WorkflowNameV1) // 传入V1版本名称标识
	// 注册延迟恢复函数确保无论正常完成还是异常都能记录工作流完成事件
	defer func() {
		// 调用可观测性模块记录工作流完成状态包含总耗时信息
		recordWorkflowComplete(wfCtx, WorkflowNameV1, "completed", time.Since(startTime)) // 记录耗时
	}()

	// 初始化输出结构体对象设置订单ID和工作流版本号字段（标记为V1版本）
	output := OrderWorkflowOutput{
		OrderID:         input.OrderID, // 关联的订单唯一标识符
		WorkflowVersion: "v1",          // 标记使用V1版本的逻辑执行
	}

	// ===== V1第一步：调用旧版库存验证活动实现 =====
	output = executeValidateInventoryActivityLegacy(wfCtx, output, input) // 使用V1专用函数
	// 检查库存验证步骤是否失败或库存不足（与V2相同的错误检查逻辑）
	if output.Status == "failed" || output.Status == "inventory_unavailable" { // 错误状态判断
		return output, fmt.Errorf("workflow v1 failed at inventory validation") // 终止并返回错误
	}

	// ===== V1第二步：调用旧版支付处理活动实现 =====
	output = executeProcessPaymentActivityLegacy(wfCtx, output, input) // 使用V1专用函数
	// 检查支付处理步骤是否失败或支付未成功
	if output.Status == "failed" || output.Status == "payment_failed" { // 错误状态判断
		return output, fmt.Errorf("workflow v1 failed at payment processing") // 终止并返回错误
	}

	// ===== V1第三步：调用旧版库存更新活动实现 =====
	output = executeUpdateInventoryActivityLegacy(wfCtx, output, input) // 使用V1专用函数
	// 检查库存更新步骤是否失败或更新未成功
	if output.Status == "failed" || output.Status == "inventory_update_failed" { // 错误状态判断
		return output, fmt.Errorf("workflow v1 failed at inventory update") // 终止并返回错误
	}

	// ===== V1第四步：调用旧版通知发送活动实现 =====
	executeSendNotificationActivityLegacy(wfCtx, output, input) // 使用V1专用函数（不等待结果）
	output.NotificationSent = true                              // 标记通知已发送（乐观设置）
	output.Status = "completed"                                 // 设置最终成功状态

	return output, nil // 返回V1版本的完整成功结果无错误
}

// updateOrderFromWorkflowOutput 订单状态同步函数，将工作流最终执行结果回写到订单数据中
// 确保订单状态与工作流结果保持一致性，更新状态、时间戳、版本号和业务标志字段
// 参数ctx：请求上下文，用于状态存储查询和持久化操作
// 参数orderID：需要更新的目标订单唯一标识符
// 参数output：工作流最终输出结构体，包含各步骤的执行结果状态
func updateOrderFromWorkflowOutput(ctx context.Context, orderID string, output OrderWorkflowOutput) {
	// 通过Dapr客户端从Redis状态存储中查询当前订单的最新数据
	item, err := daprClient.GetState(ctx, stateStoreName, "order-"+orderID, nil) // 键名格式："order-"+orderId
	if err != nil {
		// 查询失败时直接返回不进行后续操作（避免覆盖错误数据）
		return // 静默忽略错误（生产环境应记录日志）
	}

	// 检查查询结果是否为空（订单不存在的情况）
	if item == nil || len(item.Value) == 0 {
		// 订单不存在时直接返回不创建新记录（仅更新已有订单）
		return // 静默忽略空数据
	}

	// 反序列化查询到的订单数据到Order结构体获取当前状态
	var order Order
	json.Unmarshal(item.Value, &order) // JSON反序列化到结构体

	// 更新订单的核心业务字段反映工作流的最终执行结果
	order.Status = output.Status                      // 同步工作流最终状态（completed/failed等）
	order.UpdatedAt = time.Now().Format(time.RFC3339) // 更新最后修改时间戳
	order.Version++                                   // 递增乐观锁版本号防止并发冲突
	order.InventoryChecked = output.InventoryValid    // 同步库存验证结果标志
	order.PaymentProcessed = output.PaymentSuccess    // 同步支付处理成功标志

	// 调用双写持久化函数将更新后的订单数据保存回数据库和状态存储
	saveOrderToState(ctx, order) // 完成状态同步
}

// generateInstanceID 生成全局唯一的工作流实例标识符
// 使用递增计数器配合UUID短码确保实例ID的全局唯一性和可读性（便于日志追踪和调试）
// 返回值：格式为"wf-{序号}-{UUID短码}"的字符串标识符
func generateInstanceID() string {
	// 原子递增全局计数器获取当前序号（保证并发安全）
	instanceCounter++ // 自增操作
	// 格式化构造实例ID字符串包含序号和8位UUID短码
	return fmt.Sprintf("wf-%d-%s", instanceCounter, uuid.New().String()[:8]) // UUID截取前8位
}

// updateInstanceStatus 更新工作流实例的运行状态和细粒度自定义状态
// 在工作流执行过程中实时更新进度信息供外部查询接口使用
// 参数instanceID：目标工作流实例的唯一标识符
// 参数status：主状态值（running/completed/failed/canceled）
// 参数customStatus：细粒度自定义状态（validating_inventory/payment_processing等）
func updateInstanceStatus(instanceID string, status, customStatus string) {
	// 从全局并发安全映射表中加载指定实例的当前数据
	if obj, ok := workflowInstances.Load(instanceID); ok { // 查找并类型断言成功
		// 类型断言将通用接口转换为具体的WorkflowInstance结构体
		instance := obj.(WorkflowInstance) // 接口到具体类型的转换
		// 更新实例的主运行状态字段
		instance.Status = status // 新的主状态值
		// 更新实例的细粒度自定义状态字段（反映当前正在执行的步骤）
		instance.CustomStatus = customStatus // 新的自定义状态值
		// 将更新后的实例对象重新存回映射表覆盖旧数据（并发安全）
		workflowInstances.Store(instanceID, instance) // 更新存储
	}
	// 实例不存在时静默忽略（可能已被清理或从未注册）
}

// updateInstanceCompleted 标记工作流实例已完成执行（无论成功或失败）
// 更新最终状态、完成时间戳、输出结果和错误信息，清除中间的customStatus字段
// 参数instanceID：目标工作流实例的唯一标识符
// 参数status：最终终止状态（completed/failed/canceled）
// 参数output：工作流最终输出结构体（包含各步骤的详细结果或空对象）
// 参数errMsg：错误信息字符串（仅在失败时有值，成功时应传空字符串）
// 参数completedAt：实例完成的时间戳（记录实际结束时刻）
func updateInstanceCompleted(instanceID string, status string, output OrderWorkflowOutput, errMsg string, completedAt time.Time) {
	// 从全局并发安全映射表中加载指定实例的当前数据
	if obj, ok := workflowInstances.Load(instanceID); ok { // 查找并类型断言成功
		// 类型断言将通用接口转换为具体的WorkflowInstance结构体
		instance := obj.(WorkflowInstance) // 接口到具体类型的转换
		// 更新实例的最终终止状态字段
		instance.Status = status // 最终状态：completed或failed
		// 清除中间过程的细粒度状态（完成后不再需要步骤信息）
		instance.CustomStatus = "" // 重置为空字符串
		// 设置完成时间戳指针指向传入的实际完成时刻
		instance.CompletedAt = &completedAt // 指针赋值（非nil表示已终止）
		// 保存工作流的完整输出结果供后续查询和分析
		instance.Output = output // 输出数据对象
		// 保存错误信息字符串（成功时为空字符串表示无错误）
		instance.Error = errMsg // 错误详情或空字符串
		// 将更新后的完成实例对象重新存回映射表覆盖旧数据（并发安全）
		workflowInstances.Store(instanceID, instance) // 更新存储
	}
	// 实例不存在时静默忽略（可能已被清理或从未注册）
}

// stopWorkflowEngineSystem 优雅停止工作流引擎系统的完整关闭流程
// 在应用接收到终止信号时调用此函数确保所有运行中的工作流实例安全退出并释放资源
// 实现优雅关闭机制：统计活跃实例→等待缓冲→取消后台协程→清理资源
// 返回值：关闭过程中的错误信息（通常为nil表示成功完成）
func stopWorkflowEngineSystem() error {

	// 定义计数器变量用于统计当前仍在运行中的工作流实例数量
	runningCount := 0 // 初始值为0
	// 遍历全局并发安全映射表中的所有工作流实例进行状态统计
	workflowInstances.Range(func(key, value interface{}) bool { // Range遍历所有键值对
		// 类型断言将通用接口转换为具体的WorkflowInstance结构体
		instance := value.(WorkflowInstance) // 接口到具体类型的转换
		// 条件判断：仅统计处于running状态的活跃实例（忽略已完成或失败的实例）
		if instance.Status == "running" { // 运行中状态匹配
			runningCount++ // 递增活跃实例计数器
		}
		return true // 返回true继续遍历下一个实例（false则提前终止遍历）
	})

	// 条件判断：如果存在正在运行的活跃工作流实例则进行优雅等待
	if runningCount > 0 { // 存在至少一个活跃实例的情况
		// 等待5秒给运行中的工作流实例足够的缓冲时间完成当前步骤或达到安全中断点
		time.Sleep(5 * time.Second) // 优雅关闭缓冲时间
	}

	// 调用底层引擎关闭函数发送取消信号给所有后台协程触发资源释放流程
	stopDaprWorkflowEngine() // 执行实际的协程取消操作
	return nil               // 关闭流程完成返回无错误
}

// stopDaprWorkflowEngine 停止Dapr工作流引擎的后台工作者协程
// 通过取消上下文通知所有关联的后台协程安全退出，不强制杀死进程
func stopDaprWorkflowEngine() {
	// 检查引擎工作者管理器实例是否已初始化（防止空指针异常）
	if engine != nil { // 引擎非空判断
		// 调用取消函数发送退出信号给所有使用engine.ctx的goroutine触发优雅退出
		engine.cancel() // context.CancelFunc调用后所有监听ctx.Done()的协程将收到信号
	}
}
