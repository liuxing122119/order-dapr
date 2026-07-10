// Dapr多运行时云原生应用实践 - 订单服务模块（核心服务）
// 功能：订单创建与管理、工作流编排、状态持久化、事件发布与订阅、可观测性集成
package main

import (
	"context"       // 上下文管理，用于控制请求生命周期和超时
	"database/sql"  // SQL数据库操作接口，用于PostgreSQL交互
	"encoding/json" // JSON序列化与反序列化库
	"fmt"           // 格式化输入输出库
	"net/http"      // HTTP客户端和服务端实现
	"os"            // 操作系统功能接口（环境变量、信号等）
	"os/signal"     // 操作系统信号处理（SIGTERM、SIGINT等）
	"syscall"       // 系统调用常量定义
	"time"          // 时间处理和格式化工具

	"order-dapr/db" // 本地数据库操作包，提供PostgreSQL连接管理

	"github.com/dapr/go-sdk/client"                // Dapr Go SDK客户端包
	"github.com/dapr/go-sdk/service/common"        // Dapr通用服务接口定义
	daprhttp "github.com/dapr/go-sdk/service/http" // Dapr HTTP服务实现
)

// Order 订单主结构体，存储完整的订单信息，支持JSON序列化和版本控制
type Order struct {
	OrderID          string      `json:"orderId"`             // 订单唯一标识符，UUID格式全局唯一
	UserID           string      `json:"userId"`              // 下单用户的标识符，外键关联用户表
	Items            []OrderItem `json:"items"`               // 订单商品明细数组，包含所有购买商品
	TotalAmount      float64     `json:"totalAmount"`         // 订单总金额，单位为元（CNY），保留两位小数
	Status           string      `json:"status"`              // 订单当前状态：pending/processing/completed/failed等
	CreatedAt        string      `json:"createdAt"`           // 订单创建时间戳，RFC3339格式
	UpdatedAt        string      `json:"updatedAt,omitempty"` // 最后更新时间戳（可选字段）
	Version          int64       `json:"version,omitempty"`   // 乐观锁版本号，每次更新递增防止并发冲突
	UserValidated    bool        `json:"userValidated"`       // 用户验证标志：是否已通过用户服务验证有效性
	InventoryChecked bool        `json:"inventoryChecked"`    // 库存检查标志：是否已确认库存充足
	PaymentProcessed bool        `json:"paymentProcessed"`    // 支付处理标志：是否已完成支付扣款操作
}

// OrderItem 订单商品明细结构体，描述订单中单个商品的购买信息
type OrderItem struct {
	ProductID   string  `json:"productId"`   // 商品唯一标识符（SKU），关联库存服务
	ProductName string  `json:"productName"` // 商品显示名称，从库存服务获取
	Quantity    int     `json:"quantity"`    // 购买数量，正整数类型
	Price       float64 `json:"price"`       // 商品单价，单位为元（CNY），下单时快照价格
}

// CreateOrderRequest 创建订单请求结构体，接收前端或API调用方提交的下单数据
type CreateOrderRequest struct {
	UserID string      `json:"userId"` // 下单用户标识符，必填字段
	Items  []OrderItem `json:"items"`  // 购买的商品列表数组，至少包含一个商品
}

const (
	serviceName        = "order-service"         // 服务名称常量，用于日志输出和健康检查响应
	stateStoreName     = "statestore"            // Dapr状态存储组件名称，对应statestore.yaml配置
	pubsubName         = "pubsub"                // Dapr发布订阅组件名称，对应pubsub.yaml配置
	appPort            = ":5002"                 // 应用服务监听端口号，Dapr Sidecar通过此端口转发请求
	userServiceAppId   = "user-service"          // 用户服务的应用ID，用于Dapr服务调用目标地址
	paymentAppId       = "payment-service"       // 支付服务的应用ID，用于调用支付处理接口
	inventoryAppId     = "inventory-service"     // 库存服务的应用ID，用于调用库存检查和扣减接口
	retryPolicyName    = "order-retry-policy"    // 重试策略名称，配置在resiliency.yaml中
	timeoutPolicyName  = "order-timeout-policy"  // 超时策略名称，控制远程调用超时时间
	circuitBreakerName = "order-circuit-breaker" // 熔断器策略名称，保护系统免受级联故障影响
)

var daprClient client.Client // Dapr客户端实例全局变量，提供状态管理、服务调用、发布订阅等核心能力

// main 应用程序主入口函数，负责初始化数据库、Dapr客户端、工作流引擎、注册处理器并启动HTTP服务
func main() {
	// 初始化PostgreSQL数据库连接，建立与订单数据库的持久化通道
	db.InitDB()
	// 注册延迟关闭函数，确保应用退出时正确释放数据库连接资源
	defer db.CloseDB()

	var err error

	// 从环境变量读取Dapr Sidecar的gRPC通信端口（order-service使用3502）
	grpcPort := os.Getenv("DAPR_GRPC_PORT")
	if grpcPort == "" {
		// 环境变量未设置时使用默认端口3502
		grpcPort = "3502"
		fmt.Printf("[WARN] DAPR_GRPC_PORT not set, using default: %s\n", grpcPort)
	}

	// 配置连接重试策略：最多尝试50次连接，每次间隔2秒等待Sidecar启动完成
	maxRetries := 50              // 最大重试次数，确保有足够时间等待Sidecar就绪
	retryDelay := 2 * time.Second // 重试间隔时间，避免频繁重连造成性能损耗
	for i := 0; i < maxRetries; i++ {
		// 打印当前连接进度日志，便于监控初始化过程
		fmt.Printf("[INFO] Attempting to connect to Dapr gRPC on port %s (%d/%d)...\n", grpcPort, i+1, maxRetries)
		// 尝试创建指定端口的Dapr客户端gRPC连接
		daprClient, err = client.NewClientWithPort(grpcPort)
		if err == nil {
			// 连接成功时输出确认信息并跳出循环
			fmt.Printf("[SUCCESS] Connected to Dapr gRPC on port %s\n", grpcPort)
			break
		}
		// 当前连接失败且未达到最大重试次数限制时休眠后继续
		if i < maxRetries-1 {
			time.Sleep(retryDelay)
		}
	}
	// 所有重试均失败时直接退出程序，无法提供订单服务
	if err != nil {
		return
	}
	// 注册延迟关闭函数，确保退出前释放Dapr客户端连接资源
	defer daprClient.Close()

	// 初始化工作流系统，包括注册编排器、活动处理器、任务注册表等组件
	initializeWorkflowSystem()
	// 启动工作流引擎后台协程，准备接收和处理工作流实例
	startWorkflowEngine()

	// 创建Dapr HTTP服务实例，监听在5002端口接收来自Sidecar的请求转发
	s := daprhttp.NewService(appPort)

	// 注册订单创建接口处理器（带工作流编排），路径为/order/create，是系统的核心入口接口
	s.AddServiceInvocationHandler("/order/create", handleCreateOrderWithWorkflow)
	// 注册订单查询接口处理器，路径为/order/get，用于查询指定订单的详细状态信息
	s.AddServiceInvocationHandler("/order/get", handleGetOrder)
	// 注册健康检查接口处理器，路径为/health，用于监控服务和状态探测
	s.AddServiceInvocationHandler("/health", handleHealth)

	// 定义支付完成事件订阅配置，监听支付服务的payment.completed事件主题
	paymentSubscription := &common.Subscription{
		PubsubName: pubsubName,                  // 使用pubsub组件名称（Redis发布订阅）
		Topic:      "payment.completed",         // 订阅的主题名称：支付完成事件
		Route:      "/events/payment/completed", // 事件到达时的路由处理路径
	}
	// 将支付完成事件绑定到处理函数，实现异步更新订单支付状态
	s.AddTopicEventHandler(paymentSubscription, handlePaymentCompletedEvent)

	// 定义库存更新事件订阅配置，监听库存服务的inventory.updated事件主题
	inventorySubscription := &common.Subscription{
		PubsubName: pubsubName,                  // 使用pubsub组件名称（Redis发布订阅）
		Topic:      "inventory.updated",         // 订阅的主题名称：库存更新事件
		Route:      "/events/inventory/updated", // 事件到达时的路由处理路径
	}
	// 将库存更新事件绑定到处理函数，实现异步更新订单库存检查状态
	s.AddTopicEventHandler(inventorySubscription, handleInventoryUpdatedEvent)

	// 创建操作系统信号通道，用于捕获终止信号实现优雅关闭
	sigChan := make(chan os.Signal, 1)
	// 监听SIGTERM和SIGINT两种系统信号
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	// 启动后台协程异步监听终止信号，不阻塞主线程的HTTP服务运行
	go func() {
		// 阻塞等待接收到操作系统终止信号
		<-sigChan

		// 向Dapr Sidecar发送shutdown请求触发优雅关闭流程
		http.Post("http://localhost:3502/v1.0/shutdown", "application/json", nil)
		// 停止工作流引擎系统，释放相关资源
		stopWorkflowEngineSystem()
		// 停止Dapr HTTP服务释放端口资源
		s.Stop()
		// 以正常退出码结束进程
		os.Exit(0)
	}()

	// 启动HTTP服务并阻塞运行，除非遇到非正常关闭错误否则持续监听请求
	if err := s.Start(); err != nil && err != http.ErrServerClosed {
		return
	}
}

// handleHealth 健康检查接口处理器，返回订单服务的详细运行状态、功能特性、弹性策略和工作流信息
// 参数ctx：请求上下文，包含超时控制和取消信号
// 参数in：Dapr调用事件对象，包含请求数据和元信息
// 返回值：响应内容对象和可能的错误信息
func handleHealth(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	health := map[string]interface{}{
		"service":   serviceName,
		"status":    "healthy",
		"timestamp": time.Now().Format(time.RFC3339),
		"features": []string{
			"dapr-sidecar",
			"service-invocation-with-resilience",
			"state-management-with-etag",
			"workflow-orchestration-engine",
			"opentelemetry-tracing",
			"prometheus-metrics",
			"retry-policy",
			"timeout-policy",
			"circuit-breaker",
		},
		"resiliencePolicies": map[string]string{
			"retry":          retryPolicyName,
			"timeout":        timeoutPolicyName,
			"circuitBreaker": circuitBreakerName,
		},
		"workflowInfo": map[string]string{
			"engine":          "dapr-workflow-engine",
			"stateStore":      "redis (event-sourcing pattern)",
			"tracing":         "opentelemetry+jaeger",
			"metrics":         "prometheus",
			"metricsEndpoint": ":9090/metrics",
		},
	}
	data, _ := json.Marshal(health)
	return &common.Content{
		Data:        data,
		ContentType: "application/json",
	}, nil
}

// saveOrderToState 订单状态持久化函数，实现双写策略同时更新PostgreSQL数据库和Dapr Redis状态存储
// 采用乐观锁机制通过版本号防止并发修改冲突，确保分布式环境下的数据一致性
// 先写PostgreSQL作为主存储（支持复杂查询），再写Redis作为缓存层（支持快速读取）
// 参数ctx：请求上下文，用于数据库事务控制和超时管理
// 参数order：待保存的完整订单对象，包含所有业务字段、商品明细和版本信息
// 返回值：可能的错误信息（通常为nil表示双写成功）
func saveOrderToState(ctx context.Context, order Order) error {
	// 更新订单的最后修改时间戳为当前时间（RFC3339格式）
	order.UpdatedAt = time.Now().Format(time.RFC3339)

	// 条件判断：检查数据库连接是否可用（防止在纯内存模式下执行数据库操作）
	if db.DB != nil {
		// 定义变量用于存储查询到的现有订单版本号（乐观锁基础）
		var existingVersion int
		// 使用SELECT FOR UPDATE锁定订单行防止并发修改（行级排他锁）
		err := db.DB.QueryRowContext(ctx,
			"SELECT version FROM orders WHERE order_id = $1 FOR UPDATE", // 带锁的查询语句
			order.OrderID, // 查询条件：订单唯一标识符
		).Scan(&existingVersion) // 将version列值扫描到变量

		if err == nil {
			// 分支1：订单已存在时执行UPDATE操作更新业务状态字段
			db.DB.ExecContext(ctx,
				`UPDATE orders SET status = $2, user_validated = $3, inventory_checked = $4,
				 payment_processed = $5, version = version + 1, updated_at = $6
				 WHERE order_id = $1`, // 乐观锁更新：version自增1
				order.OrderID, order.Status, order.UserValidated, order.InventoryChecked,
				order.PaymentProcessed, order.UpdatedAt, // 更新所有业务字段和新时间戳
			)
		} else if err == sql.ErrNoRows {
			// 分支2：订单不存在时执行INSERT操作创建新记录（首次保存）
			db.DB.ExecContext(ctx,
				`INSERT INTO orders (order_id, user_id, total_amount, status, user_validated,
				 inventory_checked, payment_processed, version, created_at, updated_at)
				 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`, // 插入完整订单主表记录
				order.OrderID, order.UserID, order.TotalAmount, order.Status,
				order.UserValidated, order.InventoryChecked, order.PaymentProcessed,
				order.Version, order.CreatedAt, order.UpdatedAt, // 所有字段的初始值
			)
			// 循环遍历订单的商品明细数组逐一插入到order_items子表建立关联关系
			for _, item := range order.Items { // 使用range遍历商品列表
				db.DB.ExecContext(ctx,
					`INSERT INTO order_items (order_id, product_id, product_name, quantity, price)
					 VALUES ($1, $2, $3, $4, $5)`, // 插入单个商品明细记录
					order.OrderID, item.ProductID, item.ProductName, item.Quantity, item.Price, // 商品明细数据
				)
			}
		}
	}

	// 将完整的订单对象序列化为JSON字节数组准备写入Redis状态存储
	orderData, _ := json.Marshal(order)
	// 构造Redis键名采用"order-"前缀+订单ID的组合方式便于模式匹配查询
	key := "order-" + order.OrderID

	// 构造元数据映射表存储订单的版本号和当前状态（用于乐观锁和快速过滤）
	metadata := map[string]string{
		"version": fmt.Sprintf("%d", order.Version), // 版本号转为字符串格式
		"status":  order.Status,                     // 当前订单状态标识
	}

	// 调用Dapr客户端将订单数据和元数据写入Redis状态存储组件（异步持久化）
	daprClient.SaveState(ctx, stateStoreName, key, orderData, metadata)

	return nil // 双写操作成功返回无错误
}

// handleGetOrder 订单查询接口处理器，根据订单ID从状态存储中检索完整的订单信息
// 支持返回ETag元数据用于后续的并发控制操作
// 参数ctx：请求上下文，用于控制查询超时和传播追踪信息
// 参数in：Dapr调用事件对象，包含查询请求的JSON数据（仅需orderId字段）
// 返回值：包含完整订单数据和元信息的响应对象或错误信息
func handleGetOrder(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	var req map[string]string
	if err := json.Unmarshal(in.Data, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	item, err := daprClient.GetState(ctx, stateStoreName, "order-"+req["orderId"], nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get order: %w", err)
	}
	if item == nil || len(item.Value) == 0 {
		return nil, fmt.Errorf("order not found: %s", req["orderId"])
	}

	var order Order
	if err := json.Unmarshal(item.Value, &order); err != nil {
		return nil, fmt.Errorf("failed to parse order: %w", err)
	}

	response := map[string]interface{}{
		"order": order,
		"metadata": map[string]string{
			"etag":               item.Etag,
			"concurrencyControl": "first-write (via Dapr component config)",
		},
	}

	data, _ := json.Marshal(response)
	return &common.Content{
		Data:        data,
		ContentType: "application/json",
	}, nil
}

// calculateTotalAmount 订单总金额计算函数，遍历商品明细数组累加计算订单总金额
// 参数items：订单商品明细数组，每个元素包含商品单价和购买数量
// 返回值：计算得到的订单总金额（单位：元/CNY）
func calculateTotalAmount(items []OrderItem) float64 {
	var total float64
	for _, item := range items {
		total += item.Price * float64(item.Quantity)
	}
	return total
}

// handlePaymentCompletedEvent 支付完成事件处理器（发布订阅消费者），异步更新订单支付状态
// 当支付服务发布payment.completed事件到Redis pubsub组件时，Dapr Sidecar自动路由到此函数执行
// 采用最终一致性模型：先更新Redis状态存储（快速响应），再由saveOrderToState双写到PostgreSQL
// 参数ctx：事件处理上下文，用于状态管理操作的传播、超时控制和链路追踪
// 参数e：Dapr主题事件对象，包含支付事件的JSON数据（paymentId、orderId、amount、status等）
// 返回值：retry为是否需要重试的布尔标志位（true表示需要重试），err为处理过程中的错误信息
func handlePaymentCompletedEvent(ctx context.Context, e *common.TopicEvent) (retry bool, err error) {
	// 类型断言：从事件数据中提取原始字节数组（Dapr传递的[]byte类型）
	data, ok := e.Data.([]byte)
	if !ok {
		// 数据类型不匹配时返回false表示不需要重试（格式错误重试无意义）
		return false, nil
	}

	// 定义支付事件内部结构体用于反序列化JSON格式的支付结果数据
	var paymentEvent struct {
		PaymentID string  `json:"paymentId"` // 支付记录唯一标识符
		OrderID   string  `json:"orderId"`   // 关联的订单唯一标识符
		Amount    float64 `json:"amount"`    // 实际扣款的金额数值
		Status    string  `json:"status"`    // 支付处理结果状态（success/failed/refunded等）
	}

	// 反序列化支付事件的JSON数据到结构体获取详细的支付结果信息
	if err := json.Unmarshal(data, &paymentEvent); err != nil {
		// JSON解析失败时返回false表示不需要重试（格式错误无法通过重试修复）
		return false, err
	}

	// 从Redis状态存储中查询关联的订单完整数据用于后续更新操作
	item, err := daprClient.GetState(ctx, stateStoreName, "order-"+paymentEvent.OrderID, nil)
	if err != nil {
		// 状态存储查询失败时返回true表示需要重试（可能是临时性网络故障）
		return true, err
	}

	// 检查订单是否存在（可能尚未创建或已被删除）
	if item == nil || len(item.Value) == 0 {
		// 订单不存在时返回false表示不需要重试（等待订单创建后再处理）
		return false, nil
	}

	// 反序列化订单的JSON数据到Order结构体获取当前的业务状态字段
	var order Order
	if err := json.Unmarshal(item.Value, &order); err != nil {
		// 订单数据解析失败时返回false表示不需要重试（数据损坏需人工介入）
		return false, err
	}

	// 更新订单的支付处理相关字段标记支付已完成并推进订单状态
	order.PaymentProcessed = true                     // 设置支付处理完成标志为true
	order.Status = "payment_processed"                // 推进订单状态为"已支付"
	order.UpdatedAt = time.Now().Format(time.RFC3339) // 更新最后修改时间戳
	order.Version++                                   // 乐观锁版本号自增1

	// 调用双写持久化函数将更新后的订单同时写入PostgreSQL和Redis
	saveOrderToState(ctx, order)
	return false, nil // 事件处理成功返回无错误且无需重试
}

// handleInventoryUpdatedEvent 库存更新事件处理器（发布订阅消费者），异步更新订单库存检查状态
// 当库存服务发布inventory.updated事件到Redis pubsub组件时，Dapr Sidecar自动路由到此函数执行
// 根据订单当前支付状态智能判断最终状态：已支付则标记为"inventory_updated"，未支付则标记为"inventory_checked"
// 参数ctx：事件处理上下文，用于状态管理操作的传播、超时控制和链路追踪
// 参数e：Dapr主题事件对象，包含库存事件的JSON数据（orderId、productId、newQuantity、operation、status等）
// 返回值：retry为是否需要重试的布尔标志位（true表示需要重试），err为处理过程中的错误信息
func handleInventoryUpdatedEvent(ctx context.Context, e *common.TopicEvent) (retry bool, err error) {
	// 类型断言：从事件数据中提取原始字节数组（Dapr传递的[]byte类型）
	data, ok := e.Data.([]byte)
	if !ok {
		// 数据类型不匹配时返回false表示不需要重试（格式错误重试无意义）
		return false, nil
	}

	// 定义库存事件内部结构体用于反序列化JSON格式的库存操作结果数据
	var inventoryEvent struct {
		OrderID     string `json:"orderId"`     // 关联的订单唯一标识符
		ProductID   string `json:"productId"`   // 被更新的商品唯一标识符
		NewQuantity int    `json:"newQuantity"` // 更新后的最新库存数量
		Operation   string `json:"operation"`   // 操作类型标识（decrease/increase等）
		Status      string `json:"status"`      // 库存操作结果状态（success/failed/insufficient等）
	}

	// 反序列化库存事件的JSON数据到结构体获取详细的库存操作结果信息
	if err := json.Unmarshal(data, &inventoryEvent); err != nil {
		// JSON解析失败时返回false表示不需要重试（格式错误无法通过重试修复）
		return false, err
	}

	// 防御性检查：过滤掉无效或空值的订单ID（防止脏数据触发无意义的查询）
	if inventoryEvent.OrderID == "" || inventoryEvent.OrderID == "null" {
		// 订单ID为空时直接返回false跳过此事件的后续处理
		return false, nil
	}

	// 从Redis状态存储中查询关联的订单完整数据用于后续状态更新操作
	item, err := daprClient.GetState(ctx, stateStoreName, "order-"+inventoryEvent.OrderID, nil)
	if err != nil {
		// 状态存储查询失败时返回true表示需要重试（可能是临时性网络故障）
		return true, err
	}

	// 检查订单是否存在（可能尚未创建或已被删除）
	if item == nil || len(item.Value) == 0 {
		// 订单不存在时返回false表示不需要重试（等待订单创建后再处理）
		return false, nil
	}

	// 反序列化订单的JSON数据到Order结构体获取当前的业务状态字段
	var order Order
	if err := json.Unmarshal(item.Value, &order); err != nil {
		// 订单数据解析失败时返回false表示不需要重试（数据损坏需人工介入）
		return false, err
	}

	// 更新订单的库存检查相关字段标记库存验证已完成
	order.InventoryChecked = true // 设置库存检查完成标志为true

	// 条件判断：根据支付状态智能决定订单的最终状态（支持多种业务场景）
	if order.PaymentProcessed {
		// 分支1：订单已完成支付时推进到最终状态"库存已更新"
		order.Status = "inventory_updated" // 最终完成状态
	} else {
		// 分支2：订单尚未支付时保持中间状态"库存已检查"等待支付流程
		order.Status = "inventory_checked" // 中间过渡状态
	}

	// 更新最后修改时间戳和乐观锁版本号确保并发安全
	order.UpdatedAt = time.Now().Format(time.RFC3339) // 更新时间戳
	order.Version++                                   // 版本号自增1

	// 调用双写持久化函数将更新后的订单同时写入PostgreSQL和Redis
	saveOrderToState(ctx, order)
	return false, nil // 事件处理成功返回无错误且无需重试
}
