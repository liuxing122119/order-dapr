// 订单服务主程序包 - 负责订单创建、查询、状态管理和事件处理
// 核心功能：订单生命周期管理、工作流编排、弹性策略（重试/超时/熔断）
package main

import (
	"context"                // 上下文控制库，用于请求生命周期管理
	"database/sql"           // SQL数据库操作标准库
	"encoding/json"          // JSON编解码库
	"fmt"                    // 格式化输出库
	"net/http"               // HTTP客户端和服务端库
	"os"                     // 操作系统功能库
	"os/signal"              // 信号处理库
	"syscall"                // 系统调用库
	"time"                   // 时间处理库

	"order-dapr/db"          // 导入自定义数据库操作包

	"github.com/dapr/go-sdk/client"           // Dapr Go SDK客户端包
	"github.com/dapr/go-sdk/service/common"   // Dapr通用服务接口包
	daprhttp "github.com/dapr/go-sdk/service/http"  // Dapr HTTP服务实现包
)

// Order 订单结构体
// 用于表示完整的订单信息及其状态
type Order struct {
	OrderID          string      `json:"orderId"`          // 订单唯一标识符
	UserID           string      `json:"userId"`           // 下单用户ID
	Items            []OrderItem `json:"items"`            // 订单项列表（OrderItem切片）
	TotalAmount      float64     `json:"totalAmount"`      // 订单总金额（元）
	Status           string      `json:"status"`           // 订单状态（pending/processing/completed等）
	CreatedAt        string      `json:"createdAt"`        // 订单创建时间戳
	UpdatedAt        string      `json:"updatedAt,omitempty"`  // 订单更新时间戳（omitempty表示可选字段）
	Version          int64       `json:"version,omitempty"`    // 乐观锁版本号（用于并发控制）
	UserValidated    bool        `json:"userValidated"`       // 用户验证状态标志
	InventoryChecked bool        `json:"inventoryChecked"`    // 库存检查状态标志
	PaymentProcessed bool        `json:"paymentProcessed"`    // 支付处理状态标志
}

// OrderItem 订单项结构体
// 表示订单中的单个商品条目
type OrderItem struct {
	ProductID   string  `json:"productId"`   // 商品ID
	ProductName string  `json:"productName"` // 商品名称
	Quantity    int     `json:"quantity"`    // 购买数量
	Price       float64 `json:"price"`       // 商品单价
}

// CreateOrderRequest 创建订单请求结构体
// 用于接收创建订单时的请求数据
type CreateOrderRequest struct {
	UserID string      `json:"userId"`  // 用户ID（必填）
	Items  []OrderItem `json:"items"`   // 订单商品列表（必填）
}

// 常量定义区域 - 服务配置和依赖服务标识
const (
	serviceName        = "order-service"         // 当前服务名称
	stateStoreName     = "statestore"             // 状态存储组件名称
	pubsubName         = "pubsub"                 // 发布订阅组件名称
	appPort            = ":5002"                  // HTTP服务监听端口
	userServiceAppId   = "user-service"           // 用户服务应用ID（用于服务调用）
	paymentAppId       = "payment-service"        // 支付服务应用ID
	inventoryAppId     = "inventory-service"      // 库存服务应用ID
	retryPolicyName    = "order-retry-policy"     // 重试策略名称
	timeoutPolicyName  = "order-timeout-policy"   // 超时策略名称
	circuitBreakerName = "order-circuit-breaker"  // 熔断器策略名称
)

// daprClient 全局Dapr客户端对象
var daprClient client.Client

// main 主函数 - 订单服务的入口点
// 功能：初始化数据库、Dapr客户端、工作流系统、注册HTTP接口、启动服务
func main() {
	// 初始化数据库连接
	db.InitDB()
	// defer延迟调用 - 确保程序退出时关闭数据库连接
	defer db.CloseDB()

	var err error // 错误变量声明

	// grpcPort 从环境变量获取Dapr gRPC端口号
	grpcPort := os.Getenv("DAPR_GRPC_PORT")
	if grpcPort == "" {
		// if判断 - 如果环境变量未设置，使用默认值3502
		grpcPort = "3502"
		fmt.Printf("[WARN] DAPR_GRPC_PORT not set, using default: %s\n", grpcPort)
	}

	// maxRetries 最大重试次数常量
	maxRetries := 50              // 设置为50次重试
	// retryDelay 重试间隔时间常量
	retryDelay := 2 * time.Second // 设置为2秒间隔

	// for循环 - 重试建立Dapr gRPC连接
	for i := 0; i < maxRetries; i++ {
		fmt.Printf("[INFO] Attempting to connect to Dapr gRPC on port %s (%d/%d)...\n", grpcPort, i+1, maxRetries)
		// client.NewClientWithPort 创建指定端口的Dapr客户端实例
		daprClient, err = client.NewClientWithPort(grpcPort)
		if err == nil {
			// 连接成功，打印日志并break跳出循环
			fmt.Printf("[SUCCESS] Connected to Dapr gRPC on port %s\n", grpcPort)
			break  // break跳转语句 - 退出for循环
		}
		// if条件判断 - 如果不是最后一次重试，等待后继续
		if i < maxRetries-1 {
			time.Sleep(retryDelay)  // sleep函数 - 暂停执行
		}
	}

	// if判断 - 如果所有重试都失败，退出程序
	if err != nil {
		return  // return语句 - 退出main函数
	}
	// defer注册延迟关闭Dapr客户端
	defer daprClient.Close()

	// initializeWorkflowSystem 初始化工作流系统（包含遥测和引擎初始化）
	initializeWorkflowSystem()
	// startWorkflowEngine 启动工作流引擎
	startWorkflowEngine()

	// daprhttp.NewService 创建HTTP服务实例
	s := daprhttp.NewService(appPort)

	// AddServiceInvocationHandler 注册服务调用处理函数
	// 参数1: "/order/create" - 创建订单API路径
	// 参数2: handleCreateOrderWithWorkflow - 处理函数（带工作流编排）
	s.AddServiceInvocationHandler("/order/create", handleCreateOrderWithWorkflow)

	// 注册获取订单详情的处理函数
	s.AddServiceInvocationHandler("/order/get", handleGetOrder)

	// 注册健康检查处理函数
	s.AddServiceInvocationHandler("/health", handleHealth)

	// paymentSubscription 支付完成事件订阅配置
	paymentSubscription := &common.Subscription{
		PubsubName: pubsubName,              // PubSub组件名
		Topic:      "payment.completed",     // 订阅主题：支付完成事件
		Route:      "/events/payment/completed",  // 路由路径
	}
	// 注册支付完成事件处理器
	s.AddTopicEventHandler(paymentSubscription, handlePaymentCompletedEvent)

	// inventorySubscription 库存更新事件订阅配置
	inventorySubscription := &common.Subscription{
		PubsubName: pubsubName,              // PubSub组件名
		Topic:      "inventory.updated",     // 订阅主题：库存更新事件
		Route:      "/events/inventory/updated",  // 路由路径
	}
	// 注册库存更新事件处理器
	s.AddTopicEventHandler(inventorySubscription, handleInventoryUpdatedEvent)

	// sigChan 创建信号通道（缓冲区大小为1）
	sigChan := make(chan os.Signal, 1)
	// signal.Notify 注册信号监听器
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)  // 监听终止和中断信号

	// go关键字 - 启动goroutine异步执行优雅关闭逻辑
	go func() {
		<-sigChan  // 阻塞等待信号

		// http.Post 发送关闭请求到Dapr sidecar
		http.Post("http://localhost:3502/v1.0/shutdown", "application/json", nil)
		// stopWorkflowEngineSystem 停止工作流引擎系统
		stopWorkflowEngineSystem()
		// s.Stop 停止HTTP服务器
		s.Stop()
		// os.Exit 退出程序（状态码0表示正常）
		os.Exit(0)
	}()

	// s.Start 启动HTTP服务器（阻塞运行直到服务停止）
	if err := s.Start(); err != nil && err != http.ErrServerClosed {
		return  // 非正常关闭错误则退出
	}
}

// handleHealth 健康检查处理函数
// 功能：返回服务的详细健康信息、功能特性和配置信息
func handleHealth(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	// health 健康信息映射表
	health := map[string]interface{}{
		"service":   serviceName,                    // 服务名称
		"status":    "healthy",                      // 服务状态
		"timestamp": time.Now().Format(time.RFC3339),  // 当前时间戳
		"features": []string{                         // 功能特性列表（字符串切片）
			"dapr-sidecar",                          // Dapr sidecar支持
			"service-invocation-with-resilience",    // 带弹性的服务间调用
			"state-management-with-etag",            // 带ETag的状态管理（乐观并发控制）
			"workflow-orchestration-engine",         // 工作流编排引擎
			"opentelemetry-tracing",                 // OpenTelemetry分布式追踪
			"prometheus-metrics",                    // Prometheus指标监控
			"retry-policy",                          // 重试策略支持
			"timeout-policy",                        // 超时策略支持
			"circuit-breaker",                       // 熔断器机制
		},
		"resiliencePolicies": map[string]string{    // 弹性策略配置映射
			"retry":          retryPolicyName,       // 重试策略名称
			"timeout":        timeoutPolicyName,     // 超时策略名称
			"circuitBreaker": circuitBreakerName,    // 熔断器策略名称
		},
		"workflowInfo": map[string]string{          // 工作流系统信息映射
			"engine":          "dapr-workflow-engine",  // 引擎类型
			"stateStore":      "redis (event-sourcing pattern)",  // 状态存储后端
			"tracing":         "opentelemetry+jaeger",  // 追踪方案
			"metrics":         "prometheus",            // 指标方案
			"metricsEndpoint": ":9090/metrics",         // 指标暴露端点
		},
	}
	// json.Marshal 序列化为JSON字节数组
	data, _ := json.Marshal(health)
	// 构建并返回响应内容对象
	return &common.Content{
		Data:        data,
		ContentType: "application/json",
	}, nil
}

// saveOrderToState 保存订单到状态存储函数
// 功能：将订单数据同时保存到PostgreSQL数据库和Dapr状态存储（双写模式）
// 参数：
//   - ctx context.Context - 请求上下文
//   - order Order - 要保存的订单对象
// 返回值：
//   - error - 错误信息（nil表示成功）
func saveOrderToState(ctx context.Context, order Order) error {
	// 更新订单的时间戳为当前时间
	order.UpdatedAt = time.Now().Format(time.RFC3339)

	// if判断 - 如果数据库连接可用，执行数据库持久化操作
	if db.DB != nil {
		var existingVersion int  // existingVersion 已有版本号变量
		// db.DB.QueryRowContext 执行SQL查询并扫描结果
		// FOR UPDATE 行级锁 - 防止并发修改
		err := db.DB.QueryRowContext(ctx,
			"SELECT version FROM orders WHERE order_id = $1 FOR UPDATE",
			order.OrderID,  // 参数1: 订单ID
		).Scan(&existingVersion)  // Scan将查询结果扫描到existingVersion变量

		// if-else if条件判断链 - 判断订单是否已存在
		if err == nil {
			// 分支1: 订单存在，执行UPDATE更新操作
			db.DB.ExecContext(ctx,
				`UPDATE orders SET status = $2, user_validated = $3, inventory_checked = $4,
				 payment_processed = $5, version = version + 1, updated_at = $6
				 WHERE order_id = $1`,
				order.OrderID, order.Status, order.UserValidated, order.InventoryChecked,
				order.PaymentProcessed, order.UpdatedAt,  // 多个参数对应$1-$6占位符
			)
		} else if err == sql.ErrNoRows {
			// 分支2: 订单不存在（sql.ErrNoRows），执行INSERT插入操作
			db.DB.ExecContext(ctx,
				`INSERT INTO orders (order_id, user_id, total_amount, status, user_validated,
				 inventory_checked, payment_processed, version, created_at, updated_at)
				 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
				order.OrderID, order.UserID, order.TotalAmount, order.Status,
				order.UserValidated, order.InventoryChecked, order.PaymentProcessed,
				order.Version, order.CreatedAt, order.UpdatedAt,  // 10个参数
			)

			// for range循环 - 插入订单的所有订单项
			for _, item := range order.Items {  // _忽略索引，item是当前元素
				db.DB.ExecContext(ctx,
					`INSERT INTO order_items (order_id, product_id, product_name, quantity, price)
					 VALUES ($1, $2, $3, $4, $5)`,
					order.OrderID, item.ProductID, item.ProductName, item.Quantity, item.Price,
				)
			}
		}
	}

	// 序列化订单对象为JSON
	orderData, _ := json.Marshal(order)
	// key 状态存储键名 - 格式："order-" + 订单ID
	key := "order-" + order.OrderID

	// metadata 元数据映射 - 包含版本号和状态信息
	metadata := map[string]string{
		"version": fmt.Sprintf("%d", order.Version),  // fmt.Sprintf格式化版本号为字符串
		"status":  order.Status,                       // 订单状态
	}

	// daprClient.SaveState 保存到Dapr状态存储（带元数据）
	daprClient.SaveState(ctx, stateStoreName, key, orderData, metadata)

	return nil  // 返回nil表示成功
}

// handleGetOrder 获取订单详情处理函数
// 功能：根据订单ID从状态存储中查询并返回订单详细信息
func handleGetOrder(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	// req 请求参数映射（orderId -> 字符串）
	var req map[string]string
	// 反序列化请求数据
	if err := json.Unmarshal(in.Data, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)  // 格式错误返回异常
	}

	// daprClient.GetState 从状态存储获取订单数据
	item, err := daprClient.GetState(ctx, stateStoreName, "order-"+req["orderId"], nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get order: %w", err)  // 获取失败返回错误
	}
	// if判断 - 检查订单是否存在
	if item == nil || len(item.Value) == 0 {
		return nil, fmt.Errorf("order not found: %s", req["orderId"])  // len函数获取字节切片长度
	}

	// order 订单对象
	var Order
	// 反序列化订单数据
	if err := json.Unmarshal(item.Value, &order); err != nil {
		return nil, fmt.Errorf("failed to parse order: %w", err)  // 解析失败返回错误
	}

	// response 响应映射表 - 包含订单数据和元数据
	response := map[string]interface{}{
		"order": order,  // 订单完整数据
		"metadata": map[string]string{  // 元数据子映射
			"etag":               item.Etag,  // ETag实体标签（用于乐观并发控制）
			"concurrencyControl": "first-write (via Dapr component config)",  // 并发控制策略说明
		},
	}

	// 序列化并返回响应
	data, _ := json.Marshal(response)
	return &common.Content{
		Data:        data,
		ContentType: "application/json",
	}, nil
}

// calculateTotalAmount 计算订单总金额函数
// 功能：遍历所有订单项，计算总金额（单价 × 数量之和）
// 参数：
//   - items []OrderItem - 订单项列表
// 返回值：
//   - float64 - 总金额
func calculateTotalAmount(items []OrderItem) float64 {
	var total float64  // total 总金额累加变量（初始值为0）

	// for range循环 - 遍历每个订单项进行金额计算
	for _, item := range items {  // _忽略索引，item是当前订单项
		total += item.Price * float64(item.Quantity)  // +=复合赋值运算符 - 累加金额
		// float64类型转换 - 将Quantity从int转为float64进行浮点运算
	}

	return total  // return语句 - 返回计算得到的总金额
}

// handlePaymentCompletedEvent 支付完成事件处理函数
// 功能：当收到支付完成事件时，更新订单的支付状态
func handlePaymentCompletedEvent(ctx context.Context, e *common.TopicEvent) (retry bool, err error) {
	// data 类型断言 - 将事件数据断言为[]byte字节数组
	data, ok := e.Data.([]byte)
	if !ok {
		return false, nil  // 断言失败返回不重试
	}

	// paymentEvent 支付事件结构体（匿名结构体）
	var paymentEvent struct {
		PaymentID string  `json:"paymentId"`  // 支付ID
		OrderID   string  `json:"orderId"`    // 关联的订单ID
		Amount    float64 `json:"amount"`     // 支付金额
		Status    string  `json:"status"`     // 支付状态
	}

	// 反序列化支付事件数据
	if err := json.Unmarshal(data, &paymentEvent); err != nil {
		return false, err  // 解析失败返回错误
	}

	// 从状态存储获取关联的订单数据
	item, err := daprClient.GetState(ctx, stateStoreName, "order-"+paymentEvent.OrderID, nil)
	if err != nil {
		return true, err  // 获取失败返回需要重试
	}

	// if判断 - 检查订单数据是否为空
	if item == nil || len(item.Value) == 0 {
		return false, nil  // 订单不存在返回不重试
	}

	// order 订单对象
	var Order
	// 反序列化订单数据
	if err := json.Unmarshal(item.Value, &order); err != nil {
		return false, err  // 解析失败返回错误
	}

	// 更新订单的支付相关状态字段
	order.PaymentProcessed = true   // 设置支付已处理标志为true
	order.Status = "payment_processed"  // 更新订单状态为"支付已完成"
	order.UpdatedAt = time.Now().Format(time.RFC3339)  // 更新时间戳
	order.Version++  // ++自增运算符 - 版本号递增（乐观锁）

	// 调用saveOrderToState保存更新后的订单
	saveOrderToState(ctx, order)
	return false, nil  // 成功返回不重试
}

// handleInventoryUpdatedEvent 库存更新事件处理函数
// 功能：当收到库存更新事件时，更新订单的库存检查状态
func handleInventoryUpdatedEvent(ctx context.Context, e *common.TopicEvent) (retry bool, err error) {
	// data 类型断言
	data, ok := e.Data.([]byte)
	if !ok {
		return false, nil
	}

	// inventoryEvent 库存事件结构体（匿名结构体）
	var inventoryEvent struct {
		OrderID     string `json:"orderId"`     // 关联的订单ID
		ProductID   string `json:"productId"`   // 商品ID
		NewQuantity int    `json:"newQuantity"` // 新的库存数量
		Operation   string `json:"operation"`   // 操作类型（increase/decrease）
		Status      string `json:"status"`      // 操作状态
	}

	// 反序列化库存事件数据
	if err := json.Unmarshal(data, &inventoryEvent); err != nil {
		return false, err
	}

	// if判断 - 过滤无效的空订单ID
	if inventoryEvent.OrderID == "" || inventoryEvent.OrderID == "null" {
		return false, nil  // 空ID直接返回不处理
	}

	// 从状态存储获取订单数据
	item, err := daprClient.GetState(ctx, stateStoreName, "order-"+inventoryEvent.OrderID, nil)
	if err != nil {
		return true, err  // 获取失败需要重试
	}

	// 检查订单是否存在
	if item == nil || len(item.Value) == 0 {
		return false, nil  // 订单不存在
	}

	// order 订单对象
	var Order
	// 反序列化
	if err := json.Unmarshal(item.Value, &order); err != nil {
		return false, err
	}

	// 更新库存检查状态
	order.InventoryChecked = true  // 设置库存已检查标志

	// if-else条件判断 - 根据支付状态决定最终订单状态
	if order.PaymentProcessed {
		// if分支 - 支付也已完成，状态设为"库存已更新"
		order.Status = "inventory_updated"
	} else {
		// else分支 - 支付未完成，状态设为"库存已检查"
		order.Status = "inventory_checked"
	}

	// 更新时间戳和版本号
	order.UpdatedAt = time.Now().Format(time.RFC3339)
	order.Version++  // 版本号自增

	// 保存更新后的订单
	saveOrderToState(ctx, order)
	return false, nil  // 成功返回
}