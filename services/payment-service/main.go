// Dapr多运行时云原生应用实践 - 支付服务模块
// 功能：处理订单支付、管理支付记录、订阅订单创建事件
package main

import (
	"context"       // 上下文管理，用于控制请求生命周期和超时
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
	"github.com/google/uuid"                       // UUID生成库，用于生成唯一标识符
)

// Payment 支付记录结构体，存储单笔支付的完整信息，支持JSON序列化持久化
type Payment struct {
	PaymentID     string  `json:"paymentId"`     // 支付唯一标识符，UUID格式全局唯一
	OrderID       string  `json:"orderId"`       // 关联订单的唯一标识符，外键关联
	Amount        float64 `json:"amount"`        // 支付金额，单位为元（CNY），保留两位小数
	Status        string  `json:"status"`        // 支付状态：pending/completed/failed/refunded
	PaymentMethod string  `json:"paymentMethod"` // 支付方式：credit_card/alipay/wechat_pay等
	CreatedAt     string  `json:"createdAt"`     // 支付创建时间戳，RFC3339格式
}

const (
	serviceName    = "payment-service" // 服务名称常量，用于日志输出和健康检查响应
	stateStoreName = "statestore"      // Dapr状态存储组件名称，对应statestore.yaml配置
	pubsubName     = "pubsub"          // Dapr发布订阅组件名称，对应pubsub.yaml配置
	appPort        = ":5003"           // 应用服务监听端口号，Dapr Sidecar通过此端口转发请求
)

var daprClient client.Client // Dapr客户端实例全局变量，提供状态管理和服务调用能力

// main 应用程序主入口函数，负责初始化数据库连接、Dapr客户端、注册处理器并启动HTTP服务
func main() {
	// 初始化PostgreSQL数据库连接，建立与订单数据库的持久化通道
	db.InitDB()
	// 注册延迟关闭函数，确保应用退出时正确释放数据库连接资源
	defer db.CloseDB()

	var err error

	// 从环境变量读取Dapr Sidecar的gRPC通信端口（payment-service使用3503）
	grpcPort := os.Getenv("DAPR_GRPC_PORT")
	if grpcPort == "" {
		// 环境变量未设置时使用默认端口3503
		grpcPort = "3503"
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
			fmt.Printf("[SUCCESS] Connected to Darp gRPC on port %s\n", grpcPort)
			break
		}
		// 当前连接失败且未达到最大重试次数限制时休眠后继续
		if i < maxRetries-1 {
			time.Sleep(retryDelay)
		}
	}
	// 所有重试均失败时直接退出程序，无法提供支付服务
	if err != nil {
		return
	}
	// 注册延迟关闭函数，确保退出前释放Dapr客户端连接资源
	defer daprClient.Close()

	// 创建Dapr HTTP服务实例，监听在5003端口接收来自Sidecar的请求转发
	s := daprhttp.NewService(appPort)

	// 注册支付处理接口处理器，路径为/payment/process，供订单服务工作流调用执行扣款操作
	s.AddServiceInvocationHandler("/payment/process", handleProcessPayment)
	// 注册健康检查接口处理器，路径为/health，用于监控服务和状态探测
	s.AddServiceInvocationHandler("/health", handleHealth)

	// 定义发布订阅配置对象，订阅订单服务的order.created事件主题
	subscription := &common.Subscription{
		PubsubName: pubsubName,              // 使用pubsub组件名称（Redis发布订阅）
		Topic:      "order.created",         // 订阅的主题名称：订单创建事件
		Route:      "/events/order/created", // 事件到达时的路由处理路径
	}

	// 将事件订阅绑定到具体的处理函数，实现异步事件驱动架构
	s.AddTopicEventHandler(subscription, handleOrderCreatedEvent)

	// 创建操作系统信号通道，用于捕获终止信号实现优雅关闭
	sigChan := make(chan os.Signal, 1)
	// 监听SIGTERM和SIGINT两种系统信号
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	// 启动后台协程异步监听终止信号，不阻塞主线程的HTTP服务运行
	go func() {
		// 阻塞等待接收到操作系统终止信号
		<-sigChan

		// 向Dapr Sidecar发送shutdown请求触发优雅关闭流程
		http.Post("http://localhost:3503/v1.0/shutdown", "application/json", nil)
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

// handleHealth 健康检查接口处理器，返回支付服务运行状态、功能特性和订阅信息
// 参数ctx：请求上下文，包含超时控制和取消信号
// 参数in：Dapr调用事件对象，包含请求数据和元信息
// 返回值：响应内容对象和可能的错误信息
func handleHealth(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	// 构造健康检查响应数据结构
	health := map[string]interface{}{
		"service":   serviceName,                     // 服务名称标识：payment-service
		"status":    "healthy",                       // 当前健康状态：healthy表示正常运行
		"timestamp": time.Now().Format(time.RFC3339), // 响应生成时间戳
		"features": []string{ // 已启用功能特性列表
			"dapr-sidecar",       // Dapr Sidecar代理模式支持
			"service-invocation", // 服务间调用能力（被订单服务调用）
			"state-management",   // 状态管理能力（支付记录持久化）
			"pubsub-subscriber",  // 发布订阅消费者能力（接收订单事件）
			"graceful-shutdown",  // 优雅关闭机制支持
		},
		"subscriptions": []string{ // 当前已订阅的事件主题列表
			"order.created", // 订阅订单创建事件主题
		},
	}
	// 序列化健康检查数据为JSON格式
	data, _ := json.Marshal(health)
	// 构造并返回标准Dapr响应对象
	return &common.Content{
		Data:        data,               // JSON格式的健康检查数据
		ContentType: "application/json", // 声明MIME类型为JSON
	}, nil
}

// handleOrderCreatedEvent 订单创建事件处理器（发布订阅消费者），实现异步事件驱动架构
// 当订单服务发布order.created事件时自动触发此函数执行预创建支付记录操作
// 参数ctx：事件处理上下文，用于状态管理操作的传播
// 参数e：Dapr主题事件对象，包含事件数据和元信息
// 返回值：retry为是否需要重试的标志位，err为处理过程中的错误信息
func handleOrderCreatedEvent(ctx context.Context, e *common.TopicEvent) (retry bool, err error) {
	// 从事件对象中提取原始字节数据，进行类型断言确保是[]byte类型
	data, ok := e.Data.([]byte)
	if !ok {
		// 数据类型不匹配时返回false表示不需要重试，错误为nil
		return false, nil
	}

	// 定义订单事件的内部结构体用于反序列化
	var orderEvent struct {
		OrderID     string  `json:"orderId"`     // 订单唯一标识符
		UserID      string  `json:"userId"`      // 下单用户标识符
		TotalAmount float64 `json:"totalAmount"` // 订单总金额
		Status      string  `json:"status"`      // 订单当前状态
	}

	// 将事件数据反序列化为订单事件结构体
	if err := json.Unmarshal(data, &orderEvent); err != nil {
		// JSON解析失败时返回false不重试，避免无效数据反复处理
		return false, err
	}

	// 构造预创建支付记录的数据结构，用于在正式支付前预留记录
	paymentRecord := map[string]interface{}{
		"orderId":    orderEvent.OrderID,                            // 关联的订单ID
		"amount":     orderEvent.TotalAmount,                        // 预留支付金额
		"status":     "pending_preparation",                         // 状态标记为待准备
		"preparedAt": time.Now().Format(time.RFC3339),               // 预创建时间戳
		"message":    "Payment record pre-created from order event", // 备注说明信息
	}

	// 序列化支付记录为JSON格式
	paymentData, _ := json.Marshal(paymentRecord)
	// 通过Dapr客户端将预创建支付记录保存到Redis状态存储中，键名使用"payment-prep-"前缀
	if err := daprClient.SaveState(ctx, stateStoreName, "payment-prep-"+orderEvent.OrderID, paymentData, nil); err != nil {
		// 状态保存失败时返回true触发重试机制，确保数据最终一致性
		return true, err
	}
	// 处理成功返回false表示无需重试
	return false, nil
}

// handleProcessPayment 支付处理接口处理器，执行订单支付扣款操作并持久化支付记录
// 这是支付服务的核心业务接口，被订单服务工作流同步调用完成实际支付流程
// 参数ctx：请求上下文，用于数据库事务控制和超时管理
// 参数in：Dapr调用事件对象，包含支付请求的JSON数据（订单ID、金额等）
// 返回值：包含完整支付记录的响应对象或错误信息
func handleProcessPayment(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	// 定义通用map类型变量接收灵活的JSON请求数据
	var req map[string]interface{}
	// 反序列化输入的JSON请求数据到map结构
	if err := json.Unmarshal(in.Data, &req); err != nil {
		// JSON解析失败时直接返回错误信息给调用方
		return nil, err
	}

	// 构造完整的支付记录对象，填充各字段信息
	payment := Payment{
		PaymentID:     uuid.New().String(),             // 生成全局唯一的支付ID（UUID格式）
		OrderID:       req["orderId"].(string),         // 从请求中提取关联订单ID
		Amount:        req["amount"].(float64),         // 从请求中提取支付金额
		Status:        "completed",                     // 设置支付状态为已完成（模拟支付成功）
		PaymentMethod: "credit_card",                   // 默认使用信用卡支付方式
		CreatedAt:     time.Now().Format(time.RFC3339), // 记录当前时间作为创建时间
	}

	// 检查数据库连接是否可用，实现双写策略：同时写入PostgreSQL和Dapr状态存储
	if db.DB != nil {
		// 执行SQL插入语句将支付记录持久化到PostgreSQL数据库的payments表
		db.DB.ExecContext(ctx,
			`INSERT INTO payments (payment_id, order_id, amount, status, payment_method, created_at)
			 VALUES ($1, $2, $3, $4, $5, $6)`, // 使用PostgreSQL参数化查询防止SQL注入
			payment.PaymentID,     // 支付唯一标识符
			payment.OrderID,       // 关联订单标识符
			payment.Amount,        // 支付金额
			payment.Status,        // 支付状态
			payment.PaymentMethod, // 支付方式
			payment.CreatedAt,     // 创建时间戳
		)
	}

	// 将支付记录序列化为JSON格式并保存到Dapr Redis状态存储中，键名使用"payment-"前缀
	paymentData, _ := json.Marshal(payment)
	daprClient.SaveState(ctx, stateStoreName, "payment-"+payment.PaymentID, paymentData, nil)

	// 序列化完整的支付记录作为响应数据返回给调用方
	data, _ := json.Marshal(payment)
	return &common.Content{
		Data:        data,               // 完整的支付记录JSON数据
		ContentType: "application/json", // 声明MIME类型为JSON格式
	}, nil
}
