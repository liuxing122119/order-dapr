// 支付服务主程序包 - 负责订单支付处理和支付事件管理
// 功能：处理支付请求、保存支付记录、响应订单创建事件
package main

import (
	"context"       // 上下文控制库
	"encoding/json" // JSON编解码库
	"fmt"           // 格式化输出库
	"net/http"      // HTTP客户端和服务端库
	"os"            // 操作系统功能库
	"os/signal"     // 信号处理库
	"syscall"       // 系统调用库
	"time"          // 时间处理库

	"order-dapr/db" // 导入自定义数据库操作包

	"github.com/dapr/go-sdk/client"                // Dapr Go SDK客户端包
	"github.com/dapr/go-sdk/service/common"        // Dapr通用服务接口包
	daprhttp "github.com/dapr/go-sdk/service/http" // Dapr HTTP服务实现包
	"github.com/google/uuid"                       // UUID生成库（用于生成唯一ID）
)

// Payment 支付记录结构体
// 用于表示一次完整的支付交易信息
type Payment struct {
	PaymentID     string  `json:"paymentId"`     // 支付唯一标识符
	OrderID       string  `json:"orderId"`       // 关联的订单ID
	Amount        float64 `json:"amount"`        // 支付金额（元）
	Status        string  `json:"status"`        // 支付状态（pending/completed/failed等）
	PaymentMethod string  `json:"paymentMethod"` // 支付方式（credit_card/alipay/wechat等）
	CreatedAt     string  `json:"createdAt"`     // 创建时间戳
}

// 常量定义区域 - 服务配置参数
const (
	serviceName    = "payment-service" // 服务名称 - 标识当前微服务
	stateStoreName = "statestore"      // 状态存储组件名称
	pubsubName     = "pubsub"          // 发布订阅组件名称
	appPort        = ":5003"           // HTTP服务监听端口
)

// daprClient 全局Dapr客户端对象
var daprClient client.Client

// main 主函数 - 支付服务的入口点
// 功能：初始化数据库连接、Dapr客户端、注册HTTP接口、启动服务
func main() {
	db.InitDB()        // 初始化数据库连接
	defer db.CloseDB() // defer延迟调用 - 程序退出时关闭数据库连接

	var err error // 错误变量声明

	// grpcPort 从环境变量获取Dapr gRPC端口
	grpcPort := os.Getenv("DAPR_GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "3503" // 默认使用3503端口
		fmt.Printf("[WARN] DAPR_GRPC_PORT not set, using default: %s\n", grpcPort)
	}

	maxRetries := 50              // 最大重试次数设为50次
	retryDelay := 2 * time.Second // 重试间隔设为2秒

	// for循环 - 重试建立Dapr gRPC连接
	for i := 0; i < maxRetries; i++ {
		fmt.Printf("[INFO] Attempting to connect to Dapr gRPC on port %s (%d/%d)...\n", grpcPort, i+1, maxRetries)
		daprClient, err = client.NewClientWithPort(grpcPort) // 创建指定端口的Dapr客户端
		if err == nil {
			fmt.Printf("[SUCCESS] Connected to Darp gRPC on port %s\n", grpcPort)
			break // break跳出循环
		}
		if i < maxRetries-1 {
			time.Sleep(retryDelay) // sleep等待后重试
		}
	}

	if err != nil {
		return // 所有重试失败则退出
	}
	defer daprClient.Close() // defer延迟关闭Dapr客户端

	s := daprhttp.NewService(appPort) // 创建HTTP服务实例

	// AddServiceInvocationHandler 注册服务调用处理函数
	s.AddServiceInvocationHandler("/payment/process", handleProcessPayment) // 处理支付请求
	s.AddServiceInvocationHandler("/health", handleHealth)                  // 健康检查

	subscription := &common.Subscription{ // 订阅配置对象
		PubsubName: pubsubName,              // PubSub组件名
		Topic:      "order.created",         // 订阅主题：订单创建事件
		Route:      "/events/order/created", // 路由路径
	}
	s.AddTopicEventHandler(subscription, handleOrderCreatedEvent) // 注册事件处理器

	sigChan := make(chan os.Signal, 1)                      // 创建信号通道
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT) // 监听终止和中断信号

	go func() { // go启动goroutine处理优雅关闭
		<-sigChan                                                                 // 阻塞等待信号
		http.Post("http://localhost:3503/v1.0/shutdown", "application/json", nil) // 通知Dapr sidecar关闭
		s.Stop()                                                                  // 停止HTTP服务
		os.Exit(0)                                                                // 正常退出程序
	}()

	if err := s.Start(); err != nil && err != http.ErrServerClosed {
		return // 启动失败退出
	}
}

// handleHealth 健康检查处理函数
// 功能：返回服务的健康状态、功能特性和订阅信息
func handleHealth(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	health := map[string]interface{}{ // 健康信息映射表
		"service":   serviceName,
		"status":    "healthy",
		"timestamp": time.Now().Format(time.RFC3339),
		"features": []string{ // 功能特性列表（字符串切片）
			"dapr-sidecar",
			"service-invocation",
			"state-management",
			"pubsub-subscriber",
			"graceful-shutdown",
		},
		"subscriptions": []string{ // 订阅的主题列表
			"order.created",
		},
	}
	data, _ := json.Marshal(health)
	return &common.Content{
		Data:        data,
		ContentType: "application/json",
	}, nil
}

// handleOrderCreatedEvent 订单创建事件处理函数
// 功能：当收到订单创建事件时，预创建支付记录（预留状态）
func handleOrderCreatedEvent(ctx context.Context, e *common.TopicEvent) (retry bool, err error) {
	data, ok := e.Data.([]byte) // 类型断言获取事件数据
	if !ok {
		return false, nil
	}

	var orderEvent struct { // 订单事件结构体（匿名结构体）
		OrderID     string  `json:"orderId"`
		UserID      string  `json:"userId"`
		TotalAmount float64 `json:"totalAmount"`
		Status      string  `json:"status"`
	}

	if err := json.Unmarshal(data, &orderEvent); err != nil {
		return false, err
	}

	paymentRecord := map[string]interface{}{ // 预创建的支付记录映射
		"orderId":    orderEvent.OrderID,
		"amount":     orderEvent.TotalAmount,
		"status":     "pending_preparation", // 状态：待准备
		"preparedAt": time.Now().Format(time.RFC3339),
		"message":    "Payment record pre-created from order event",
	}

	paymentData, _ := json.Marshal(paymentRecord)
	if err := daprClient.SaveState(ctx, stateStoreName, "payment-prep-"+orderEvent.OrderID, paymentData, nil); err != nil {
		return true, err // 保存失败返回需要重试
	}

	return false, nil // 成功返回不重试
}

// handleProcessPayment 处理支付请求函数
// 功能：接收支付请求、创建支付记录、保存到数据库和状态存储
func handleProcessPayment(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	var req map[string]interface{} // req 请求参数映射（灵活接受各种格式）
	if err := json.Unmarshal(in.Data, &req); err != nil {
		return nil, err
	}

	payment := Payment{ // payment 创建支付对象
		PaymentID:     uuid.New().String(),             // uuid生成唯一支付ID
		OrderID:       req["orderId"].(string),         // 从请求中提取订单ID（类型断言）
		Amount:        req["amount"].(float64),         // 提取金额
		Status:        "completed",                     // 状态设为已完成
		PaymentMethod: "credit_card",                   // 默认使用信用卡支付方式
		CreatedAt:     time.Now().Format(time.RFC3339), // 创建时间戳
	}

	if db.DB != nil { // if判断 - 如果数据库可用，持久化到数据库
		db.DB.ExecContext(ctx,
			`INSERT INTO payments (payment_id, order_id, amount, status, payment_method, created_at)
			 VALUES ($1, $2, $3, $4, $5, $6)`,
			payment.PaymentID, payment.OrderID, payment.Amount,
			payment.Status, payment.PaymentMethod, payment.CreatedAt, // SQL参数对应$1-$6占位符
		)
	}

	paymentData, _ := json.Marshal(payment)                                                   // 序列化支付对象
	daprClient.SaveState(ctx, stateStoreName, "payment-"+payment.PaymentID, paymentData, nil) // 保存到状态存储

	data, _ := json.Marshal(payment)
	return &common.Content{
		Data:        data,
		ContentType: "application/json",
	}, nil
}
