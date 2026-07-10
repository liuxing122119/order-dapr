// 库存服务主程序包 - 负责管理商品库存的查询、更新和订单事件处理
package main

import (
	"context"       // 上下文控制库，用于请求生命周期管理和超时控制
	"encoding/json" // JSON编解码库，用于数据序列化和反序列化
	"fmt"           // 格式化输出库
	"net/http"      // HTTP客户端和服务端库
	"os"            // 操作系统功能库，用于环境变量读取等
	"os/signal"     // 信号处理库，用于捕获系统信号
	"syscall"       // 系统调用库，定义了SIGTERM、SIGINT等信号常量
	"time"          // 时间处理库

	"order-dapr/db" // 导入自定义数据库操作包

	"github.com/dapr/go-sdk/client"                // Dapr Go SDK客户端包
	"github.com/dapr/go-sdk/service/common"        // Dapr通用服务接口包
	daprhttp "github.com/dapr/go-sdk/service/http" // Dapr HTTP服务实现包
)

// Inventory 库存信息结构体
// 用于表示单个商品的库存数据
type Inventory struct {
	ProductID   string  `json:"productId"`   // 商品ID - 唯一标识符
	ProductName string  `json:"productName"` // 商品名称 - 商品显示名称
	Quantity    int     `json:"quantity"`    // 库存数量 - 当前可用库存量
	Price       float64 `json:"price"`       // 商品单价 - 单位价格（元）
	Category    string  `json:"category"`    // 商品分类 - 所属类别名称
}

// 常量定义区域 - 定义服务配置相关的常量
const (
	serviceName    = "inventory-service" // 服务名称 - 标识当前微服务
	stateStoreName = "statestore"        // 状态存储名称 - Dapr状态存储组件标识
	pubsubName     = "pubsub"            // 发布订阅组件名称 - Dapr PubSub组件标识
	appPort        = ":5004"             // 应用监听端口 - HTTP服务监听地址
)

// daprClient 全局Dapr客户端对象
// 用于调用Dapr运行时的各种能力（状态管理、服务调用、发布订阅等）
var daprClient client.Client

// main 主函数 - 库存服务的入口点
// 功能：初始化数据库连接、Dapr客户端、注册HTTP处理函数、启动HTTP服务器
func main() {
	// 初始化数据库连接
	db.InitDB()
	// 注册defer语句，确保程序退出时关闭数据库连接
	defer db.CloseDB()

	var err error // 错误变量声明

	// grpcPort 从环境变量获取Dapr gRPC端口
	grpcPort := os.Getenv("DAPR_GRPC_PORT")
	if grpcPort == "" {
		// 如果环境变量未设置，使用默认端口3504
		grpcPort = "3504"
		fmt.Printf("[WARN] DAPR_GRPC_PORT not set, using default: %s\n", grpcPort)
	}

	// maxRetries 最大重试次数 - 连接Dapr的重试上限
	maxRetries := 50 // 设置为50次重试
	// retryDelay 重试间隔时间 - 每次重试之间的等待时间
	retryDelay := 2 * time.Second // 设置为2秒间隔

	// for循环 - 重试连接Dapr gRPC服务器
	// 循环变量i从0开始，到maxRetries-1结束，每次递增1
	for i := 0; i < maxRetries; i++ {
		// 打印当前尝试连接的日志信息
		fmt.Printf("[INFO] Attempting to connect to Dapr gRPC on port %s (%d/%d)...\n", grpcPort, i+1, maxRetries)

		// client.NewClientWithPort 创建指定端口的Dapr客户端
		daprClient, err = client.NewClientWithPort(grpcPort)
		if err == nil {
			// 连接成功，打印成功日志并跳出循环
			fmt.Printf("[SUCCESS] Connected to Darp gRPC on port %s\n", grpcPort)
			break // break语句 - 跳出for循环
		}
		// if判断 - 如果不是最后一次重试，则等待后继续重试
		if i < maxRetries-1 {
			time.Sleep(retryDelay) // sleep函数 - 暂停执行指定的时长
		}
	}

	// if判断 - 如果所有重试都失败，则退出程序
	if err != nil {
		return // return语句 - 退出main函数
	}
	// defer注册延迟调用 - 程序退出时关闭Dapr客户端连接
	defer daprClient.Close()

	// daprhttp.NewService 创建Dapr HTTP服务实例
	// 参数：appPort - 服务监听端口
	s := daprhttp.NewService(appPort)

	// AddServiceInvocationHandler 注册服务调用处理函数
	// 参数1: "/inventory/check" - API路径（检查库存接口）
	// 参数2: handleCheckInventory - 处理函数引用
	s.AddServiceInvocationHandler("/inventory/check", handleCheckInventory)

	// 注册更新库存接口的处理函数
	s.AddServiceInvocationHandler("/inventory/update", handleUpdateInventory)

	// 注册健康检查接口的处理函数
	s.AddServiceInvocationHandler("/health", handleHealth)

	// subscription 创建订阅配置对象
	// 用于配置PubSub事件订阅规则
	subscription := &common.Subscription{
		PubsubName: pubsubName,              // 发布订阅组件名称
		Topic:      "order.created",         // 订阅的主题名（订单创建事件）
		Route:      "/events/order/created", // 事件路由路径
	}

	// AddTopicEventHandler 注册主题事件处理函数
	// 当接收到order.created主题的消息时，调用handleOrderCreatedEvent处理
	s.AddTopicEventHandler(subscription, handleOrderCreatedEvent)

	// sigChan 创建信号通道
	// 用于接收操作系统发送的终止信号
	sigChan := make(chan os.Signal, 1)
	// signal.Notify 注册信号监听
	// 监听SIGTERM（优雅终止）和SIGINT（中断，如Ctrl+C）信号
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	// go关键字 - 启动新的goroutine（协程）异步执行
	go func() {
		// <-sigChan 从信号通道接收数据（阻塞等待信号）
		<-sigChan

		// http.Post 发送HTTP POST请求
		// 通知Dapr sidecar关闭
		http.Post("http://localhost:3504/v1.0/shutdown", "application/json", nil)
		// s.Stop 停止HTTP服务
		s.Stop()
		// os.Exit 退出程序，参数0表示正常退出
		os.Exit(0)
	}()

	// s.Start 启动HTTP服务器（阻塞运行）
	// if err != nil 判断启动是否出错
	if err := s.Start(); err != nil && err != http.ErrServerClosed {
		return // 如果出现非正常关闭错误，退出程序
	}
}

// handleHealth 健康检查处理函数
// 功能：返回服务的健康状态、功能特性和订阅信息
// 参数：
//   - ctx context.Context - 请求上下文，用于超时控制和取消操作
//   - in *common.InvocationEvent - Dapr调用事件对象，包含请求数据
//
// 返回值：
//   - *common.Content - 响应内容对象
//   - error - 错误信息（nil表示成功）
func handleHealth(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	// health 健康状态映射表
	// 使用map[string]interface{}存储异构数据
	health := map[string]interface{}{
		"service":   serviceName,                     // 服务名称
		"status":    "healthy",                       // 服务状态：健康
		"timestamp": time.Now().Format(time.RFC3339), // 当前时间戳（RFC3339格式）
		"features": []string{ // 功能特性列表（字符串切片）
			"dapr-sidecar",       // Dapr sidecar支持
			"service-invocation", // 服务间调用能力
			"state-management",   // 状态管理能力
			"pubsub-subscriber",  // 发布订阅订阅者能力
			"graceful-shutdown",  // 优雅关闭能力
		},
		"subscriptions": []string{ // 订阅的主题列表
			"order.created", // 订单创建事件
		},
	}
	// json.Marshal 将health对象序列化为JSON字节数组
	data, _ := json.Marshal(health)
	// 返回响应内容对象
	return &common.Content{
		Data:        data,               // JSON格式的响应数据
		ContentType: "application/json", // 内容类型：JSON
	}, nil // 返回nil错误表示成功
}

// handleOrderCreatedEvent 订单创建事件处理函数
// 功能：当收到订单创建事件时，创建库存预留记录
// 参数：
//   - ctx context.Context - 请求上下文
//   - e *common.TopicEvent - 主题事件对象，包含事件数据
//
// 返回值：
//   - retry bool - 是否需要重试（true表示需要重试）
//   - err error - 错误信息
func handleOrderCreatedEvent(ctx context.Context, e *common.TopicEvent) (retry bool, err error) {
	// data 类型断言 - 将事件数据断言为[]byte类型
	data, ok := e.Data.([]byte)
	// if判断 - 如果类型断言失败，直接返回不重试
	if !ok {
		return false, nil // return语句 - 返回不重试且无错误
	}

	// orderEvent 订单事件结构体（匿名结构体）
	// 用于解析订单创建事件的JSON数据
	var orderEvent struct {
		OrderID string     `json:"orderId"` // 订单ID
		UserID  string     `json:"userId"`  // 用户ID
		Items   []struct { // 订单项数组（匿名结构体切片）
			ProductID   string  `json:"productId"`   // 商品ID
			ProductName string  `json:"productName"` // 商品名称
			Quantity    int     `json:"quantity"`    // 数量
			Price       float64 `json:"price"`       // 单价
		} `json:"items"` // JSON字段名为items
	}

	// json.Unmarshal 反序列化 - 将JSON字节数据解析为结构体
	if err := json.Unmarshal(data, &orderEvent); err != nil {
		return false, err // 解析失败返回错误
	}

	// reservationRecord 库存预留记录映射
	reservationRecord := map[string]interface{}{
		"orderId":    orderEvent.OrderID,                                 // 关联的订单ID
		"itemCount":  len(orderEvent.Items),                              // 订单项数量（len函数获取切片长度）
		"status":     "reservation_pending",                              // 预留状态：待处理
		"reservedAt": time.Now().Format(time.RFC3339),                    // 预留时间戳
		"message":    "Inventory reservation initiated from order event", // 预留说明消息
	}

	// json.Marshal 序列化预留记录为JSON
	reservationData, _ := json.Marshal(reservationRecord)
	// daprClient.SaveState 保存状态到Dapr状态存储
	// 参数：ctx上下文、stateStoreName状态存储名、键名、值数据、元数据（nil表示无元数据）
	if err := daprClient.SaveState(ctx, stateStoreName, "inventory-reservation-"+orderEvent.OrderID, reservationData, nil); err != nil {
		return true, err // 保存失败返回需要重试和错误信息
	}

	// 成功返回不需要重试且无错误
	return false, nil
}

// handleCheckInventory 库存检查处理函数
// 功能：批量检查多个商品的库存是否充足
// 参数：
//   - ctx context.Context - 请求上下文
//   - in *common.InvocationEvent - 调用事件对象
//
// 返回值：
//   - *common.Content - 响应内容（包含每个商品的库存检查结果）
//   - error - 错误信息
func handleCheckInventory(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	// items 待检查的商品列表（匿名结构体切片）
	var items []struct {
		ProductID string `json:"productId"` // 商品ID
		Quantity  int    `json:"quantity"`  // 需求数量
	}
	// json.Unmarshal 反序列化请求数据
	if err := json.Unmarshal(in.Data, &items); err != nil {
		return nil, err // 解析失败返回错误
	}

	// result 结果数组 - 使用make创建与items相同长度的切片
	result := make([]map[string]interface{}, len(items))
	// allAvailable 全局可用标志 - 初始假设所有商品都可用
	allAvailable := true

	// for range循环 - 遍历待检查的商品列表
	// i是索引，item是当前元素
	for i, item := range items {
		// daprClient.GetState 从状态存储获取库存数据
		// 键名格式："inventory-" + 商品ID
		stateItem, err := daprClient.GetState(ctx, stateStoreName, "inventory-"+item.ProductID, nil)
		if err != nil {
			// 获取失败时设置该商品不可用
			result[i] = map[string]interface{}{
				"productId": item.ProductID, // 商品ID
				"available": false,          // 是否可用：否
				"error":     err.Error(),    // 错误信息
			}
			allAvailable = false // 设置全局标志为不可用
			continue             // continue语句 - 跳过本次循环剩余代码，进入下一次迭代
		}

		// inventory 库存对象变量
		var inventory Inventory
		// 反序列化库存数据
		if err := json.Unmarshal(stateItem.Value, &inventory); err != nil {
			// 解析失败时设置该商品不可用
			result[i] = map[string]interface{}{
				"productId": item.ProductID,
				"available": false,
				"error":     "parse error", // 解析错误
			}
			allAvailable = false
			continue // continue跳转 - 进入下一次循环
		}

		// available 单个商品可用性判断
		// 判断条件：库存数量 >= 需求数量
		available := inventory.Quantity >= item.Quantity
		if !available {
			// if判断 - 如果某个商品不可用，更新全局标志
			allAvailable = false
		}

		// 设置当前商品的检查结果
		result[i] = map[string]interface{}{
			"productId":   item.ProductID,        // 商品ID
			"productName": inventory.ProductName, // 商品名称
			"requested":   item.Quantity,         // 请求数量
			"available":   inventory.Quantity,    // 可用库存数量
			"isAvailable": available,             // 是否可用
		}
	}

	// response 最终响应映射
	response := map[string]interface{}{
		"allAvailable": allAvailable, // 所有商品是否都可用
		"items":        result,       // 各商品详细检查结果数组
	}

	// 序列化并返回响应
	data, _ := json.Marshal(response)
	return &common.Content{
		Data:        data,
		ContentType: "application/json",
	}, nil
}

// handleUpdateInventory 库存更新处理函数
// 功能：根据操作类型增加或减少商品库存数量
// 参数：
//   - ctx context.Context - 请求上下文
//   - in *common.InvocationEvent - 调用事件对象
//
// 返回值：
//   - *common.Content - 更新后的库存信息和操作结果
//   - error - 错误信息
func handleUpdateInventory(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	// req 更新请求结构体
	var req struct {
		ProductID string `json:"productId"` // 商品ID
		Quantity  int    `json:"quantity"`  // 变更数量
		Operation string `json:"operation"` // 操作类型（increase/decrease）
	}
	// 反序列化请求数据
	if err := json.Unmarshal(in.Data, &req); err != nil {
		return nil, err // 解析失败返回错误
	}

	// 从状态存储获取当前库存数据
	item, err := daprClient.GetState(ctx, stateStoreName, "inventory-"+req.ProductID, nil)
	if err != nil {
		return nil, err // 获取失败返回错误
	}

	// inventory 库存对象
	var inventory Inventory
	// 反序列化库存数据
	if err := json.Unmarshal(item.Value, &inventory); err != nil {
		return nil, err // 解析失败返回错误
	}

	// switch多路选择语句 - 根据操作类型执行不同的库存变更逻辑
	switch req.Operation {
	case "decrease":
		// case分支 - 减少库存操作
		inventory.Quantity -= req.Quantity // 减法运算 - 扣减库存
	case "increase":
		// case分支 - 增加库存操作
		inventory.Quantity += req.Quantity // 加法运算 - 增加库存
	default:
		// default分支 - 无效操作类型
		return nil, fmt.Errorf("invalid operation: %s", req.Operation) // 返回错误
	}

	// if判断 - 如果数据库连接存在，同步更新数据库中的库存
	if db.DB != nil {
		// db.DB.ExecContext 执行SQL UPDATE语句
		// $1, $2, $3是PostgreSQL占位符参数
		db.DB.ExecContext(ctx,
			`UPDATE inventory SET quantity = $2, updated_at = $3 WHERE product_id = $1`,
			req.ProductID, inventory.Quantity, time.Now(), // 参数：商品ID、新数量、更新时间
		)
	}

	// 序列化更新后的库存数据
	itemData, _ := json.Marshal(inventory)
	// 保存到Dapr状态存储
	daprClient.SaveState(ctx, stateStoreName, "inventory-"+req.ProductID, itemData, nil)

	// 构建成功响应
	response := map[string]interface{}{
		"success":     true,                             // 操作是否成功
		"message":     "Inventory updated successfully", // 操作消息
		"productId":   req.ProductID,                    // 商品ID
		"newQuantity": inventory.Quantity,               // 更新后的库存数量
	}
	// 序列化并返回响应
	data, _ := json.Marshal(response)
	return &common.Content{
		Data:        data,
		ContentType: "application/json",
	}, nil
}
