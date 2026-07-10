// Dapr多运行时云原生应用实践 - 库存服务模块
// 功能：管理商品库存、提供库存检查和扣减接口、订阅订单创建事件
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
)

// Inventory 商品库存结构体，存储单个商品的库存信息，支持JSON序列化
type Inventory struct {
	ProductID   string  `json:"productId"`   // 商品唯一标识符（SKU），作为主键使用
	ProductName string  `json:"productName"` // 商品显示名称，用于订单展示
	Quantity    int     `json:"quantity"`    // 当前可用库存数量，整数类型
	Price       float64 `json:"price"`       // 商品单价，单位为元（CNY）
	Category    string  `json:"category"`    // 商品分类名称，如"电子产品"、"服装"等
}

const (
	serviceName    = "inventory-service" // 服务名称常量，用于日志输出和健康检查响应
	stateStoreName = "statestore"        // Dapr状态存储组件名称，对应statestore.yaml配置
	pubsubName     = "pubsub"            // Dapr发布订阅组件名称，对应pubsub.yaml配置
	appPort        = ":5004"             // 应用服务监听端口号，Dapr Sidecar通过此端口转发请求
)

var daprClient client.Client // Dapr客户端实例全局变量，提供状态管理和服务调用能力

// main 应用程序主入口函数，负责初始化数据库连接、Dapr客户端、注册处理器并启动HTTP服务
func main() {
	// 初始化PostgreSQL数据库连接，建立与库存数据库的持久化通道
	db.InitDB()
	// 注册延迟关闭函数，确保应用退出时正确释放数据库连接资源
	defer db.CloseDB()

	var err error

	// 从环境变量读取Dapr Sidecar的gRPC通信端口（inventory-service使用3504）
	grpcPort := os.Getenv("DAPR_GRPC_PORT")
	if grpcPort == "" {
		// 环境变量未设置时使用默认端口3504
		grpcPort = "3504"
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
	// 所有重试均失败时直接退出程序，无法提供库存服务
	if err != nil {
		return
	}
	// 注册延迟关闭函数，确保退出前释放Dapr客户端连接资源
	defer daprClient.Close()

	// 创建Dapr HTTP服务实例，监听在5004端口接收来自Sidecar的请求转发
	s := daprhttp.NewService(appPort)

	// 注册库存检查接口处理器，路径为/inventory/check，供订单服务验证库存充足性
	s.AddServiceInvocationHandler("/inventory/check", handleCheckInventory)
	// 注册库存更新接口处理器，路径为/inventory/update，用于订单完成后扣减库存数量
	s.AddServiceInvocationHandler("/inventory/update", handleUpdateInventory)
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
		http.Post("http://localhost:3504/v1.0/shutdown", "application/json", nil)
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

// handleHealth 健康检查接口处理器，返回库存服务运行状态、功能特性和订阅信息
// 参数ctx：请求上下文，包含超时控制和取消信号
// 参数in：Dapr调用事件对象，包含请求数据和元信息
// 返回值：响应内容对象和可能的错误信息
func handleHealth(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	// 构造健康检查响应数据结构
	health := map[string]interface{}{
		"service":   serviceName,                     // 服务名称标识：inventory-service
		"status":    "healthy",                       // 当前健康状态：healthy表示正常运行
		"timestamp": time.Now().Format(time.RFC3339), // 响应生成时间戳
		"features": []string{ // 已启用功能特性列表
			"dapr-sidecar",       // Dapr Sidecar代理模式支持
			"service-invocation", // 服务间调用能力（被订单服务调用）
			"state-management",   // 状态管理能力（库存数据持久化）
			"pubsub-subscriber",  // 发布订阅消费者能力（接收订单事件）
			"graceful-shutdown",  // 优雅关闭机制支持
		},
		"subscriptions": []string{ // 当前已订阅的事件主题列表
			"order.created", // 订阅订单创建事件主题
		},
	}
	// 序列化健康检查数据为JSON格式并返回标准响应
	data, _ := json.Marshal(health)
	return &common.Content{
		Data:        data,               // JSON格式的健康检查数据
		ContentType: "application/json", // 声明MIME类型为JSON
	}, nil
}

// handleOrderCreatedEvent 订单创建事件处理器（发布订阅消费者），实现异步库存预留机制
// 当订单服务发布order.created事件时自动触发此函数创建库存预留记录
// 参数ctx：事件处理上下文，用于状态管理操作的传播
// 参数e：Dapr主题事件对象，包含事件数据和元信息
// 返回值：retry为是否需要重试的标志位，err为处理过程中的错误信息
func handleOrderCreatedEvent(ctx context.Context, e *common.TopicEvent) (retry bool, err error) {
	// 从事件对象中提取原始字节数据并进行类型断言
	data, ok := e.Data.([]byte)
	if !ok {
		// 数据类型不匹配时返回false表示不需要重试
		return false, nil
	}

	// 定义包含商品明细的订单事件结构体用于反序列化
	var orderEvent struct {
		OrderID string     `json:"orderId"` // 订单唯一标识符
		UserID  string     `json:"userId"`  // 下单用户标识符
		Items   []struct { // 订单商品明细数组
			ProductID   string  `json:"productId"`   // 商品标识符
			ProductName string  `json:"productName"` // 商品显示名称
			Quantity    int     `json:"quantity"`    // 购买数量
			Price       float64 `json:"price"`       // 商品单价
		} `json:"items"` // JSON字段名为items
	}

	// 将事件数据反序列化为订单事件结构体
	if err := json.Unmarshal(data, &orderEvent); err != nil {
		// JSON解析失败时返回false避免无效数据反复处理
		return false, err
	}

	// 构造库存预留记录数据结构，用于在正式扣减前预留库存资源
	reservationRecord := map[string]interface{}{
		"orderId":    orderEvent.OrderID,                                 // 关联的订单ID
		"itemCount":  len(orderEvent.Items),                              // 预留的商品种类数量
		"status":     "reservation_pending",                              // 状态标记为待确认
		"reservedAt": time.Now().Format(time.RFC3339),                    // 预留时间戳
		"message":    "Inventory reservation initiated from order event", // 备注说明
	}

	// 序列化预留记录为JSON格式
	reservationData, _ := json.Marshal(reservationRecord)
	// 通过Dapr客户端将预留记录保存到Redis状态存储中，键名使用"inventory-reservation-"前缀
	if err := daprClient.SaveState(ctx, stateStoreName, "inventory-reservation-"+orderEvent.OrderID, reservationData, nil); err != nil {
		// 保存失败时返回true触发重试机制确保数据最终一致性
		return true, err
	}
	// 处理成功返回false表示无需重试
	return false, nil
}

// handleCheckInventory 库存检查接口处理器，批量验证多个商品的库存充足性
// 这是库存服务的核心接口之一，被订单服务工作流调用以验证下单前库存是否足够
// 参数ctx：请求上下文，用于控制查询超时和传播追踪信息
// 参数in：Dapr调用事件对象，包含商品列表的JSON请求数据（商品ID和需求数量）
// 返回值：包含每个商品库存检查结果的详细响应对象或错误信息
func handleCheckInventory(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	// 定义请求结构体数组用于接收待检查的商品列表
	var items []struct {
		ProductID string `json:"productId"` // 商品唯一标识符
		Quantity  int    `json:"quantity"`  // 需要检查的库存数量
	}
	// 反序列化输入的JSON请求数据到商品数组结构
	if err := json.Unmarshal(in.Data, &items); err != nil {
		// JSON解析失败时直接返回错误信息给调用方
		return nil, err
	}

	// 预分配结果数组空间，大小与输入商品列表一致，避免动态扩容开销
	result := make([]map[string]interface{}, len(items))
	// 初始化全局可用性标志，假设所有商品都可用，后续检查中发现不足时置为false
	allAvailable := true

	// 循环遍历每个商品逐一进行库存充足性检查
	for i, item := range items {
		// 通过Dapr客户端从Redis状态存储中查询指定商品的库存数据，键名使用"inventory-"前缀
		stateItem, err := daprClient.GetState(ctx, stateStoreName, "inventory-"+item.ProductID, nil)
		if err != nil {
			// 查询失败时记录该商品为不可用并标记错误信息
			result[i] = map[string]interface{}{
				"productId": item.ProductID, // 商品标识符
				"available": false,          // 标记为不可用
				"error":     err.Error(),    // 记录具体错误原因
			}
			// 将全局可用性标志置为false，表示至少有一个商品检查失败
			allAvailable = false
			continue // 跳过当前商品继续处理下一个
		}

		// 反序列化查询到的库存数据到Inventory结构体
		var inventory Inventory
		if err := json.Unmarshal(stateItem.Value, &inventory); err != nil {
			// 数据解析失败时标记该商品为解析错误状态
			result[i] = map[string]interface{}{
				"productId": item.ProductID, // 商品标识符
				"available": false,          // 标记为不可用
				"error":     "parse error",  // 统一错误描述保护安全细节
			}
			// 全局标志置为false
			allAvailable = false
			continue // 继续处理下一个商品
		}

		// 比较当前可用库存与需求数量，判断是否满足条件
		available := inventory.Quantity >= item.Quantity
		if !available {
			// 库存不足时将全局可用性标志置为false
			allAvailable = false
		}

		// 构造该商品的详细检查结果信息
		result[i] = map[string]interface{}{
			"productId":   item.ProductID,        // 商品标识符
			"productName": inventory.ProductName, // 商品显示名称
			"requested":   item.Quantity,         // 请求的数量
			"available":   inventory.Quantity,    // 当前可用库存
			"isAvailable": available,             // 是否充足的判断结果
		}
	}

	// 构造最终的批量检查响应对象，包含全局判断结果和各商品明细
	response := map[string]interface{}{
		"allAvailable": allAvailable, // 全局标志：所有商品是否均满足库存要求
		"items":        result,       // 各商品的详细检查结果数组
	}

	// 序列化响应数据为JSON格式并返回标准Dapr响应对象
	data, _ := json.Marshal(response)
	return &common.Content{
		Data:        data,               // 完整的库存检查结果JSON数据
		ContentType: "application/json", // 声明MIME类型为JSON格式
	}, nil
}

// handleUpdateInventory 库存更新接口处理器，执行商品库存的扣减或增加操作
// 这是库存服务的另一个核心接口，被订单服务工作流在支付成功后调用以实际扣减库存数量
// 参数ctx：请求上下文，用于数据库事务控制和超时管理
// 参数in：Dapr调用事件对象，包含更新请求的JSON数据（商品ID、数量、操作类型）
// 返回值：包含更新结果的响应对象或错误信息
func handleUpdateInventory(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	// 定义库存更新请求结构体
	var req struct {
		ProductID string `json:"productId"` // 待操作的商品唯一标识符
		Quantity  int    `json:"quantity"`  // 操作的数量（正整数）
		Operation string `json:"operation"` // 操作类型：decrease(扣减)或increase(增加)
	}
	// 反序列化输入的JSON请求数据到请求结构体
	if err := json.Unmarshal(in.Data, &req); err != nil {
		// JSON解析失败时直接返回错误信息给调用方
		return nil, err
	}

	// 通过Dapr客户端从Redis状态存储中查询当前商品的最新库存数据
	item, err := daprClient.GetState(ctx, stateStoreName, "inventory-"+req.ProductID, nil)
	if err != nil {
		// 查询失败时直接返回错误信息
		return nil, err
	}

	// 反序列化查询到的库存数据到Inventory结构体获取当前状态
	var inventory Inventory
	if err := json.Unmarshal(item.Value, &inventory); err != nil {
		// 数据解析失败时返回错误信息
		return nil, err
	}

	// 根据操作类型执行不同的库存数量变更逻辑
	switch req.Operation {
	case "decrease":
		// 扣减操作：从当前库存中减去指定数量（订单出库场景）
		inventory.Quantity -= req.Quantity
	case "increase":
		// 增加操作：向当前库存中添加指定数量（退货入库场景）
		inventory.Quantity += req.Quantity
	default:
		// 不支持的操作类型时返回明确的错误信息
		return nil, fmt.Errorf("invalid operation: %s", req.Operation)
	}

	// 检查数据库连接是否可用，实现双写策略：同时更新PostgreSQL和Dapr状态存储
	if db.DB != nil {
		// 执行SQL UPDATE语句更新PostgreSQL数据库中的库存记录
		db.DB.ExecContext(ctx,
			`UPDATE inventory SET quantity = $2, updated_at = $3 WHERE product_id = $1`, // 参数化查询防止SQL注入
			req.ProductID,      // 商品标识符作为WHERE条件
			inventory.Quantity, // 更新后的新库存数量
			time.Now(),         // 当前时间作为最后更新时间
		)
	}

	// 将更新后的库存数据序列化为JSON格式并保存到Dapr Redis状态存储中
	itemData, _ := json.Marshal(inventory)
	daprClient.SaveState(ctx, stateStoreName, "inventory-"+req.ProductID, itemData, nil)

	// 构造成功的更新结果响应对象
	response := map[string]interface{}{
		"success":     true,                             // 操作成功标志
		"message":     "Inventory updated successfully", // 成功提示消息
		"productId":   req.ProductID,                    // 被操作的商品标识符
		"newQuantity": inventory.Quantity,               // 更新后的最新库存数量
	}
	// 序列化响应数据为JSON格式并返回标准Dapr响应对象
	data, _ := json.Marshal(response)
	return &common.Content{
		Data:        data,               // 完整的更新结果JSON数据
		ContentType: "application/json", // 声明MIME类型为JSON格式
	}, nil
}
