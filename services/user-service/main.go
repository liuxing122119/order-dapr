// Dapr多运行时云原生应用实践 - 用户服务模块
// 功能：提供用户信息查询、用户有效性验证等服务
package main

import (
	"context"       // 上下文管理，用于控制请求生命周期和超时
	"encoding/json" // JSON序列化与反序列化库
	"fmt"           // 格式化输入输出库
	"net/http"      // HTTP客户端和服务端实现
	"os"            // 操作系统功能接口（环境变量、信号等）
	"os/signal"     // 操作系统信号处理（SIGTERM、SIGINT等）
	"sync"          // 同步原语（互斥锁、单次执行等）
	"syscall"       // 系统调用常量定义
	"time"          // 时间处理和格式化工具

	"github.com/dapr/go-sdk/client"                // Dapr Go SDK客户端包
	"github.com/dapr/go-sdk/service/common"        // Dapr通用服务接口定义
	daprhttp "github.com/dapr/go-sdk/service/http" // Dapr HTTP服务实现
)

// User 用户信息结构体，存储用户的基本资料，支持JSON序列化
type User struct {
	UserID    string `json:"userId"`              // 用户唯一标识符，作为主键使用
	Name      string `json:"name"`                // 用户真实姓名
	Email     string `json:"email"`               // 电子邮箱地址，用于通知发送
	Phone     string `json:"phone"`               // 联系电话号码，11位手机号格式
	Address   string `json:"address"`             // 联系地址信息，包含省市区详细地址
	CreatedAt string `json:"createdAt,omitempty"` // 账户创建时间，RFC3339格式（可选字段）
}

// UserValidationRequest 用户验证请求结构体，接收订单服务发起的用户有效性查询请求
type UserValidationRequest struct {
	UserID string `json:"userId"` // 待验证的用户标识符，必填字段
}

// UserValidationResponse 用户验证响应结构体，返回验证结果和可选的用户详细信息
type UserValidationResponse struct {
	Valid   bool   `json:"valid"`          // 用户有效性标志：true表示有效，false表示无效或不存在
	Message string `json:"message"`        // 验证结果说明消息，描述验证失败的具体原因
	User    *User  `json:"user,omitempty"` // 用户详细信息对象指针，仅在验证通过时返回
}

const (
	serviceName    = "user-service" // 服务名称常量，用于日志输出和健康检查响应
	stateStoreName = "statestore"   // Dapr状态存储组件名称，对应statestore.yaml配置
	appPort        = ":5001"        // 应用服务监听端口号，Dapr Sidecar通过此端口转发请求
)

var (
	daprClient     client.Client // Dapr客户端实例，提供状态管理和服务调用能力
	daprClientOnce sync.Once     // 同步原语，确保Dapr客户端在整个应用生命周期中只初始化一次
	daprClientErr  error         // Dapr客户端初始化过程中捕获的错误信息，供后续检查使用
)

// getDaprClient 获取Dapr客户端实例（单例模式），支持自动重试连接机制
// 采用sync.Once确保整个应用生命周期中只执行一次初始化逻辑
// 返回值：client.Client为Dapr客户端实例，error为初始化过程中的错误信息
func getDaprClient() (client.Client, error) {
	daprClientOnce.Do(func() {
		var err error

		// 从系统环境变量读取Dapr Sidecar的gRPC通信端口
		grpcPort := os.Getenv("DAPR_GRPC_PORT")
		if grpcPort == "" {
			// 环境变量未设置时使用默认端口3501（user-service的标准端口）
			grpcPort = "3501"
			fmt.Printf("[WARN] DAPR_GRPC_PORT not set, using default: %s\n", grpcPort)
		}

		// 配置连接重试策略：最多尝试50次连接，每次间隔2秒等待
		maxRetries := 50              // 最大重试次数，确保有足够时间等待Sidecar启动完成
		retryDelay := 2 * time.Second // 重试间隔时间，避免频繁连接造成资源浪费
		for i := 0; i < maxRetries; i++ {
			// 打印当前连接进度日志，便于调试和监控初始化过程
			fmt.Printf("[INFO] Attempting to connect to Dapr gRPC on port %s (%d/%d)...\n", grpcPort, i+1, maxRetries)
			// 调用Dapr SDK创建指定端口的gRPC客户端连接
			daprClient, err = client.NewClientWithPort(grpcPort)
			if err == nil {
				// 连接成功时输出确认信息并立即返回，不再进行后续重试
				fmt.Printf("[SUCCESS] Connected to Dapr gRPC on port %s\n", grpcPort)
				return
			}
			// 当前连接失败且未达到最大重试次数限制时，休眠后继续下一次尝试
			if i < maxRetries-1 {
				time.Sleep(retryDelay)
			}
		}
		// 所有重试均失败时构造错误信息并保存到全局变量
		daprClientErr = fmt.Errorf("failed to initialize Dapr client after %d attempts: %w", maxRetries, err)
	})
	// 返回已初始化的客户端实例或nil，以及可能的错误信息
	return daprClient, daprClientErr
}

// main 应用程序主入口函数，负责初始化Dapr客户端、注册HTTP处理器、启动HTTP服务并处理优雅关闭
func main() {
	// 调用单例模式获取Dapr客户端实例，建立与Sidecar的gRPC通信连接
	var err error
	daprClient, err = getDaprClient()
	if err != nil {
		// Dapr客户端初始化失败时直接退出，无法提供正常服务
		return
	}
	// 注册延迟关闭函数，确保应用退出前释放Dapr客户端连接资源
	defer func() {
		if daprClient != nil {
			daprClient.Close()
		}
	}()

	// 创建Dapr HTTP服务实例，监听在5001端口接收来自Sidecar的请求转发
	s := daprhttp.NewService(appPort)

	// 注册用户验证接口处理器，路径为/user/validate，供订单服务调用验证用户有效性
	s.AddServiceInvocationHandler("/user/validate", handleValidateUser)
	// 注册健康检查接口处理器，路径为/health，用于服务监控和状态探测
	s.AddServiceInvocationHandler("/health", handleHealth)

	// 创建操作系统信号通道，用于捕获终止信号实现优雅关闭
	sigChan := make(chan os.Signal, 1)
	// 监听SIGTERM（正常终止）和SIGINT（Ctrl+C中断）两种信号
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	// 启动后台协程异步监听信号，避免阻塞主线程的HTTP服务运行
	go func() {
		// 阻塞等待接收到操作系统终止信号
		<-sigChan

		// 向Dapr Sidecar发送shutdown请求，触发Sidecar优雅关闭流程
		http.Post("http://localhost:3501/v1.0/shutdown", "application/json", nil)
		// 停止Dapr HTTP服务，释放端口资源
		s.Stop()
		// 以退出码0正常结束进程
		os.Exit(0)
	}()

	// 启动HTTP服务并阻塞等待请求，除非遇到非正常关闭错误否则持续运行
	if err := s.Start(); err != nil && err != http.ErrServerClosed {
		return
	}
}

// handleHealth 健康检查接口处理器，返回服务运行状态和已启用功能列表
// 参数ctx：请求上下文，包含超时控制和取消信号
// 参数in：Dapr调用事件对象，包含请求数据和元信息
// 返回值：响应内容对象和可能的错误信息
func handleHealth(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	// 构造健康检查响应数据结构，包含服务基本信息、状态时间戳和功能特性列表
	health := map[string]interface{}{
		"service":   serviceName,                     // 服务名称标识
		"status":    "healthy",                       // 当前健康状态：healthy表示正常运行
		"timestamp": time.Now().Format(time.RFC3339), // 响应生成时间戳，用于判断时效性
		"features": []string{ // 已启用功能特性列表，展示服务能力
			"dapr-sidecar",             // Dapr Sidecar代理模式支持
			"service-invocation",       // 服务间调用能力（被订单服务调用）
			"state-management",         // 状态管理能力（用户数据持久化）
			"etag-concurrency-control", // ETag并发控制机制（乐观锁）
			"user-validation-api",      // 用户验证API接口
			"graceful-shutdown",        // 优雅关闭机制支持
		},
	}
	// 将健康检查数据序列化为JSON格式
	data, _ := json.Marshal(health)
	// 构造Dapr标准响应内容对象，指定JSON内容类型
	return &common.Content{
		Data:        data,               // JSON序列化后的响应数据
		ContentType: "application/json", // MIME类型声明为JSON格式
	}, nil
}

// handleValidateUser 用户验证接口处理器，供订单服务调用验证用户有效性和获取用户详细信息
// 这是用户服务的核心业务接口，通过Dapr状态存储查询用户数据并返回验证结果
// 参数ctx：请求上下文，用于控制查询超时和传播追踪信息
// 参数in：Dapr调用事件，包含UserValidationRequest格式的JSON请求数据
// 返回值：包含验证结果的用户响应对象或错误信息
func handleValidateUser(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	// 定义请求结构体变量并反序列化输入的JSON数据
	var req UserValidationRequest
	if err := json.Unmarshal(in.Data, &req); err != nil {
		// JSON解析失败时返回格式错误异常
		return nil, fmt.Errorf("invalid validation request: %w", err)
	}

	// 校验必填字段：用户ID不能为空字符串
	if req.UserID == "" {
		// 构造参数缺失的错误响应
		response := UserValidationResponse{
			Valid:   false,                // 标记为无效请求
			Message: "userID is required", // 错误原因说明
		}
		data, _ := json.Marshal(response)
		return &common.Content{Data: data, ContentType: "application/json"}, nil
	}

	// 通过Dapr客户端从Redis状态存储中查询用户数据，键名为"user-"前缀加用户ID
	item, err := daprClient.GetState(ctx, stateStoreName, "user-"+req.UserID, nil)
	if err != nil {
		// 状态存储查询失败时返回系统错误响应
		response := UserValidationResponse{
			Valid:   false,                                       // 标记为验证失败
			Message: fmt.Sprintf("error checking user: %v", err), // 包含具体错误信息的消息
		}
		data, _ := json.Marshal(response)
		return &common.Content{Data: data, ContentType: "application/json"}, nil
	}

	// 检查查询结果是否为空（用户不存在的情况）
	if item == nil || len(item.Value) == 0 {
		// 返回用户未找到的响应
		response := UserValidationResponse{
			Valid:   false,            // 标记为无效用户
			Message: "user not found", // 明确说明用户不存在
		}
		data, _ := json.Marshal(response)
		return &common.Content{Data: data, ContentType: "application/json"}, nil
	}

	// 反序列化查询到的用户数据到User结构体
	var user User
	if err := json.Unmarshal(item.Value, &user); err != nil {
		// 数据解析失败时返回内部错误响应
		response := UserValidationResponse{
			Valid:   false,            // 标记为处理失败
			Message: "internal error", // 隐藏具体错误细节保护安全
		}
		data, _ := json.Marshal(response)
		return &common.Content{Data: data, ContentType: "application/json"}, nil
	}

	// 构造验证成功的完整响应，包含用户详细信息和有效性标志
	response := UserValidationResponse{
		Valid:   true,            // 标记为有效用户
		Message: "user is valid", // 成功提示消息
		User:    &user,           // 返回完整的用户资料对象指针
	}
	// 序列化最终响应为JSON格式并返回
	data, _ := json.Marshal(response)
	return &common.Content{
		Data:        data,               // 完整的JSON响应数据
		ContentType: "application/json", // 声明内容类型
	}, nil
}
