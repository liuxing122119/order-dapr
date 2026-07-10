// 用户服务主程序包 - 负责用户信息管理和用户验证服务
// 功能：提供用户验证接口、健康检查、优雅关闭等核心功能
package main

import (
	"context"       // 上下文控制库，用于请求生命周期管理
	"encoding/json" // JSON编解码库，用于数据序列化和反序列化
	"fmt"           // 格式化输出库
	"net/http"      // HTTP客户端和服务端库
	"os"            // 操作系统功能库，用于环境变量读取
	"os/signal"     // 信号处理库，用于捕获系统信号
	"sync"          // 同步原语库（sync.Once用于单例模式）
	"syscall"       // 系统调用库，定义SIGTERM、SIGINT等常量
	"time"          // 时间处理库

	"github.com/dapr/go-sdk/client"                // Dapr Go SDK客户端包
	"github.com/dapr/go-sdk/service/common"        // Dapr通用服务接口包
	daprhttp "github.com/dapr/go-sdk/service/http" // Dapr HTTP服务实现包
)

// User 用户信息结构体
// 用于表示系统中的用户实体数据
type User struct {
	UserID    string `json:"userId"`              // 用户唯一标识符
	Name      string `json:"name"`                // 用户姓名
	Email     string `json:"email"`               // 电子邮箱地址
	Phone     string `json:"phone"`               // 联系电话号码
	Address   string `json:"address"`             // 联系地址
	CreatedAt string `json:"createdAt,omitempty"` // 账户创建时间（可选字段）
}

// UserValidationRequest 用户验证请求结构体
// 作为用户验证接口的输入参数
type UserValidationRequest struct {
	UserID string `json:"userId"` // 待验证的用户ID（必填）
}

// UserValidationResponse 用户验证响应结构体
// 作为用户验证接口的返回结果
type UserValidationResponse struct {
	Valid   bool   `json:"valid"`          // 是否有效标志（true:有效, false:无效）
	Message string `json:"message"`        // 验证结果消息说明
	User    *User  `json:"user,omitempty"` // 用户详细信息（可选，仅当valid=true时返回）
}

// 常量定义区域 - 服务配置参数
const (
	serviceName    = "user-service" // 服务名称 - 标识当前微服务
	stateStoreName = "statestore"   // 状态存储组件名称
	appPort        = ":5001"        // HTTP服务监听端口
)

// 全局变量定义区域 - 使用sync.Once实现单例模式的Dapr客户端
var (
	daprClient     client.Client // daprClient 全局Dapr客户端对象
	daprClientOnce sync.Once     // daprClientOnce 确保只初始化一次的同步原语
	daprClientErr  error         // daprClientErr 客户端初始化错误
)

// getDaprClient 获取Dapr客户端函数（单例模式）
// 功能：使用sync.Once确保Dapr客户端只被初始化一次，避免重复连接
// 返回值：
//   - client.Client - Dapr客户端实例
//   - error - 初始化错误（如果有的话）
func getDaprClient() (client.Client, error) {
	daprClientOnce.Do(func() { // Do方法 - 只执行一次（即使多次调用也只会执行第一次）
		var err error

		grpcPort := os.Getenv("DAPR_GRPC_PORT") // 从环境变量获取gRPC端口
		if grpcPort == "" {
			grpcPort = "3501" // 默认使用3501端口
			fmt.Printf("[WARN] DAPR_GRPC_PORT not set, using default: %s\n", grpcPort)
		}

		maxRetries := 50              // 最大重试次数设为50次
		retryDelay := 2 * time.Second // 重试间隔设为2秒

		for i := 0; i < maxRetries; i++ { // for循环重试连接
			fmt.Printf("[INFO] Attempting to connect to Dapr gRPC on port %s (%d/%d)...\n", grpcPort, i+1, maxRetries)
			daprClient, err = client.NewClientWithPort(grpcPort) // 创建指定端口的客户端
			if err == nil {
				fmt.Printf("[SUCCESS] Connected to Dapr gRPC on port %s\n", grpcPort)
				return // 连接成功直接return退出Do函数
			}
			if i < maxRetries-1 {
				time.Sleep(retryDelay) // sleep等待后继续重试
			}
		}
		daprClientErr = fmt.Errorf("failed to initialize Dapr client after %d attempts: %w", maxRetries, err)
	})
	return daprClient, daprClientErr // return返回客户端和可能的错误
}

// main 主函数 - 用户服务的入口点
// 功能：初始化Dapr客户端、注册HTTP接口、启动HTTP服务、等待终止信号
func main() {
	var err error
	daprClient, err = getDaprClient() // getDaprClient 获取单例Dapr客户端
	if err != nil {
		return // 初始化失败退出程序
	}
	defer func() { // defer延迟调用 - 确保程序退出时关闭客户端
		if daprClient != nil {
			daprClient.Close()
		}
	}()

	s := daprhttp.NewService(appPort) // 创建HTTP服务实例

	s.AddServiceInvocationHandler("/user/validate", handleValidateUser) // 注册用户验证接口
	s.AddServiceInvocationHandler("/health", handleHealth)              // 注册健康检查接口

	sigChan := make(chan os.Signal, 1)                      // sigChan 创建信号通道（缓冲区大小为1）
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT) // 监听终止和中断信号

	go func() { // go关键字 - 启动goroutine异步执行优雅关闭逻辑
		<-sigChan                                                                 // 阻塞等待信号到达
		http.Post("http://localhost:3501/v1.0/shutdown", "application/json", nil) // 通知Dapr sidecar关闭
		s.Stop()                                                                  // 停止HTTP服务
		os.Exit(0)                                                                // os.Exit正常退出程序（状态码0）
	}()

	if err := s.Start(); err != nil && err != http.ErrServerClosed { // s.Start启动服务并阻塞运行
		return // 非正常关闭错误则退出
	}
}

// handleHealth 健康检查处理函数
// 功能：返回服务的健康状态、支持的功能特性列表
func handleHealth(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	health := map[string]interface{}{ // health 健康信息映射表
		"service":   serviceName,
		"status":    "healthy",
		"timestamp": time.Now().Format(time.RFC3339),
		"features": []string{ // features 功能特性列表（字符串切片）
			"dapr-sidecar",
			"service-invocation",
			"state-management",
			"etag-concurrency-control", // ETag并发控制能力
			"user-validation-api",      // 用户验证API
			"graceful-shutdown",
		},
	}
	data, _ := json.Marshal(health) // json.Marshal 序列化为JSON字节数组
	return &common.Content{
		Data:        data,
		ContentType: "application/json",
	}, nil
}

// handleValidateUser 用户验证处理函数
// 服务间调用接口：供Order Service验证用户有效性（模块1核心）
// 功能：根据用户ID查询状态存储中的用户数据，判断用户是否有效
// 参数：
//   - ctx context.Context - 请求上下文
//   - in *common.InvocationEvent - Dapr调用事件对象
//
// 返回值：
//   - *common.Content - 响应内容（包含验证结果和可选的用户信息）
//   - error - 错误信息
func handleValidateUser(ctx context.Context, in *common.InvocationEvent) (*common.Content, error) {
	var req UserValidationRequest // req 用户验证请求对象
	if err := json.Unmarshal(in.Data, &req); err != nil {
		return nil, fmt.Errorf("invalid validation request: %w", err) // 反序列化失败返回错误
	}

	if req.UserID == "" { // if判断 - 检查UserID是否为空字符串
		response := UserValidationResponse{
			Valid:   false,                // 设置无效标志
			Message: "userID is required", // 错误消息：缺少用户ID
		}
		data, _ := json.Marshal(response)
		return &common.Content{Data: data, ContentType: "application/json"}, nil
	}

	item, err := daprClient.GetState(ctx, stateStoreName, "user-"+req.UserID, nil)
	if err != nil { // if判断 - 获取状态失败
		response := UserValidationResponse{
			Valid:   false,
			Message: fmt.Sprintf("error checking user: %v", err), // 返回具体错误信息
		}
		data, _ := json.Marshal(response)
		return &common.Content{Data: data, ContentType: "application/json"}, nil
	}

	if item == nil || len(item.Value) == 0 { // if判断 - 检查用户数据是否为空
		response := UserValidationResponse{
			Valid:   false,
			Message: "user not found", // 用户不存在
		}
		data, _ := json.Marshal(response)
		return &common.Content{Data: data, ContentType: "application/json"}, nil
	}

	var user User                                             // user 用户对象变量
	if err := json.Unmarshal(item.Value, &user); err != nil { // 反序列化用户数据
		response := UserValidationResponse{
			Valid:   false,
			Message: "internal error", // 内部解析错误
		}
		data, _ := json.Marshal(response)
		return &common.Content{Data: data, ContentType: "application/json"}, nil
	}

	response := UserValidationResponse{ // response 构建成功响应
		Valid:   true,            // 设置有效标志为true
		Message: "user is valid", // 成功消息
		User:    &user,           // 返回用户详细信息指针
	}
	data, _ := json.Marshal(response)
	return &common.Content{
		Data:        data,
		ContentType: "application/json",
	}, nil // return返回成功响应和无错误
}
