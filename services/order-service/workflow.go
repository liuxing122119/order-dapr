// Dapr多运行时云原生应用实践 - 工作流编排器定义模块（V2版本）
// 功能：订单处理工作流编排逻辑、版本控制、输入输出结构体定义
package main

import (
	"context"       // 上下文管理，用于控制请求生命周期和超时
	"encoding/json" // JSON序列化与反序列化库
	"fmt"           // 格式化输入输出库
	"time"          // 时间处理和格式化工具

	"github.com/dapr/durabletask-go/task" // Dapr持久化任务框架，提供编排器和活动接口
	"github.com/dapr/go-sdk/client"       // Dapr Go SDK客户端包
)

const (
	WorkflowNameV1 = "OrderProcessingWorkflow-v1" // V1版本工作流名称标识符（兼容旧版）
	WorkflowNameV2 = "OrderProcessingWorkflow-v2" // V2版本工作流名称标识符（当前默认）
	CurrentVersion = "v2"                         // 当前活跃的工作流版本号常量
)

// OrderWorkflowInput 订单处理工作流输入参数结构体，包含启动工作流所需的全部业务数据
type OrderWorkflowInput struct {
	OrderID     string      `json:"orderId"`           // 订单唯一标识符（必填字段）
	UserID      string      `json:"userId"`            // 用户唯一标识符（必填字段）
	Items       []OrderItem `json:"items"`             // 购买的商品列表数组（至少一个商品）
	TotalAmount float64     `json:"totalAmount"`       // 订单总金额（单位：元/CNY）
	Version     string      `json:"version,omitempty"` // 工作流版本号（可选字段，默认使用CurrentVersion）
}

// OrderWorkflowOutput 订单处理工作流输出结果结构体，记录每个步骤的执行状态和最终结果
type OrderWorkflowOutput struct {
	OrderID          string `json:"orderId"`          // 关联的订单唯一标识符
	Status           string `json:"status"`           // 最终执行状态（completed/failed等）
	InventoryValid   bool   `json:"inventoryValid"`   // 库存验证是否通过标志
	PaymentSuccess   bool   `json:"paymentSuccess"`   // 支付处理是否成功标志
	InventoryUpdated bool   `json:"inventoryUpdated"` // 库存扣减是否完成标志
	NotificationSent bool   `json:"notificationSent"` // 通知是否已发送标志
	Error            string `json:"error,omitempty"`  // 错误信息字符串（仅在失败时有值）
	WorkflowVersion  string `json:"workflowVersion"`  // 执行此工作流的版本号标识
}

// ExecuteOrderProcessingWorkflow V2版本的订单处理工作流编排器核心实现函数
// 按顺序编排四个活动步骤：库存验证→支付处理→库存更新→通知发送，实现完整的订单业务流程
// 参数ctx：Dapr编排器上下文对象，提供活动调用、输入获取、日志记录等核心能力
// 返回值：工作流最终执行结果和可能的错误信息（包含每个步骤的状态详情）
func ExecuteOrderProcessingWorkflow(ctx *task.OrchestrationContext) (any, error) {
	// 定义输入结构体变量用于接收启动工作流时传入的参数数据
	input := OrderWorkflowInput{}
	// 从编排器上下文中反序列化JSON格式的输入参数到结构体
	if err := ctx.GetInput(&input); err != nil {
		// 输入参数解析失败时直接返回空输出和错误信息给调用方
		return OrderWorkflowOutput{}, err
	}

	// 记录工作流开始时间用于计算总执行耗时和性能指标统计
	startTime := time.Now()
	// 创建后台上下文对象用于可观测性模块的指标记录操作
	wfCtx := context.Background()
	// 注册延迟恢复函数确保无论正常完成还是异常都能记录工作流完成事件
	defer func() {
		// 调用可观测性模块记录工作流完成状态到Prometheus指标系统
		recordWorkflowComplete(wfCtx, ctx.Name, "completed", time.Since(startTime)) // 记录名称、状态、耗时
	}()

	// 初始化输出结构体对象设置订单ID和工作流版本号字段
	output := OrderWorkflowOutput{
		OrderID:         input.OrderID,  // 关联的订单唯一标识符
		WorkflowVersion: CurrentVersion, // 标记使用V2版本的逻辑执行
	}

	// ===== 第一步：调用库存验证活动检查商品库存充足性 =====
	var validateResult interface{} // 定义通用接口变量接收活动返回结果
	// 调用ValidateInventoryActivity活动并等待其完成执行
	err := ctx.CallActivity("ValidateInventoryActivity", // 活动名称标识
		task.WithActivityInput(input), // 传递完整的工作流输入作为活动参数
	).Await(&validateResult) // 等待活动完成并将结果存储到变量中

	if err != nil {
		// 库存验证活动执行出错时更新输出状态为失败并记录错误信息
		output.Status = "failed"                                           // 失败状态标识
		output.Error = fmt.Sprintf("inventory validation failed: %v", err) // 错误详情
		return output, err                                                 // 返回失败结果和原始错误终止工作流
	}

	// 类型断言将验证结果转换为具体的输出结构体类型以便访问详细字段
	if validateResultVal, ok := validateResult.(OrderWorkflowOutput); ok { // 类型安全转换
		output.InventoryValid = validateResultVal.InventoryValid // 提取库存验证标志
		// 条件判断：检查库存是否充足可用
		if !validateResultVal.InventoryValid { // 库存不足时的错误分支
			output.Status = "inventory_unavailable"              // 设置状态为库存不足
			output.Error = validateResultVal.Error               // 保存详细的错误消息
			return output, fmt.Errorf("inventory not available") // 终止工作流并返回错误
		}
	}

	// ===== 第二步：调用支付处理活动执行订单扣款操作 =====
	var paymentResult interface{} // 定义通用接口变量接收支付活动返回结果
	// 调用ProcessPaymentActivity活动并等待其完成执行
	err = ctx.CallActivity("ProcessPaymentActivity", // 活动名称标识
		task.WithActivityInput(input), // 传递完整的工作流输入作为活动参数
	).Await(&paymentResult) // 等待活动完成并将结果存储到变量中

	if err != nil {
		// 支付处理活动执行出错时更新输出状态为失败并记录错误信息
		output.Status = "failed"                                         // 失败状态标识
		output.Error = fmt.Sprintf("payment processing failed: %v", err) // 错误详情
		return output, err                                               // 返回失败结果和原始错误终止工作流
	}

	// 类型断言将支付结果转换为具体的输出结构体类型以便访问详细字段
	if paymentResultVal, ok := paymentResult.(OrderWorkflowOutput); ok { // 类型安全转换
		output.PaymentSuccess = paymentResultVal.PaymentSuccess // 提取支付成功标志
		// 条件判断：检查支付是否成功完成
		if !paymentResultVal.PaymentSuccess { // 支付失败时的错误分支
			output.Status = "payment_failed"                   // 设置状态为支付失败
			output.Error = paymentResultVal.Error              // 保存详细的错误消息
			return output, fmt.Errorf("payment not completed") // 终止工作流并返回错误
		}
	}

	// ===== 第三步：调用库存更新活动扣减已售商品的库存数量 =====
	var updateResult interface{} // 定义通用接口变量接收库存更新活动返回结果
	// 调用UpdateInventoryActivity活动并等待其完成执行
	err = ctx.CallActivity("UpdateInventoryActivity", // 活动名称标识
		task.WithActivityInput(input), // 传递完整的工作流输入作为活动参数
	).Await(&updateResult) // 等待活动完成并将结果存储到变量中

	if err != nil {
		// 库存更新活动执行出错时更新输出状态为失败并记录错误信息
		output.Status = "failed"                                       // 失败状态标识
		output.Error = fmt.Sprintf("inventory update failed: %v", err) // 错误详情
		return output, err                                             // 返回失败结果和原始错误终止工作流
	}

	// 类型断言将库存更新结果转换为具体的输出结构体类型以便访问详细字段
	if updateResultVal, ok := updateResult.(OrderWorkflowOutput); ok { // 类型安全转换
		output.InventoryUpdated = updateResultVal.InventoryUpdated // 提取库存更新标志
		// 条件判断：检查库存扣减是否成功完成
		if !updateResultVal.InventoryUpdated { // 库存更新失败时的错误分支
			output.Status = "failed"                             // 设置状态为失败
			output.Error = updateResultVal.Error                 // 保存详细的错误消息
			return output, fmt.Errorf("inventory update failed") // 终止工作流并返回错误
		}
	}

	// ===== 第四步：调用通知发送活动通知用户订单处理完成 =====
	ctx.CallActivity("SendNotificationActivity", // 活动名称标识（最后一步）
		task.WithActivityInput(input), // 传递完整的工作流输入作为活动参数
	) // 注意：此步骤不等待结果直接异步执行以提高响应速度
	output.NotificationSent = true // 标记通知已发送（乐观设置）

	// 所有步骤均成功执行完毕时设置最终状态为已完成并返回成功结果
	output.Status = "completed" // 最终成功状态标识
	return output, nil          // 返回完整的成功结果无错误
}

// ExecuteOrderProcessingWorkflowV1 V1版本的订单处理工作流编排器（兼容性实现）
// 保留旧版逻辑用于向后兼容支持旧客户端或配置仍使用V1版本的场景
// 参数ctx：Dapr编排器上下文对象，提供活动调用、输入获取等能力
// 返回值：工作流最终执行结果和可能的错误信息（使用V1版本的逻辑和状态码）
func ExecuteOrderProcessingWorkflowV1(ctx *task.OrchestrationContext) (any, error) {
	// 定义输入结构体变量用于接收启动工作流时传入的参数数据
	input := OrderWorkflowInput{}
	// 从编排器上下文中反序列化JSON格式的输入参数到结构体
	if err := ctx.GetInput(&input); err != nil {
		// 输入参数解析失败时直接返回空输出和错误信息给调用方
		return OrderWorkflowOutput{}, err
	}

	// 记录工作流开始时间用于计算总执行耗时和性能指标统计
	startTime := time.Now()
	// 创建后台上下文对象用于可观测性模块的指标记录操作
	wfCtx := context.Background()
	// 注册延迟恢复函数确保无论正常完成还是异常都能记录工作流完成事件
	defer func() {
		// 调用可观测性模块记录工作流完成状态到Prometheus指标系统
		recordWorkflowComplete(wfCtx, ctx.Name, "completed", time.Since(startTime)) // 记录名称、状态、耗时
	}()

	// 初始化输出结构体对象设置订单ID和工作流版本号字段（标记为v1版本）
	output := OrderWorkflowOutput{
		OrderID:         input.OrderID, // 关联的订单唯一标识符
		WorkflowVersion: "v1",          // 标记使用V1版本的逻辑执行
	}

	// ===== V1第一步：调用旧版库存验证活动实现 =====
	output = executeValidateInventoryActivityLegacy(wfCtx, output, input) // 使用V1专用函数
	// 检查库存验证步骤是否失败或库存不足
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
	output.NotificationSent = true                              // 标记通知已发送
	output.Status = "completed"                                 // 设置最终成功状态

	return output, nil // 返回V1版本的完整成功结果无错误
}

// executeValidateInventoryActivityLegacy V1版本的库存验证活动实现（降级兼容专用）
// 直接在当前进程内调用库存服务验证商品库存充足性，不经过Dapr引擎调度
// 参数ctx：请求上下文，用于Dapr服务调用和指标记录
// 参数output：当前工作流输出对象（用于累加更新结果状态）
// 参数input：工作流输入参数结构体（包含订单ID、用户信息、商品列表等）
// 返回值：更新后的工作流输出结构体（包含库存验证结果或错误信息）
func executeValidateInventoryActivityLegacy(ctx context.Context, output OrderWorkflowOutput, input OrderWorkflowInput) OrderWorkflowOutput {

	// 记录活动开始时间用于计算执行耗时和性能指标统计
	activityStart := time.Now() // 起始时间戳
	// 注册延迟恢复函数确保无论正常返回还是异常都能记录活动执行情况
	defer func() {
		success := output.Error == ""                                      // 根据错误字段是否为空判断执行是否成功
		recordActivityExecution(ctx, "ValidateInventoryActivity", success) // 记录到Prometheus指标系统
		_ = time.Since(activityStart)                                      // 计算耗时（预留用于未来的性能分析）
	}()

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
	resp, err := daprClient.InvokeMethodWithContent(ctx, "inventory-service", "/inventory/check", "post", content)
	if err != nil {
		// 服务调用失败时更新输出状态为失败并记录错误信息
		output.Status = "failed"                                      // 失败状态标识
		output.InventoryValid = false                                 // 库存验证标志设为false
		output.Error = fmt.Sprintf("inventory check failed: %v", err) // 错误信息详情
		return output
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
		return output
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
		return output
	}

	// 所有商品库存均充足时设置验证通过标志并返回成功结果
	output.InventoryValid = true // 库存验证标志设为true表示全部通过
	return output
}

// executeProcessPaymentActivityLegacy V1版本的支付处理活动实现（降级兼容专用）
// 直接在当前进程内调用支付服务执行订单扣款操作，不经过Dapr引擎调度
// 参数ctx：请求上下文，用于Dapr服务调用和指标记录
// 参数output：当前工作流输出对象（用于累加更新结果状态）
// 参数input：工作流输入参数结构体（包含订单ID、用户信息、金额等）
// 返回值：更新后的工作流输出结构体（包含支付处理结果或错误信息）
func executeProcessPaymentActivityLegacy(ctx context.Context, output OrderWorkflowOutput, input OrderWorkflowInput) OrderWorkflowOutput {

	// 记录活动开始时间用于计算执行耗时和性能指标统计
	activityStart := time.Now() // 起始时间戳
	// 注册延迟恢复函数确保无论正常返回还是异常都能记录活动执行情况
	defer func() {
		success := output.Error == ""                                   // 根据错误字段是否为空判断执行是否成功
		recordActivityExecution(ctx, "ProcessPaymentActivity", success) // 记录到Prometheus指标系统
		_ = time.Since(activityStart)                                   // 计算耗时（预留用于未来的性能分析）
	}()

	// 定义支付请求内部结构体用于构造支付服务调用所需的参数格式
	type PaymentRequest struct {
		OrderID     string  `json:"orderId"`     // 订单唯一标识符（关联字段）
		UserID      string  `json:"userId"`      // 用户标识符（扣款账户）
		Amount      float64 `json:"amount"`      // 支付金额（单位：元/CNY）
		Currency    string  `json:"currency"`    // 货币类型（如"CNY"）
		Description string  `json:"description"` // 支付描述信息
	}

	// 构造支付请求对象填充所有必需字段用于调用支付服务接口
	paymentReq := PaymentRequest{
		OrderID:     input.OrderID,                                                    // 订单唯一标识符关联到支付记录
		UserID:      input.UserID,                                                     // 用户标识符指定扣款账户
		Amount:      input.TotalAmount,                                                // 订单总金额作为支付金额
		Currency:    "CNY",                                                            // 使用人民币作为支付货币类型
		Description: fmt.Sprintf("Order %s - Dapr Workflow Engine V1", input.OrderID), // 支付描述包含订单ID和版本标识
	}

	// 将支付请求对象序列化为JSON字节数组用于HTTP请求体
	reqBytes, _ := json.Marshal(paymentReq)
	// 构造Dapr服务调用的数据内容对象，包含JSON格式的请求体和MIME类型声明
	content := &client.DataContent{
		ContentType: "application/json", // 声明内容类型为JSON格式
		Data:        reqBytes,           // 支付请求的完整数据
	}
	// 通过Dapr客户端调用支付服务的/payment/process接口执行实际的支付扣款操作
	resp, err := daprClient.InvokeMethodWithContent(ctx, "payment-service", "/payment/process", "post", content)
	if err != nil {
		// 服务调用失败时更新输出状态为失败并记录错误信息
		output.Status = "failed"                              // 失败状态标识
		output.PaymentSuccess = false                         // 支付成功标志设为false
		output.Error = fmt.Sprintf("payment failed: %v", err) // 错误信息详情
		return output                                         // 返回失败结果
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
		return output                                      // 返回失败结果
	}

	// 条件判断：检查支付服务返回的状态是否表示成功（success或completed）
	if payResp.Status != "success" && payResp.Status != "completed" {
		// 支付未成功时更新输出状态为支付失败并记录实际状态
		output.Status = "payment_failed"                                 // 支付失败状态标识
		output.PaymentSuccess = false                                    // 支付成功标志设为false
		output.Error = fmt.Sprintf("payment status: %s", payResp.Status) // 实际状态信息
		return output                                                    // 返回失败结果
	}

	// 支付成功时设置成功标志并返回完成结果
	output.PaymentSuccess = true // 支付成功标志设为true表示扣款已完成
	return output                // 返回成功结果无错误
}

// executeUpdateInventoryActivityLegacy V1版本的库存更新活动实现（降级兼容专用）
// 直接在当前进程内调用库存服务扣减已售商品库存，不经过Dapr引擎调度
// 参数ctx：请求上下文，用于Dapr服务调用和指标记录
// 参数output：当前工作流输出对象（用于累加更新结果状态）
// 参数input：工作流输入参数结构体（包含订单ID、商品列表等）
// 返回值：更新后的工作流输出结构体（包含库存扣减结果或错误信息）
func executeUpdateInventoryActivityLegacy(ctx context.Context, output OrderWorkflowOutput, input OrderWorkflowInput) OrderWorkflowOutput {

	// 记录活动开始时间用于计算执行耗时和性能指标统计
	activityStart := time.Now() // 起始时间戳
	// 注册延迟恢复函数确保无论正常返回还是异常都能记录活动执行情况
	defer func() {
		success := output.Error == ""                                    // 根据错误字段是否为空判断执行是否成功
		recordActivityExecution(ctx, "UpdateInventoryActivity", success) // 记录到Prometheus指标系统
		_ = time.Since(activityStart)                                    // 计算耗时（预留用于未来的性能分析）
	}()

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
		resp, err := daprClient.InvokeMethodWithContent(ctx, "inventory-service", "/inventory/update", "post", content)
		if err != nil {
			// 服务调用失败时更新输出状态为失败并记录错误信息
			output.Status = "failed"                                       // 失败状态标识
			output.InventoryUpdated = false                                // 库存更新标志设为false
			output.Error = fmt.Sprintf("inventory update failed: %v", err) // 错误信息详情
			return output                                                  // 返回失败结果（任一商品失败即终止）
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
			return output                                      // 返回失败结果
		}

		// 条件判断：检查库存服务返回的操作结果是否成功
		if !updateResp.Success {
			// 库存更新失败时更新输出状态为失败并记录服务端错误消息
			output.Status = "failed"          // 失败状态标识
			output.InventoryUpdated = false   // 库存更新标志设为false
			output.Error = updateResp.Message // 保存服务端返回的错误消息
			return output                     // 返回失败结果（任一商品失败即终止）
		}
	} // 循环结束：所有商品均已成功完成库存扣减

	// 所有商品库存均成功扣减时设置更新完成标志并返回成功结果
	output.InventoryUpdated = true // 库存更新标志设为true表示全部扣减完成
	return output                  // 返回成功结果无错误
}

// executeSendNotificationActivityLegacy V1版本的通知发送活动实现（降级兼容专用）
// 直接在当前进程内构造通知消息并记录日志，不经过Dapr引擎调度
// 参数ctx：请求上下文，用于指标记录和日志输出
// 参数output：当前工作流输出对象（用于累加更新结果状态）
// 参数input：工作流输入参数结构体（包含订单ID、用户信息等）
// 返回值：更新后的工作流输出结构体（通知发送状态标志已设为true）
func executeSendNotificationActivityLegacy(ctx context.Context, output OrderWorkflowOutput, input OrderWorkflowInput) OrderWorkflowOutput {

	// 记录活动开始时间用于计算执行耗时和性能指标统计
	activityStart := time.Now() // 起始时间戳
	// 注册延迟恢复函数确保无论正常返回还是异常都能记录活动执行情况
	defer func() {
		success := output.Error == ""                                     // 根据错误字段是否为空判断执行是否成功
		recordActivityExecution(ctx, "SendNotificationActivity", success) // 记录到Prometheus指标系统
		_ = time.Since(activityStart)                                     // 计算耗时（预留用于未来的性能分析）
	}()

	// 构造通知消息的完整内容包含订单处理结果的所有关键信息
	notificationMsg := map[string]interface{}{
		"order_id":        input.OrderID,                                                                                      // 订单唯一标识符（关联字段）
		"user_id":         input.UserID,                                                                                       // 用户标识符（通知接收人）
		"status":          output.Status,                                                                                      // 当前订单的处理状态结果
		"total_amount":    input.TotalAmount,                                                                                  // 订单总金额（通知展示）
		"message":         fmt.Sprintf("Order %s has been processed successfully via Dapr Workflow Engine V1", input.OrderID), // 通知文本消息
		"timestamp":       time.Now().Format(time.RFC3339),                                                                    // 消息生成的时间戳（ISO8601格式）
		"workflow_engine": "dapr-workflow-engine-v1",                                                                          // 工作流引擎版本标识
		"version":         "v1",                                                                                               // 处理流程版本号
	}

	// 将通知消息对象序列化为JSON字节数组用于发布订阅事件传输
	msgBytes, _ := json.Marshal(notificationMsg)

	// 通过Dapr客户端将通知消息发布到pubsub组件的orders主题实现异步通知分发
	err := daprClient.PublishEvent(ctx, "pubsub", "orders", msgBytes)
	if err != nil {
		// 发布失败时设置通知发送标志为false表示未成功发送
		output.NotificationSent = false // 通知发送标志设为false
	} else {
		// 发布成功时设置通知发送标志为true表示已成功发送
		output.NotificationSent = true // 通知发送标志设为true表示已成功发布
	}

	return output // 返回包含通知发送状态的最终结果
}
