// Dapr多运行时云原生应用实践 - 可观测性指标收集模块
// 功能：OpenTelemetry集成、Prometheus指标导出、工作流性能监控、错误追踪统计
package main

import (
	"context"  // 上下文管理，用于指标记录的传播和超时控制
	"fmt"      // 格式化输入输出库用于错误消息构造
	"net/http" // HTTP服务端实现用于暴露Prometheus指标端点
	"time"     // 时间处理工具用于耗时计算

	"go.opentelemetry.io/otel"                         // OpenTelemetry核心API包
	"go.opentelemetry.io/otel/attribute"               // 属性标签定义包，用于指标维度标记
	"go.opentelemetry.io/otel/exporters/prometheus"    // Prometheus导出器实现
	"go.opentelemetry.io/otel/metric"                  // 指标API接口定义包
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"    // SDK指标提供者实现包
	"go.opentelemetry.io/otel/sdk/resource"            // 资源属性配置包（服务元数据）
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0" // OpenTelemetry语义约定标准常量
)

var (
	meter metric.Meter // 全局Meter实例指针，用于创建和管理所有自定义指标

	// 工作流执行次数计数器：统计工作流的启动、完成、失败等事件总数
	workflowCounter metric.Int64Counter // 整型计数器指标（单调递增）
	// 工作流执行耗时直方图：记录每个工作流从启动到完成的执行时间分布
	workflowDuration metric.Float64Histogram // 浮点型直方图指标（支持分位数计算）
	// 活动执行次数计数器：统计各活动处理器被调用的总次数
	activityCounter metric.Int64Counter // 整型计数器指标（按活动名称维度）
	// 订单金额上下行计数器：跟踪当前正在处理的订单总金额（可增可减）
	orderTotalMetric metric.Float64UpDownCounter // 浮点型上下行计数器（ Gauge类型）
	// 工作流错误次数计数器：统计工作流执行过程中发生的各类错误总数
	errorCounter metric.Int64Counter  // 整型计数器指标（按错误类型维度）
	promExporter *prometheus.Exporter // Prometheus导出器实例指针，负责格式化指标数据
)

// initTelemetry 初始化OpenTelemetry可观测性系统，配置Prometheus导出器和自定义指标
// 这是应用启动时必须调用的初始化函数，建立完整的监控指标收集和暴露能力
// 返回值：初始化过程中的错误信息（通常为nil表示成功）
func initTelemetry() error {

	// 合并默认资源属性和自定义服务元数据构造完整的资源描述对象
	res, err := resource.Merge( // 资源合并操作
		resource.Default(), // 使用OpenTelemetry提供的默认资源（包含操作系统、运行时等基础信息）
		resource.NewWithAttributes( // 创建自定义资源属性覆盖或补充默认值
			semconv.SchemaURL, // 语义约定Schema版本URL标识符
			semconv.ServiceName("order-workflow-service"), // 服务名称：订单工作流服务
			semconv.ServiceVersion("1.0.0"),               // 服务版本号：1.0.0初始版本
		),
	)
	if err != nil {
		// 资源创建失败时返回包装后的错误信息便于排查配置问题
		return fmt.Errorf("failed to create resource: %w", err) // 错误包装保留原始信息
	}

	// 创建Prometheus指标导出器实例用于将OTel格式转换为Prometheus可读格式
	var promErr error                        // 定义错误变量接收导出器创建结果
	promExporter, promErr = prometheus.New() // 创建默认配置的Prometheus导出器
	if promErr != nil {
		// 导出器创建失败时返回明确的错误提示
		return fmt.Errorf("failed to create Prometheus exporter: %w", promErr) // 错误包装
	}

	// 创建SDK Meter提供者实例并配置读取器和资源属性
	provider := sdkmetric.NewMeterProvider( // 新建MeterProvider对象
		sdkmetric.WithReader(promExporter), // 配置Prometheus作为指标数据读取器
		sdkmetric.WithResource(res),        // 关联前面创建的资源属性对象
	)
	// 将全局MeterProvider设置为刚创建的实例供后续所有代码使用
	otel.SetMeterProvider(provider) // 全局注册生效

	// 从全局MeterProvider获取专用Meter实例用于创建工作流相关的所有自定义指标
	meter = otel.Meter("order-workflow-metrics") // Meter名称标识：订单工作流指标集合

	// 定义多个错误变量分别接收每个指标的创建结果用于统一检查
	var err1, err2, err3, err4, err5 error // 分别对应5个指标的创建状态

	// ===== 创建指标1：工作流执行总次数计数器 =====
	workflowCounter, err1 = meter.Int64Counter( // 创建整型计数器
		"workflow_total", // 指标名称：工作流执行总数
		metric.WithDescription("Total number of workflows executed"), // 指标描述说明
		metric.WithUnit("{workflows}"),                               // 指标单位：工作流次数
	)

	// ===== 创建指标2：工作流执行耗时直方图 =====
	workflowDuration, err2 = meter.Float64Histogram( // 创建浮点型直方图
		"workflow_duration_seconds",                                      // 指标名称：工作流执行耗时（秒）
		metric.WithDescription("Workflow execution duration in seconds"), // 指标描述说明
		metric.WithUnit("s"),                                             // 指标单位：秒（second）
	)

	// ===== 创建指标3：活动执行总次数计数器 =====
	activityCounter, err3 = meter.Int64Counter( // 创建整型计数器
		"activity_total", // 指标名称：活动执行总数
		metric.WithDescription("Total number of activities executed"), // 指标描述说明
		metric.WithUnit("{activities}"),                               // 指标单位：活动次数
	)

	// ===== 创建指标4：订单总金额上下行计数器 =====
	orderTotalMetric, err4 = meter.Float64UpDownCounter( // 创建浮点型上下行计数器
		"order_total_amount", // 指标名称：订单总金额
		metric.WithDescription("Total order amount being processed"), // 指标描述说明
		metric.WithUnit("By"), // 指标单位：人民币元（Yuan）
	)

	// ===== 创建指标5：工作流错误总次数计数器 =====
	errorCounter, err5 = meter.Int64Counter( // 创建整型计数器
		"workflow_errors_total",                                   // 指标名称：工作流错误总数
		metric.WithDescription("Total number of workflow errors"), // 指标描述说明
		metric.WithUnit("{errors}"),                               // 指标单位：错误次数
	)

	// 统一检查所有指标的创建结果（实际生产环境应分别处理每个错误）
	if err1 != nil || err2 != nil || err3 != nil || err4 != nil || err5 != nil { // 任一指标创建失败
		// 预留的错误处理逻辑位置（可根据需要添加日志记录或返回错误）
	}

	// 启动后台HTTP服务暴露Prometheus指标端点供Prometheus服务器抓取
	go func() { // 异步启动不阻塞主流程
		// 创建HTTP多路复用器用于路由不同的请求路径
		mux := http.NewServeMux() // 新建路由器实例
		// 注册/metrics路径的处理函数响应Prometheus抓取请求
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) { // 处理函数闭包
			// 设置响应头声明内容类型为Prometheus文本格式
			w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8") // Prometheus标准格式
			w.WriteHeader(http.StatusOK)                                               // 设置HTTP状态码为200成功
			// 写入指标数据的头部注释说明（实际数据由promExporter自动填充）
			w.Write([]byte("# Prometheus Metrics for Order Workflow Service\n")) // 注释头部
		})
		// 在9090端口启动HTTP监听服务暴露指标端点（与main.go中的health端点配置一致）
		http.ListenAndServe(":9090", mux) // 阻塞式启动HTTP服务（在后台goroutine中运行）
	}() // 立即调用匿名函数启动异步协程

	return nil
}

// recordWorkflowStart 记录工作流启动事件到Prometheus指标系统
// 在工作流开始执行时调用此函数记录启动计数和属性标签用于监控分析
// 参数ctx：请求上下文，用于指标记录的传播和超时控制
// 参数workflowName：工作流名称标识符（如V1或V2版本的名称）
// 返回值：传入的上下文对象（便于链式调用）
func recordWorkflowStart(ctx context.Context, workflowName string) context.Context {

	// 检查工作流计数器指标是否已正确初始化（防止空指针异常）
	if workflowCounter != nil { // 指标非空判断
		// 调用Add方法递增工作流总次数计数器并附加维度标签
		workflowCounter.Add(ctx, 1, metric.WithAttributes( // 计数值+1
			attribute.String("workflow_name", workflowName), // 工作流名称
			attribute.String("status", "started"),           // 状态为已启动
		))
	}

	return ctx // 返回原始上下文供调用方继续使用
}

// recordWorkflowComplete 记录工作流完成事件到Prometheus指标系统（包含耗时统计和错误追踪）
// 在工作流执行结束时调用此函数记录完成状态、执行时长和可能的错误信息
// 参数ctx：请求上下文，用于指标记录的传播
// 参数workflowName：工作流名称标识符
// 参数status：最终完成状态（completed/failed/error等）
// 参数duration：工作流从开始到结束的总执行耗时
func recordWorkflowComplete(ctx context.Context, workflowName string, status string, duration time.Duration) {

	// 检查工作流耗时直方图指标是否已初始化并记录执行时间分布数据
	if workflowDuration != nil { // 指标非空判断
		// 调用Record方法记录本次执行的耗时值（秒为单位）并附加维度标签
		workflowDuration.Record(ctx, duration.Seconds(), metric.WithAttributes( // 记录耗时样本
			attribute.String("workflow_name", workflowName), // 工作流名称
			attribute.String("status", status),              // 最终完成状态
		))
	}

	// 检查工作流计数器指标是否已初始化并递增完成次数统计
	if workflowCounter != nil { // 指标非空判断
		// 调用Add方法递增完成计数器并附加维度标签区分不同状态
		workflowCounter.Add(ctx, 1, metric.WithAttributes( // 完成计数+1
			attribute.String("workflow_name", workflowName), // 工作流名称
			attribute.String("status", status),              // 完成状态
		))
	}

	// 条件判断：仅在失败或错误状态下额外记录错误计数指标用于告警
	if status == "failed" || status == "error" { // 错误状态匹配
		// 检查错误计数器指标是否已初始化
		if errorCounter != nil { // 指标非空判断
			// 调用Add方法递增错误计数器并附加错误类型标签
			errorCounter.Add(ctx, 1, metric.WithAttributes( // 错误计数+1
				attribute.String("workflow_name", workflowName), // 关联的工作流名称
				attribute.String("error_type", status),          // 错误类型标识
			))
		}
	}
}

// recordActivityExecution 记录活动处理器执行事件到Prometheus指标系统
// 在每个活动处理器开始或结束时调用此函数记录执行结果状态用于性能监控
// 参数ctx：请求上下文，用于指标记录的传播
// 参数activityName：活动处理器名称标识符（如ValidateInventoryActivity等）
// 参数success：执行是否成功的标志位（true表示成功，false表示失败）
func recordActivityExecution(ctx context.Context, activityName string, success bool) {
	// 检查活动计数器指标是否已正确初始化（防止空指针异常）
	if activityCounter == nil { // 指标未初始化时直接返回避免错误
		return // 安全退出
	}
	// 定义状态字符串变量根据成功标志确定实际的状态值
	status := "success" // 默认设为成功状态
	if !success {       // 成功标志为false的情况
		status = "failure" // 状态改为失败
	}

	// 调用Add方法递增活动执行总数计数器并附加维度标签
	activityCounter.Add(ctx, 1, metric.WithAttributes( // 执行计数+1
		attribute.String("activity_name", activityName), // 活动名称标识符
		attribute.String("status", status),              // 执行结果状态
	))
}

// recordOrderAmount 记录订单金额到Prometheus上下行计数器指标系统
// 在创建订单时调用此函数跟踪当前正在处理的订单总金额用于业务监控
// 参数ctx：请求上下文，用于指标记录的传播
// 参数orderID：订单唯一标识符作为维度标签用于查询特定订单的数据
// 参数amount：订单总金额数值（单位：元/CNY），正数表示增加负数表示减少
func recordOrderAmount(ctx context.Context, orderID string, amount float64) {
	// 检查订单金额上下行计数器指标是否已正确初始化（防止空指针异常）
	if orderTotalMetric == nil { // 指标未初始化时直接返回避免错误
		return // 安全退出
	}
	// 调用Add方法更新订单金额计数器的当前值（可增可减的Gauge类型）
	orderTotalMetric.Add(ctx, amount, metric.WithAttributes( // 更新金额值
		attribute.String("order_id", orderID), // 订单唯一标识符
	))
}
