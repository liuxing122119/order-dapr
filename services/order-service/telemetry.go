// 遥测监控包 - 提供OpenTelemetry和Prometheus指标监控功能
// 功能：工作流执行指标收集、活动执行统计、订单金额追踪、错误计数
package main

import (
	"context"  // 上下文控制库
	"fmt"      // 格式化输出库
	"net/http" // HTTP服务库（用于暴露Prometheus指标端点）
	"time"     // 时间处理库

	"go.opentelemetry.io/otel"                         // OpenTelemetry核心API
	"go.opentelemetry.io/otel/attribute"               // 属性标签库
	"go.opentelemetry.io/otel/exporters/prometheus"    // Prometheus导出器
	"go.opentelemetry.io/otel/metric"                  // 指标API
	"go.opentelemetry.io/otel/resource"                // 资源信息库
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"    // 指标SDK实现
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0" // 语义约定标准
)

// 全局变量定义区域 - 遥测相关的全局指标对象
var (
	meter metric.Meter // meter 计量器对象 - 用于创建各种指标

	// workflowCounter 工作流计数器 - 统计工作流执行次数
	workflowCounter metric.Int64Counter

	// workflowDuration 工作流持续时间直方图 - 记录工作流执行耗时分布
	workflowDuration metric.Float64Histogram

	// activityCounter 活动计数器 - 统计活动（Activity）执行次数
	activityCounter metric.Int64Counter

	// orderTotalMetric 订单总金额上下行计数器 - 追踪正在处理的订单金额总量
	orderTotalMetric metric.Float64UpDownCounter

	// errorCounter 错误计数器 - 统计工作流执行中的错误次数
	errorCounter metric.Int64Counter

	// promExporter Prometheus导出器实例 - 用于暴露指标数据
	promExporter *prometheus.Exporter
)

// initTelemetry 初始化遥测系统函数
// 功能：配置OpenTelemetry资源、创建Prometheus导出器、注册所有监控指标、启动HTTP指标服务器
// 返回值：
//   - error - 初始化错误（nil表示成功）
func initTelemetry() error {
	// resource.Merge 合并资源信息
	// 将默认资源与自定义服务资源合并
	res, err := resource.Merge(
		resource.Default(), // 默认资源信息
		resource.NewWithAttributes( // 创建带自定义属性的资源
			semconv.SchemaURL, // 语义约定Schema URL
			semconv.ServiceName("order-workflow-service"), // 服务名称属性
			semconv.ServiceVersion("1.0.0"),               // 服务版本属性
		),
	)
	if err != nil {
		return fmt.Errorf("failed to create resource: %w", err) // 资源创建失败返回错误
	}

	// prometheus.New 创建Prometheus导出器实例
	var promErr error
	promExporter, promErr = prometheus.New()
	if promErr != nil {
		return fmt.Errorf("failed to create Prometheus exporter: %w", promErr) // 导出器创建失败返回错误
	}

	// sdkmetric.NewMeterProvider 创建计量器提供者
	// 配置Prometheus读取器和资源信息
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(promExporter), // 设置Prometheus读取器
		sdkmetric.WithResource(res),        // 设置资源信息
	)
	// otel.SetMeterProvider 设置全局计量器提供者
	otel.SetMeterProvider(provider)

	// otel.Meter 获取或创建指定名称的计量器
	meter = otel.Meter("order-workflow-metrics") // 计量器名称：order-workflow-metrics

	// 声明多个错误变量用于接收各指标的创建结果
	var err1, err2, err3, err4, err5 error

	// meter.Int64Counter 创建整数计数器指标
	// workflow_total - 工作流执行总数计数器
	workflowCounter, err1 = meter.Int64Counter(
		"workflow_total", // 指标名称
		metric.WithDescription("Total number of workflows executed"), // 指标描述
		metric.WithUnit("{workflows}"),                               // 指标单位
	)

	// meter.Float64Histogram 创建浮点数直方图指标
	// workflow_duration_seconds - 工作流执行持续时间直方图
	workflowDuration, err2 = meter.Float64Histogram(
		"workflow_duration_seconds",
		metric.WithDescription("Workflow execution duration in seconds"),
		metric.WithUnit("s"), // 单位：秒
	)

	// activity_total - 活动执行总数计数器
	activityCounter, err3 = meter.Int64Counter(
		"activity_total",
		metric.WithDescription("Total number of activities executed"),
		metric.WithUnit("{activities}"),
	)

	// meter.Float64UpDownCounter 创建浮点数上下行计数器
	// order_total_amount - 正在处理的订单总金额（可增可减）
	orderTotalMetric, err4 = meter.Float64UpDownCounter(
		"order_total_amount",
		metric.WithDescription("Total order amount being processed"),
		metric.WithUnit("By"), // 单位：元
	)

	// workflow_errors_total - 工作流错误总数计数器
	errorCounter, err5 = meter.Int64Counter(
		"workflow_errors_total",
		metric.WithDescription("Total number of workflow errors"),
		metric.WithUnit("{errors}"),
	)

	// if判断 - 检查是否有任何指标创建失败
	if err1 != nil || err2 != nil || err3 != nil || err4 != nil || err5 != nil {
		// 可以在此处添加错误处理逻辑（当前为空实现）
	}

	// go关键字 - 启动goroutine异步运行HTTP指标服务器
	go func() {
		// http.NewServeMux 创建HTTP请求多路复用器
		mux := http.NewServeMux()
		// HandleFunc 注册/metrics路径的处理函数
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
			// w.Header.Set 设置响应头
			w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
			w.WriteHeader(http.StatusOK)                                         // WriteHeader设置HTTP状态码200
			w.Write([]byte("# Prometheus Metrics for Order Workflow Service\n")) // 写入响应内容
		})
		// http.ListenAndServe 启动HTTP服务器监听9090端口
		http.ListenAndServe(":9090", mux) // Prometheus默认抓取端口
	}()

	return nil // 初始化成功返回nil
}

// recordWorkflowStart 记录工作流开始函数
// 功能：在工作流开始时记录启动事件并更新计数器
// 参数：
//   - ctx context.Context - 上下文对象
//   - workflowName string - 工作流名称标识
//
// 返回值：
//   - context.Context - 返回传入的上下文（可用于链式调用）
func recordWorkflowStart(ctx context.Context, workflowName string) context.Context {
	// if判断 - 确保计数器已初始化
	if workflowCounter != nil {
		// Add方法 - 计数器加1操作
		workflowCounter.Add(ctx, 1, metric.WithAttributes( // WithAttributes添加维度标签
			attribute.String("workflow_name", workflowName), // 标签：工作流名称
			attribute.String("status", "started"),           // 标签：状态（已启动）
		))
	}

	return ctx // return返回上下文
}

// recordWorkflowComplete 记录工作流完成函数
// 功能：在工作流完成时记录持续时间、最终状态，并在失败时记录错误
// 参数：
//   - ctx context.Context - 上下文对象
//   - workflowName string - 工作流名称
//   - status string - 完成状态（completed/failed/error等）
//   - duration time.Duration - 执行持续时长
func recordWorkflowComplete(ctx context.Context, workflowName string, status string, duration time.Duration) {
	// if判断 - 记录工作流持续时间到直方图
	if workflowDuration != nil {
		// Record方法 - 记录一个观测值到直方图
		workflowDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(
			attribute.String("workflow_name", workflowName), // 工作流名称标签
			attribute.String("status", status),              // 完成状态标签
		))
		// duration.Seconds() - 将Duration转换为秒数的float64值
	}

	// if判断 - 记录完成状态到计数器
	if workflowCounter != nil {
		workflowCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("workflow_name", workflowName),
			attribute.String("status", status),
		))
	}

	// if判断 - 如果状态是失败或错误，记录到错误计数器
	if status == "failed" || status == "error" {
		if errorCounter != nil {
			errorCounter.Add(ctx, 1, metric.WithAttributes(
				attribute.String("workflow_name", workflowName),
				attribute.String("error_type", status), // 错误类型标签
			))
		}
	}
}

// recordActivityExecution 记录活动执行函数
// 功能：在活动（Activity）执行完成后记录其成功或失败状态
// 参数：
//   - ctx context.Context - 上下文对象
//   - activityName string - 活动名称标识
//   - success bool - 是否执行成功
func recordActivityExecution(ctx context.Context, activityName string, success bool) {
	// if判断 - 检查活动计数器是否已初始化
	if activityCounter == nil {
		return // 未初始化则直接返回
	}
	// 声明状态字符串变量
	status := "success" // 默认状态设为成功
	// if判断 - 根据success参数确定实际状态
	if !success {
		status = "failure" // 失败时设置为"failure"
	}

	// Add方法 - 记录活动执行到计数器
	activityCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("activity_name", activityName), // 活动名称标签
		attribute.String("status", status),              // 执行状态标签
	))
}

// recordOrderAmount 记录订单金额函数
// 功能：追踪正在处理的订单金额（用于计算系统负载等业务指标）
// 参数：
//   - ctx context.Context - 上下文对象
//   - orderID string - 订单ID（作为标签）
//   - amount float64 - 订单金额（元）
func recordOrderAmount(ctx context.Context, orderID string, amount float64) {
	// if判断 - 检查金额计数器是否已初始化
	if orderTotalMetric == nil {
		return // 未初始化则直接返回
	}
	// Add方法 - 将订单金额添加到上下行计数器（后续可用Subtract减去）
	orderTotalMetric.Add(ctx, amount, metric.WithAttributes(
		attribute.String("order_id", orderID), // 订单ID标签
	))
}
