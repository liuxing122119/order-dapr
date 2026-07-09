package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

var (
	meter metric.Meter

	workflowCounter  metric.Int64Counter
	workflowDuration metric.Float64Histogram
	activityCounter  metric.Int64Counter
	orderTotalMetric metric.Float64UpDownCounter
	errorCounter     metric.Int64Counter
	promExporter     *prometheus.Exporter
)

func initTelemetry() error {

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("order-workflow-service"),
			semconv.ServiceVersion("1.0.0"),
		),
	)
	if err != nil {
		return fmt.Errorf("failed to create resource: %w", err)
	}

	var promErr error
	promExporter, promErr = prometheus.New()
	if promErr != nil {
		return fmt.Errorf("failed to create Prometheus exporter: %w", promErr)
	}

	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(promExporter),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(provider)

	meter = otel.Meter("order-workflow-metrics")

	var err1, err2, err3, err4, err5 error

	workflowCounter, err1 = meter.Int64Counter(
		"workflow_total",
		metric.WithDescription("Total number of workflows executed"),
		metric.WithUnit("{workflows}"),
	)

	workflowDuration, err2 = meter.Float64Histogram(
		"workflow_duration_seconds",
		metric.WithDescription("Workflow execution duration in seconds"),
		metric.WithUnit("s"),
	)

	activityCounter, err3 = meter.Int64Counter(
		"activity_total",
		metric.WithDescription("Total number of activities executed"),
		metric.WithUnit("{activities}"),
	)

	orderTotalMetric, err4 = meter.Float64UpDownCounter(
		"order_total_amount",
		metric.WithDescription("Total order amount being processed"),
		metric.WithUnit("By"),
	)

	errorCounter, err5 = meter.Int64Counter(
		"workflow_errors_total",
		metric.WithDescription("Total number of workflow errors"),
		metric.WithUnit("{errors}"),
	)

	if err1 != nil || err2 != nil || err3 != nil || err4 != nil || err5 != nil {
	}

	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("# Prometheus Metrics for Order Workflow Service\n"))
		})
		http.ListenAndServe(":9090", mux)
	}()

	return nil
}

func recordWorkflowStart(ctx context.Context, workflowName string) context.Context {

	if workflowCounter != nil {
		workflowCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("workflow_name", workflowName),
			attribute.String("status", "started"),
		))
	}

	return ctx
}

func recordWorkflowComplete(ctx context.Context, workflowName string, status string, duration time.Duration) {

	if workflowDuration != nil {
		workflowDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(
			attribute.String("workflow_name", workflowName),
			attribute.String("status", status),
		))
	}

	if workflowCounter != nil {
		workflowCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("workflow_name", workflowName),
			attribute.String("status", status),
		))
	}

	if status == "failed" || status == "error" {
		if errorCounter != nil {
			errorCounter.Add(ctx, 1, metric.WithAttributes(
				attribute.String("workflow_name", workflowName),
				attribute.String("error_type", status),
			))
		}
	}
}

func recordActivityExecution(ctx context.Context, activityName string, success bool) {
	if activityCounter == nil {
		return
	}
	status := "success"
	if !success {
		status = "failure"
	}

	activityCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("activity_name", activityName),
		attribute.String("status", status),
	))
}

func recordOrderAmount(ctx context.Context, orderID string, amount float64) {
	if orderTotalMetric == nil {
		return
	}
	orderTotalMetric.Add(ctx, amount, metric.WithAttributes(
		attribute.String("order_id", orderID),
	))
}
