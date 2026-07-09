# Dapr订单处理系统

基于Dapr的云原生微服务订单处理系统，实现服务调用、状态管理、发布订阅、工作流编排等核心构建块。

## 技术栈

- **运行时**: Dapr 1.18.1
- **语言**: Golang 1.25.0
- **SDK**: Dapr Go SDK 1.14.0
- **存储**: Redis（状态存储+消息总线）+ PostgreSQL（业务数据）
- **可观测性**: OpenTelemetry + Jaeger + Prometheus
- **部署**: Docker Compose

## 项目结构

```
order-dapr/
├── dapr/                    # Dapr配置
│   ├── config.yaml          # 全局配置
│   └── components/          # 组件配置
├── services/                # 微服务
│   ├── user-service/        # 用户服务
│   ├── order-service/       # 订单服务（核心）
│   ├── payment-service/     # 支付服务
│   └── inventory-service/   # 库存服务
├── db/                      # 数据库连接库
├── dapr.yaml                # Multi-App Run配置
├── docker-compose.yml       # 中间件编排
└── init-db.sql              # 数据库初始化
```

## 核心功能

### 服务间调用（Service Invocation）

使用Dapr Go SDK的 `InvokeMethod` API实现服务间通信，支持弹性策略。

**弹性策略配置**：
- **重试策略**：指数退避（初始1秒，最大5秒），最多3次
- **超时策略**：5秒超时限制
- **熔断策略**：连续失败5次触发，10秒熔断

---

### 状态管理（State Management）

使用Redis作为状态存储组件，通过Dapr API管理分布式状态。

**配置文件**：`dapr/components/statestore.yaml`

---

### 发布订阅（Publish & Subscribe）

使用Redis作为消息总线，实现事件驱动通信。

**配置文件**：`dapr/components/pubsub.yaml`

---

### 工作流编排（Workflow）

使用Dapr工作流引擎编排订单处理的完整业务流程。

**工作流架构**：
```
OrderProcessingWorkflow (V2版本):

步骤1: ValidateInventoryActivity
├─ 调用 inventory-service/check
└─ 验证库存充足性

步骤2: ProcessPaymentActivity
├─ 调用 payment-service/process
└─ 处理支付扣款

步骤3: UpdateInventoryActivity
├─ 调用 inventory-service/update
└─ 扣减库存数量

步骤4: SendNotificationActivity
├─ 发送订单状态通知
└─ 更新订单状态为completed
```

---

### 可观测性集成

集成OpenTelemetry + Jaeger + Prometheus实现完整可观测性。

**配置文件**：`dapr/config.yaml`

---

## 快速启动

### 前置条件

- **Docker Desktop**（运行中）
- **Dapr CLI**（已初始化：`dapr init`）
- **Golang 1.25+**

### 启动步骤

#### 1. 启动中间件（PostgreSQL + Jaeger）

```bash
docker-compose up -d
```

**验证中间件状态**：
```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

**预期输出**：
```
NAMES            STATUS                    PORTS
dapr_postgres    Up X seconds (healthy)    0.0.0.0:5432->5432/tcp
dapr_jaeger      Up X seconds              0.0.0.0:16686->16686/tcp
dapr_redis       Up X hours                0.0.0.0:6379->6379/tcp
```

---

#### 2. 编译所有服务

```bash
# 编译用户服务
go build -o services/user-service/user-service.exe services/user-service/main.go

# 编译订单服务
go build -o services/order-service/order-service.exe services/order-service/main.go

# 编译支付服务
go build -o services/payment-service/payment-service.exe services/payment-service/main.go

# 编译库存服务
go build -o services/inventory-service/inventory-service.exe services/inventory-service/main.go
```

---

#### 3. 一键启动所有微服务

```bash
dapr run -f .
```

**预期输出**：
```
Validating config and starting app "user-service"
== APP - user-service == [SUCCESS] Connected to Dapr gRPC on port 56606

Validating config and starting app "order-service"
== APP - order-service == [SUCCESS] Connected to Dapr gRPC on port 56643

Validating config and starting app "payment-service"
== APP - payment-service == [SUCCESS] Connected to Darp gRPC on port 56683

Validating config and starting app "inventory-service"
== APP - inventory-service == [SUCCESS] Connected to Darp gRPC on port 56732
```

---

### 停止系统

#### 方式1：优雅停止（推荐）

```bash
Ctrl+C  # 在dapr run终端按此键
```

#### 方式2：强制停止（如果Ctrl+C无效）

```bash
taskkill /F /IM user-service.exe /T
taskkill /F /IM order-service.exe /T
taskkill /F /IM payment-service.exe /T
taskkill /F /IM inventory-service.exe /T
taskkill /F /IM daprd.exe /T
```

#### 停止中间件容器（可选）

```bash
docker-compose down
```

---

## 测试验证

### 健康检查

```bash
# 检查用户服务健康状态
curl http://localhost:5001/health

# 检查订单服务健康状态
curl http://localhost:5002/health

# 检查支付服务健康状态
curl http://localhost:5003/health

# 检查库存服务健康状态
curl http://localhost:5004/health
```

**预期输出**：
```json
{"service":"user-service","status":"healthy","features":["dapr-sidecar","service-invocation","state-management","user-validation-api","graceful-shutdown"],"timestamp":"2026-07-09T06:45:34Z"}
```

---

### 创建订单

```bash
curl -X POST http://localhost:5002/order/create \
  -H "Content-Type: application/json" \
  -d '{"userId":"user001","items":[{"productId":"prod001","productName":"Test Product","quantity":1,"price":99.99}]}'
```

**预期响应**：
```json
{
  "orderId": "50af3c13-afa9-4be6-95a1-4c62334fd545",
  "status": "processing",
  "instanceId": "1783579900391885900-68e7",
  "message": "Order created and Dapr Workflow Engine started with version management"
}
```

---

### 查询订单

```bash
curl -X POST http://localhost:5002/order/get \
  -H "Content-Type: application/json" \
  -d '{"orderId":"YOUR_ORDER_ID"}'
```

**注意**：将 `YOUR_ORDER_ID` 替换为创建订单时返回的 `orderId`。

---

### 分布式追踪

访问 Jaeger UI：http://localhost:16686

**操作步骤**：
1. 选择 Service：`order-service`
2. 选择 Operation：`POST /order/create`
3. 点击 Find Traces 按钮
4. 查看完整的调用链路图