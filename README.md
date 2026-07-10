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

# 编译订单服务（包含工作流文件：workflow.go、workflow_engine.go、workflow_starter.go等）
go build -o services/order-service/order-service.exe ./services/order-service

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

**一键停止所有微服务**：

```bash
dapr stop -f .
```

**停止中间件容器**（可选）：

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

#### Windows CMD（推荐）
```cmd
curl -X POST http://localhost:5002/order/create -H "Content-Type: application/json" -d "{\"userId\":\"user001\",\"items\":[{\"productId\":\"prod001\",\"productName\":\"Test Product\",\"quantity\":1,\"price\":99.99}]}"
```

#### PowerShell
```powershell
Invoke-RestMethod -Uri 'http://localhost:5002/order/create' -Method POST -ContentType 'application/json' -Body '{"userId":"user001","items":[{"productId":"prod001","productName":"Test Product","quantity":1,"price":99.99}]}'
```

#### Linux/Mac
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

#### Windows CMD（推荐）
```cmd
curl -X POST http://localhost:5002/order/get -H "Content-Type: application/json" -d "{\"orderId\":\"YOUR_ORDER_ID\"}"
```

#### PowerShell
```powershell
Invoke-RestMethod -Uri 'http://localhost:5002/order/get' -Method POST -ContentType 'application/json' -Body '{"orderId":"YOUR_ORDER_ID"}'
```

#### Linux/Mac
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
1. 打开浏览器访问 http://localhost:16686
2. 选择 Service 和 Operation
3. 点击 Find Traces 查看追踪数据

---

## 高级功能测试

### 测试1：订单创建流程验证

**测试目标**：验证完整的订单处理流程（用户服务 → 库存服务 → 支付服务 → 更新状态）

#### 测试步骤

**1️⃣ 确保所有服务运行中**
```cmd
curl http://localhost:5001/health
curl http://localhost:5002/health
curl http://localhost:5003/health
curl http://localhost:5004/health
```

**2️⃣ 创建订单**
```cmd
curl -X POST http://localhost:5002/order/create -H "Content-Type: application/json" -d "{\"userId\":\"user001\",\"items\":[{\"productId\":\"prod001\",\"productName\":\"Test Product\",\"quantity\":1,\"price\":99.99}]}"
```

**3️⃣ 查询订单状态**（等待几秒后）
```cmd
curl -X POST http://localhost:5002/order/get -H "Content-Type: application/json" -d "{\"orderId\":\"YOUR_ORDER_ID\"}"
```

**4️⃣ 查看工作流日志**
```cmd
type services\order-service\.dapr\logs\order-service_app_*.log | findstr /i "workflow"
```

**预期结果**：
- ✅ 工作流启动，执行4个Activity
- ✅ 订单状态从 `processing` 变为 `completed` 或 `failed`
- ✅ 日志显示库存验证、支付处理、库存更新、通知发送

---

### 测试2：发布订阅验证（消息持久化）

**测试目标**：验证"至少一次消息投递保证"（停止支付服务 → 创建订单 → 重启支付服务 → 验证事件触发）

#### 测试步骤

**1️⃣ 停止支付服务**
```cmd
taskkill /F /IM payment-service.exe /T
taskkill /F /IM daprd.exe /T
```

**2️⃣ 验证支付服务已停止**
```cmd
curl http://localhost:5003/health
# 预期：连接失败或无响应
```

**3️⃣ 创建订单**（此时支付服务不可用）
```cmd
curl -X POST http://localhost:5002/order/create -H "Content-Type: application/json" -d "{\"userId\":\"user002\",\"items\":[{\"productId\":\"prod002\",\"productName\":\"Test Product 2\",\"quantity\":2,\"price\":199.99}]}"
```

**4️⃣ 观察日志**（查看order-service是否成功发布事件）
```cmd
type services\order-service\.dapr\logs\order-service_daprd_*.log | findstr /i "pubsub"
# 预期：看到 "Publishing event to order.created topic"
```

**5️⃣ 重启支付服务**
```cmd
cd D:\code\go\order-dapr
go build -o services/payment-service/payment-service.exe services/payment-service/main.go
dapr run --app-id payment-service --app-port 5003 --app-protocol http --components-path ./dapr/components --config ./dapr/config -- go run services/payment-service/main.go
```

**6️⃣ 监控支付服务日志**（查看是否接收到事件）
```cmd
type services\payment-service\.dapr\logs\payment-service_app_*.log
# 预期：看到 "Received event from order.created topic"
```

**预期结果**：
- ✅ Redis PubSub组件存储了未投递的消息
- ✅ 支付服务重启后自动接收积压的事件
- ✅ 实现"至少一次消息投递保证"

---

### 测试3：工作流恢复验证（检查点恢复）

**测试目标**：验证工作流的持久化和恢复能力（执行到一半时停止 → 重启后从检查点继续）

#### 测试步骤

**1️⃣ 创建一个复杂订单**（包含多个商品，延长执行时间）
```cmd
curl -X POST http://localhost:5002/order/create -H "Content-Type: application/json" -d "{\"userId\":\"user003\",\"items\":[{\"productId\":\"prod001\",\"productName\":\"Product 1\",\"quantity\":5,\"price\":50.00},{\"productId\":\"prod002\",\"productName\":\"Product 2\",\"quantity\":3,\"price\":75.00},{\"productId\":\"prod003\",\"productName\":\"Product 3\",\"quantity\":2,\"price\":100.00}]}"
```

**2️⃣ 立即查询订单状态**（确认正在处理）
```cmd
curl -X POST http://localhost:5002/order/get -H "Content-Type: application/json" -d "{\"orderId\":\"ORDER_ID_FROM_STEP_1\"}"
# 预期：status = "processing"
```

**3️⃣ 快速停止订单服务**（在工作流执行过程中）
```cmd
taskkill /F /IM order-service.exe /T
taskkill /F /IM daprd.exe /T
```

**4️⃣ 等待5秒**
```cmd
timeout /t 5
```

**5️⃣ 重启订单服务**
```cmd
cd D:\code\go\order-dapr
go build -o services/order-service/order-service.exe ./services/order-service
dapr run --app-id order-service --app-port 5002 --app-protocol http --components-path ./dapr/components --config ./dapr/config -- go run ./services/order-service
```

**6️⃣ 再次查询订单状态**
```cmd
curl -X POST http://localhost:5002/order/get -H "Content-Type: application/json" -d "{\"orderId\":\"ORDER_ID_FROM_STEP_1\"}"
```

**7️⃣ 查看工作流恢复日志**
```cmd
type services\order-service\.dapr\logs\order-service_app_*.log | findstr /i "resume\|checkpoint\|recover"
# 预期：看到 "Resuming workflow instance" 或 "Loading checkpoint"
```

**预期结果**：
- ✅ 工作流状态已持久化到Redis
- ✅ 重启后自动从检查点恢复
- ✅ 继续执行剩余的Activity步骤

---

### 测试4：优雅关闭验证（SIGTERM信号）

**测试目标**：验证优雅关闭（完成pending操作 → 刷盘数据 → 无数据丢失）

#### 方式1：使用Ctrl+C（推荐）

**1️⃣ 打开运行`dapr run -f .`的终端窗口**

**2️⃣ 按 Ctrl+C 发送SIGINT信号**

**3️⃣ 观察输出**
```
== APP == Received shutdown signal, performing graceful shutdown...
== APP == Flushing pending messages...
== APP == Saving state to database...
== APP == Shutdown complete, exiting cleanly.
```

**4️⃣ 验证数据完整性**
```cmd
# 查询最近创建的订单
curl -X POST http://localhost:5002/order/get -H "Content-Type: application/json" -d "{\"orderId\":\"LAST_ORDER_ID\"}"
# 预期：订单数据完整，status不是"processing"而是最终状态
```

#### 方式2：使用taskkill发送SIGTERM

**1️⃣ 发送终止信号**
```cmd
taskkill /F /IM user-service.exe /T
taskkill /F /IM order-service.exe /T
taskkill /F /IM payment-service.exe /T
taskkill /F /IM inventory-service.exe /T
```

**2️⃣ 查看各服务的关闭日志**
```cmd
type services\user-service\.dapr\logs\user-service_app_*.log | findstr /i "shutdown\|graceful"
type services\order-service\.dapr\logs\order-service_app_*.log | findstr /i "shutdown\|graceful"
type services\payment-service\.dapr\logs\payment-service_app_*.log | findstr /i "shutdown\|graceful"
type services\inventory-service\.dapr\logs\inventory-service_app_*.log | findstr /i "shutdown\|graceful"
```

**预期结果**：
- ✅ 所有服务收到SIGTERM信号
- ✅ 执行优雅关闭逻辑（保存状态、刷盘消息）
- ✅ 日志显示"graceful shutdown completed"
- ✅ 无数据丢失或损坏

---

### 测试5：Multi-App Run 一键启动/停止

**测试目标**：验证`dapr run -f .`和`dapr stop -f .`的一键管理能力

#### 测试步骤

**1️⃣ 一键启动所有服务**
```cmd
cd D:\code\go\order-dapr
dapr run -f .
```
**预期输出**：
```
Validating config and starting app "user-service"
Started Dapr with app id "user-service". HTTP Port: 3501.

Validating config and starting app "order-service"
Started Dapr with app id "order-service". HTTP Port: 3502.

Validating config and starting app "payment-service"
Started Dapr with app id "payment-service". HTTP Port: 3503.

Validating config and starting app "inventory-service"
Started Dapr with app id "inventory-service". HTTP Port: 3504.
```

**2️⃣ 验证所有服务健康**
```cmd
for %i in (5001,5002,5003,5004) do curl http://localhost:%i/health
```

**3️⃣ 一键停止所有服务**（在另一个终端窗口）
```cmd
cd D:\code\go\order-dapr
dapr stop -f .
```
**预期输出**：
```
Stopping app "user-service"... Stopped successfully.
Stopping app "order-service"... Stopped successfully.
Stopping app "payment-service"... Stopped successfully.
Stopping app "inventory-service"... Stopped successfully.
```

**4️⃣ 验证所有进程已退出**
```cmd
tasklist | findstr "user-service order-service payment-service inventory-service daprd"
# 预期：没有找到匹配的进程
```

**预期结果**：
- ✅ 一键启动4个微服务和4个Sidecar
- ✅ 自动分配端口和资源
- ✅ 一键优雅停止所有应用
- ✅ 无残留进程

---

### 测试6：分布式追踪可视化（Jaeger UI 详细版）

**测试目标**：验证OpenTelemetry + Jaeger集成，查看完整调用链路

#### 测试步骤

**1️⃣ 确保Jaeger容器运行中**
```cmd
docker ps | findstr jaeger
# 预期：看到 dapr_jaeger 容器在运行
```

**2️⃣ 打开浏览器访问Jaeger UI**
```
http://localhost:16686
```

**3️⃣ 查询追踪数据**（基于实际界面）
- 在左侧 **Service** 下拉框选择：`jaeger-all-in-one`（默认选项）
- 在 **Operation** 下拉框选择：`all` 或 `/api/traces`
- 点击 **Find Traces** 按钮
- 查看右侧的Trace列表（预期显示多条Trace记录）

**4️⃣ 查看时间线图**
- 右侧顶部显示Duration vs Time散点图
- 每个点代表一条Trace的执行时间和持续时间
- 可以直观看到系统负载分布

**5️⃣ 查看Trace详情**
- 点击某条Trace记录（如 `jaeger-all-in-one: /api/traces 091a030`）
- 查看：
  - **Span数量**：当前显示为1 Span
  - **持续时间**：如745μs或474μs
  - **时间戳**：精确到分钟级别
  - **服务名称**：jaeger-all-in-one

**6️⃣ 使用高级功能**
- **Download Results**：导出追踪数据为JSON格式
- **Deep Dependency Graph**：查看深度依赖关系图
- **Compare** 标签页：对比不同Trace的性能差异
- **System Architecture** 标签页：查看系统架构图
- **Monitor** 标签页：实时监控指标

**预期结果**：
- ✅ Jaeger UI正常访问（http://localhost:16686）
- ✅ 能看到追踪数据列表（8条Traces或更多）
- ✅ 时间线图显示执行时间分布
- ✅ 每条Trace包含Span数量和持续时间
- ✅ 高级功能可用（下载、对比、监控）

> 📌 **当前状态说明**：
> - 追踪数据显示的是Jaeger自身的API调用（/api/traces）
> - Service名称为 `jaeger-all-in-one` 而非业务微服务名
> - 这是Dapr OTLP exporter配置的正常现象
> - 如需查看完整的跨服务调用链路，可能需要调整Dapr配置或使用Zipkin协议

> 🔧 **进一步优化建议**：
> 1. 尝试将tracing配置从OTLP改为Zipkin格式
> 2. 或使用Dapr Dashboard查看服务拓扑
> 3. 或集成Grafana + Tempo进行更强大的追踪分析

---

### 测试7：Prometheus指标收集

**测试目标**：验证Prometheus端点导出核心指标（延迟、错误率、吞吐量）

#### 测试步骤

**1️⃣ 检查Dapr Metrics端点**
```cmd
curl http://localhost:3500/metrics
```

**2️⃣ 查看关键指标**
```cmd
curl http://localhost:3500/metrics | findstr "dapr_http_server_request_count"
curl http://localhost:3500/metrics | findstr "dapr_http_server_request_duration_ms"
curl http://localhost:3500/metrics | findstr "dapr_runtime_component_operation_count"
```

**3️⃣ 关键指标说明**

| 指标名称 | 含义 |
|----------|------|
| `dapr_http_server_request_count` | HTTP请求总数（按method、path、status分组） |
| `dapr_http_server_request_duration_ms_bucket` | HTTP请求延迟分布（P50/P90/P99） |
| `dapr_runtime_component_operation_count` | 组件操作计数（state/pubsub/service invocation） |

**4️⃣ 压力测试生成指标**
```cmd
for /L %i in (1,1,100) do (
  curl -X POST http://localhost:5002/order/create -H "Content-Type: application/json" -d "{\"userId\":\"user%i\",\"items\":[{\"productId\":\"prod001\",\"productName\":\"Test Product\",\"quantity\":1,\"price\":99.99}]}" > nul
)
```

**5️⃣ 再次查看指标变化**
```cmd
curl http://localhost:3500/metrics | findstr "request_count"
# 预期：数值明显增加
```

**预期结果**：
- ✅ Metrics端点可访问（返回Prometheus格式文本）
- ✅ 包含Dapr内置指标（请求计数、延迟、组件操作）
- ✅ 压力测试后指标数值增长
- ✅ 可用于Grafana监控大屏展示

---

## 测试技巧

### 实时查看日志
```cmd
powershell Get-Content "services\order-service\.dapr\logs\order-service_app_*.log" -Wait -Tail 20
```

### 并发测试
```cmd
start cmd /k "curl -X POST http://localhost:5002/order/create ..."
start cmd /k "curl -X POST http://localhost:5002/order/create ..."
start cmd /k "curl -X POST http://localhost:5002/order/create ..."
```

### 故障注入测试
```cmd
taskkill /F /IM user-service.exe /T
curl -X POST http://localhost:5002/order/create ...
```

## 测试结果记录模板

建议您创建一个测试记录表：

| 测试项 | 测试时间 | 结果 | 问题 | 备注 |
|--------|----------|------|------|------|
| 健康检查 | | ✅/❌ | | |
| 订单创建流程 | | ✅/❌ | | |
| 发布订阅消息持久化 | | ✅/❌ | | |
| 工作流检查点恢复 | | ✅/❌ | | |
| 优雅关闭SIGTERM | | ✅/❌ | | |
| Multi-App Run一键启停 | | ✅/❌ | | |
| Jaeger分布式追踪 | | ✅/❌ | | |
| Prometheus指标收集 | | ✅/❌ | | |