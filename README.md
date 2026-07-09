# Dapr Order Processing System

基于Dapr的微服务订单处理系统，完整实现服务调用、状态管理、发布订阅三大构建块。

## 技术栈

- **语言**: Go 1.25
- **框架**: Dapr SDK v1.14.0
- **状态存储**: Redis
- **消息队列**: Redis PubSub

## 项目架构

```
order-dapr/
├── services/
│   ├── user-service/          # 用户验证服务
│   │   └── main.go            # /user/validate, /user/create, /user/get
│   ├── order-service/         # 订单编排服务（核心）
│   │   └── main.go            # /order/create, /order/get, /order/status/update
│   ├── payment-service/       # 支付处理服务
│   │   └── main.go            # /payment/process, /payment/create, /payment/get
│   └── inventory-service/     # 库存管理服务
│       └── main.go            # /inventory/check, /inventory/create, /inventory/get
├── dapr/
│   ├── config.yaml            # 全局配置
│   └── components/
│       ├── statestore.yaml    # Redis状态存储
│       ├── pubsub.yaml        # Redis发布订阅
│       └── resiliency.yaml    # 弹性策略
├── dapr.yaml                  # 多应用运行配置
└── docker-compose.yml         # 基础设施编排
```

## 核心功能

### 服务间调用（Service Invocation）

Order Service作为编排层，按顺序调用下游服务完成订单处理流程：

```
Order Service
    ├─[Step 1]→ User Service (/user/validate)
    │               ↓ 返回用户验证结果
    ├─[Step 2]→ Inventory Service (/inventory/check)
    │                   ↓ 返回库存检查结果
    ├─[Step 3]→ Payment Service (/payment/process)
    │                   ↓ 返回支付处理结果
    └─[Step 4]→ State Store + PubSub Event
                    ↓ 订单状态持久化 + 发布事件
```

**弹性策略**（[resiliency.yaml](dapr/components/resiliency.yaml)）:

| 策略 | 配置名 | 参数 | 说明 |
|-----|--------|------|------|
| 重试 | order-retry-policy | 恒定间隔1s,最多3次 | 网络抖动恢复 |
| 超时 | order-timeout-policy | 5s | 下游响应超时控制 |
| 断路器 | order-circuit-breaker | 连续失败3次触发,10s熔断 | 级联故障防护 |

### 状态管理（State Management）

- **存储后端**: Redis ([statestore.yaml](dapr/components/statestore.yaml))
- **并发控制**: ETag乐观锁（first-write模式）
- **一致性级别**: Eventual(默认)/Strong可配置
- **Key格式**: `order-{orderId}`, `user-{userId}`, `payment-{paymentId}`, `inventory-{productId}`

**订单状态流转**:
```
pending → validated → checked → processed → completed
```

### 发布订阅（PubSub）

- **组件**: [pubsub.yaml](dapr/components/pubsub.yaml)
- **用途**: 异步事件驱动，解耦主流程与非关键路径

## 快速启动

```bash
# 初始化Dapr
dapr init

# 一键启动所有服务
dapr run -f .
```

或逐个启动各服务（详见[dapr.yaml](dapr.yaml)配置）。