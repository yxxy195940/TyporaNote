# Spring Cloud Netflix vs Alibaba — 核心概念与面试话术

> 定位话术先说死："工作中服务发现和熔断用的是 Netflix 体系（Eureka/Hystrix/Ribbon/Gateway），Nacos、Sentinel、Seata 我个人学习用过。组件在换，原理是同一套问题：注册、发现、容错、配置——我可以按原理讲，用什么组件只是实现差异。" —— 这段话把"没做过 Alibaba"从劣势变成原理深度的展示机会。

---

## 一、组件对照表（必背）

| 能力 | Netflix 体系 | Alibaba 体系 | 关键差异 |
|---|---|---|---|
| 注册中心 | **Eureka** | **Nacos** | Eureka 是 AP、peer-to-peer 复制；Nacos 可 AP/CP 切换（AP 用 Distro，CP 用 Raft），且注册+配置中心一体 |
| 负载均衡 | **Ribbon**（客户端 LB） | Ribbon/LoadBalancer 同理 | 概念完全一致：客户端拉列表本地决策 |
| 熔断降级 | **Hystrix** | **Sentinel** | Hystrix 靠线程池隔离；Sentinel 靠并发计数 + 滑动窗口，无需建线程池，规则更细 |
| 网关 | Zuul 1（阻塞 Servlet）→ **Spring Cloud Gateway**（WebFlux+Netty） | 同用 Gateway | Gateway 正是你的 WebFlux 主场 |
| 声明式调用 | **Feign** | Feign/Dubbo | Feign = 接口+注解→动态代理→HTTP；Dubbo 走 RPC 二进制协议 |
| 配置中心 | Spring Cloud Config（Git 存储） | **Nacos Config** | Nacos 配置推送用**长轮询**（30s 容忍）不是真推送 |
| 分布式事务 | 无 | **Seata** | AT 模式：一阶段本地提交+undo_log，二阶段异步删日志/回滚 |

---

## 二、Netflix 侧必会原理（你的主场，讲深）

### Eureka
- **AP 设计**：节点间 peer-to-peer 异步复制，不保证强一致，换取可用性
- 心跳 30s 一次，90s 未收到才剔除；Ribbon 本地列表还有 30s 缓存 → **一个实例下线，最长约 3 分钟才从调用方视野消失**（经典追问）
- **自我保护模式**：一分钟内心跳比例低于阈值（网络分区时），停止剔除实例——宁要脏数据不要雪崩

### Hystrix
- **线程池隔离** vs 信号量隔离：前者可超时/限并发但有线程切换开销，后者轻量但不支持超时异步
- 熔断器三态：closed → 错误率超阈值（如 10s 内 50%）→ **open（直接走 fallback）** → 睡眠窗口后 **half-open 放一个探测请求**
- 为什么被淘汰：线程池开销大、维护停滞 → Sentinel / Resilience4j

### Spring Cloud Gateway（接你的 WebFlux）
- 基于 WebFlux + Reactor Netty：Route（谓词匹配）→ Filter 链（global/per-route）→ 转发
- **可主动讲**："网关里加全局过滤器做鉴权和限流，和我在调度中台做责任链是一件事；Zuul 1 是一请求一线程的阻塞模型，Gateway 是 EventLoop 模型——这正是我调研过 Netty 阻塞问题的原因。"

---

## 三、Alibaba 侧必会原理（学习经验级也要能讲）

### Nacos
- 注册：服务方注册+心跳；**AP/CP 可选**——临时实例走 Distro（AP），持久实例走 Raft（CP）
- 配置推送：客户端**长轮询**（挂起 30s），配置变更即返回，不是真推送——"为什么不用长连接"可答早期简化实现、长轮询已够实时

### Sentinel
- **滑动窗口统计（LeapArray）**：把时间切成小窗口格子统计 QPS/RT，避免固定窗口临界突刺——**和你 Redis 限流的分桶思路直接呼应，必讲**
- 流控形状：快速失败 / Warm Up（冷启动渐增）/ 排队等待（匀速）；还有热点参数限流、系统自适应
- 熔断策略：慢调用比例 / 异常比例 / 异常数，半开探测
- **vs Hystrix 一句话**：Hystrix 用线程池隔离换取隔离强度，Sentinel 用轻量并发控制+丰富规则，吞吐更高

### Seata AT（大概率只问概念）
- 一阶段：业务 SQL 执行前后取前后镜像写 **undo_log**，本地事务直接提交
- 二阶段：全局提交→异步删 undo_log；回滚→用镜像反补
- 前提：支持 ACID 的关系库 + 每个参与者库里有 undo_log 表

---

## 四、高频追问快答

- **"为什么你们用 Eureka 不用 Nacos？"** 存量系统+AP 够用；Nacos 一体化是新项目首选，我迁移过思路但没主导过存量替换。
- **"Eureka 和 Nacos 选型差异？"** 一致性要求：服务列表短暂陈旧可容忍→AP；要强一致配置/持久实例→CP。
- **"Hystrix 换 Sentinel 迁移要注意什么？"** 隔离模型变了（线程池→并发计数），超时语义要重新评估；注解从 @HystrixCommand 换 @SentinelResource，fallback 逻辑可复用。
- **"Feign 原理？"** 接口+注解 → JDK 动态代理 → 方法签名翻译成 HTTP 请求模板 → Ribbon/LoadBalancer 选实例 → 编解码；可加拦截器做鉴权头透传。
