# WebFlux 原理深挖 — 面试应对手册

> 原则：4年经验面试官不会要求你手写Reactor，他验证的是"你真的用过"还是"只是调了个API"。你要做的是**把原理讲成你项目里真实发生的事**，而不是背概念。

---

## 一、必背的原理主线（3分钟版本，先背这段）

> "WebFlux底层是Reactive Streams规范：四个接口 Publisher/Subscriber/Subscription/Processor，核心是**拉模型的异步序列 + 背压**。Reactor的Flux/Mono是它的实现，操作符本质是**懒执行的组装**——只有subscribe()才触发订阅链，操作符一层层包成新的Publisher。线程模型上WebFlux默认跑在**Netty的EventLoop**上，少量线程处理海量连接，靠回调驱动；数据在链路中流动时通过 **publishOn/subscribeOn** 切换执行线程。我在项目里选WebFlux，就是因为我调大模型是长耗时流式IO，阻塞模型下一个请求占一个线程，200线程就到顶了；非阻塞下几十个EventLoop线程就能扛住。"

这段话把规范、实现、线程模型、选型动机全串了，说完整面试官的印象分已经拿到。

---

## 二、高频追问 & 标准答案要点

### 1. 背压（Backpressure）到底是什么？你的项目里怎么体现？

**要点：**
- 消费者通过 `subscription.request(n)` 告诉生产者"我还能处理n条"，**消费端反压生产端**，防止上游生产过快把下游内存打爆
- Reactor里的策略：`onBackpressureBuffer/Drop/Latest/Error`
- **落到你项目**：大模型SSE流式输出，token生成速度 > 客户端消费速度时，TCP层面有滑动窗口、Reactor Netty层面有水位线（`onWriteIdle`/writeBufferLowHighWaterMark），这就是天然背压场景。能说出"我的流式响应链路就是背压的实际场景"就是加分项。

### 2. subscribeOn 和 publishOn 的区别？（必考）

| | subscribeOn | publishOn |
|---|---|---|
| 作用 | 影响信号**产生**（上游执行）在哪个线程 | 影响信号**消费/下游操作符**在哪个线程执行 |
| 多次调用 | **只有第一个生效**（离源头最近的？——实际是最早的那个生效） | 每次调用都生效，切换其后所有下游 |

**记忆法**：subscribeOn 管"从哪取数"，publishOn 管"在哪处理"。
**项目落点**：鉴权/安全校验的远程调用我放 boundedElastic 或专用调度器，避免占用EventLoop——这正是连接你"Netty网关阻塞bug"的关键证据。

### 3. map 和 flatMap 的区别？

- `map`：同步一对一转换，值 → 值
- `flatMap`：值 → **新的Publisher**（异步），一对多、顺序不保证
- 延伸：`concatMap` 保序、`flatMapSequential` 保序不保完成顺序
- **项目落点**：调用LLM是异步IO，必须flatMap包`webClient.post()`；prompt校验后拼接结果用map。

### 4. 为什么WebFlux不能用ThreadLocal？那traceId/MDC怎么传？

**这是送分题，也是你项目的真实痛点：**
- 信号在不同EventLoop线程间跳转，ThreadLocal绑定线程会丢
- 正确姿势：**Reactor Context**（`contextWrite`/`deferContextual`），Context沿订阅链向下传递
- 日志：`logback` + `Reactor` 的 `onLastOperator`/Hooks 注入，或Spring的 `Hooks.enableAutomaticContextPropagation()`（Micrometer 1.0+）
- **主动讲**："我们排查过一次traceId断链，就是filter里用了MDC.get，改成Context传递后解决" —— 真实细节最值钱。

### 5. WebFlux和Servlet（SpringMVC）性能对比？什么时候用？

**答法（避免说"WebFlux一定快"——这是经典坑）：**
- WebFlux优势在**高并发长IO**（大量连接、慢客户端、流式），省的是线程内存和上下文切换，吞吐上限更高
- **单个请求的延迟不会更快，CPU密集型场景反而不如MVC**（调度开销+调试难）
- 判断标准：IO密集+高并发连接 → WebFlux；普通CRUD低并发 → MVC，团队心智成本更低
- **落点**："我们场景是LLM调用单次10s+且流式，MVC下Tomcat 200线程很快耗尽，这是选型根本原因。"

### 6. Netty的EventLoop怎么工作的？为什么在EventLoop里做阻塞调用是灾难？

**接你简历里的Netty优化点：**
- Boss线程组接收连接 → 注册到Worker EventLoop（默认 = 2×CPU核数个线程）
- 一个EventLoop绑定N个Channel，**串行处理**该组所有channel的IO事件
- EventLoop里做阻塞调用（同步DB/HTTP）→ 整个EventLoop上成百上千连接全部卡死
- **落点**："我们早期的阻塞bug就是这个：在EventLoop里同步调鉴权服务，P99飙到几秒，把鉴权客户端改成WebClient异步 + 本地缓存兜底后恢复。" 这一段是你整个项目最真实的"事故叙事"。

### 7. Reactor的操作符是声明了就执行吗？冷流和热流？

- 操作符是**懒执行**，只是组装订阅链（装配期），subscribe()才触发（运行期）
- **冷流**：每个订阅者独立走一遍数据源（Flux.just/WebClient默认）
- **热流**：`ConnectableFlux`、`Sinks.Many`，多订阅者共享同一数据源
- **落点**：AIGC排队进度推送——多个前端订阅同一任务的进度事件，用Sinks广播，这就是热流的实际用法。

### 8. WebFlux里还能写for循环/同步代码吗？异常怎么处理？

- 可以写，但**任何阻塞操作符内（map/doOnNext）的阻塞调用都会拖垮EventLoop**；`block()`在响应式链里直接抛异常（Spring检测）
- 真有阻塞需求 → `subscribeOn(Schedulers.boundedElastic())`，专用有界线程池（默认10×CPU，队列100k）
- 异常：`onErrorResume/onErrorMap/onErrorContinue`，全链路`doOnError`记录；**注意onErrorContinue会污染链路，谨慎用**

### 9. 如果面试官问"你压测过吗？数据多少？"

提前准备一组自洽数字（口径按单机4C8G级别估）：
- 阻塞模型（改造前）：Tomcat 200线程，LLM调用10s → 理论上限 ≈ 20 QPS，实际压到线程耗尽报错
- WebFlux后：几十个EventLoop线程，长连接流式场景压到 **数百并发连接 / 单机TPS提升一个数量级**
- 关键指标不是TPS而是：**并发连接数、P99 RT（流式首token时间）、线程数（200+ → ~20）**
- 诚实话术："我们压测聚焦并发连接数和首token延迟，TPS本身受上游模型配额限制。"——主动把口径拉回你说过的地方。

---

## 三、压不住时怎么办（防穿底三板斧）

1. **降维到项目事实**："这个细节我们当时的做法是……底层机制上我的理解是……如果说错了请您指正" —— 用真实决策顶住理论追问
2. **展示知识边界**："Reactor更底层的操作符融合（operator fusion）我了解不深，生产里我用到的深度到publishOn线程切换和Context传递为止。" 诚实反而加分，瞎编必挂
3. **拉回主场**："这块原理我的实践就是解决XX问题（traceId断链/EventLoop阻塞），要不要我讲讲那个排查过程？" —— 把理论题变成你的事故复盘故事

---

## 四、考前自测清单（能秒答才算过）

- [ ] 四大接口名字 + 谁调用谁的哪个方法（订阅顺序 vs 信号顺序相反）
- [ ] subscribeOn只第一个生效、publishOn每次生效
- [ ] map/flatMap/concatMap区别
- [ ] ThreadLocal为什么失效、Context怎么用
- [ ] EventLoop阻塞调用的后果 + 你项目的真实案例
- [ ] 什么时候不该用WebFlux（CPU密集/低并发CRUD）
- [ ] 冷流热流 + Sinks的用处（进度推送）
- [ ] boundedElastic是干什么的、默认参数

> 订阅细节提醒：`Flux.create` 里 `sink.next()` 的信号流向是 **从下往上传request、从上往下传数据**——如果被问到"request(n)是谁调用的"，答"订阅链上每个操作符作为上游的Subscriber，把下游的request翻译后传给更上游"。

---

## 五、用"一次模型请求"串起 WebFlux / WebClient / Netty 关键概念（含线程归属）

> 这是最能体现"真用过"的讲法：面试官问任何概念，都可以用这条请求链路定位回答。

### 请求全链路（线程标注）

| 阶段 | 发生什么 | 跑在哪个线程 |
|---|---|---|
| 1. 连接接入 | 客户端 TCP 三次握手，**Boss EventLoop** accept 连接，注册读写事件到某个 **Worker EventLoop** | Boss（极短）→ Worker |
| 2. HTTP 解析 | Reactor Netty 读取字节、解析成 HttpRequest/HttpResponse，交给 WebFlux 的 HttpHandler | Worker EventLoop |
| 3. 责任链编排 | Controller 返回 Mono/Flux（**此时只是组装订阅链，一行业务逻辑都没跑**） | Worker EventLoop |
| 4. subscribe 触发 | 框架 subscribe 后，责任链（鉴权→限流→安全校验→网关）开始执行 | Worker EventLoop；其中同步重逻辑/远程阻塞调用放 **boundedElastic** |
| 5. WebClient 发起调用 | WebClient 底层也是 Reactor Netty 客户端：从**连接池**取连接（可复用），把请求写出，**写出后线程立即释放** | Worker EventLoop（客户端侧，通常复用或独立的 event loop group） |
| 6. 等待模型响应 | **关键：没有任何线程在等**。连接注册了 OP_READ，EventLoop 去处理成千上万个其他请求 | 空闲（零线程占用） |
| 7. SSE token 回来 | 模型每吐一个 token，客户端 Worker EventLoop 收到读就绪回调 → 解析 SSE 帧 → Flux 数据项向下游推 | Worker EventLoop（客户端侧） |
| 8. 写回给调用方 | token 经过你的处理逻辑后写回响应——**服务端**的 Worker EventLoop 触发写事件 | Worker EventLoop（服务端侧） |
| 9. 连接关闭/完成 | Flux complete → 连接归还连接池或关闭 | Worker EventLoop |

### 三个角色一句话定位

- **Netty**：最底层，管 TCP 连接、事件多路复用（epoll）、Boss/Worker EventLoop、连接池。它决定了"线程模型"。
- **WebFlux**：基于 Netty 的响应式编程框架，提供 HttpHandler→WebHandler→RouterFunction/Controller 那套，以及 Flux/Mono 组装层。它决定了"业务怎么写"。
- **WebClient**：WebFlux 生态里的 HTTP **客户端**，底层同样走 Reactor Netty，所以发起对模型服务的调用也是非阻塞的。它决定了"你怎么调下游"。

### 面试金句

> "一次请求里，Netty 负责连接和事件，WebFlux 负责编排和组装，WebClient 负责非阻塞调下游。最有说服力的点是第 6 步：等大模型吐 token 的 10 秒里，我们一个线程都不占，EventLoop 已经在服务别的请求了——这就是响应式的全部意义。"

### 常见追问快答

- **"Boss 和 Worker 的区别？"** Boss 只 accept，Worker 管 IO 读写，默认数量 2×CPU 核。
- **"连接池在哪一层？"** WebClient/Reactor Netty 的 ConnectionProvider，默认上限 500/remote host，我调大模型时按模型域名配置了连接池和 pending acquire 超时。
- **"token 回来在哪解析？"** 客户端 Worker EventLoop 上收到数据，SSE 解析在 `bodyToFlux(String.class)` / `ServerSentEvent` 的流里，重 CPU 的解析逻辑可 publishOn 切到其他调度器。

---

## 六、到底谁是 Publisher 谁是 Subscriber？（结合一次模型请求）

### 先记两条铁律

1. **数据从上游（Publisher）流向下游（Subscriber）；subscribe() 调用从下游往上游传播；request(n) 沿订阅链向上传给数据源。**
2. **你的业务代码永远不主动 subscribe() —— 最终 Subscriber 是框架。** Controller 里返回 Flux 是"组装"，订阅动作由 WebFlux/Netty 完成。

### 一次模型请求里的发布/订阅关系

请求里其实有**两条订阅链**，方向相反、衔接在你写的责任链上：

**A. 出站链（响应写回）—— 你是 Publisher，框架是 Subscriber：**
```
你的Controller返回的Flux（责任链组装结果）
    ↓ onNext(token)
WebFlux响应写出层（HttpWebHandlerAdapter → ChannelSendOperator）  ← 最终Subscriber
    ↓
Reactor Netty outbound：把token编码成字节写进Socket
```
你的 Flux 每 onNext 一个 token，框架 Subscriber 就把字节写给客户端。**这就是"框架替你订阅"的实锤。**

**B. 入站链（调模型）—— WebClient 是 Publisher，你的操作符是 Subscriber：**
```
WebClient → bodyToFlux(ServerSentEvent)      ← 源头Publisher
    ↓ onNext(SSE帧)
你链路上的每个操作符（map/flatMap/filter，每个操作符内部都是一个Subscriber+Publisher的双重身份）
    ↓
接到出站链A的头上，继续流向框架的响应写出层
```

**衔接点**：你的责任链/操作符既是 A 的 Publisher，又是 B 的 Subscriber——**每个操作符天生双重身份**，这就是 Processor 概念存在的意义（既是发布者又是订阅者）。

### subscribeOn / publishOn 的本质

- **subscribeOn**：改变的是"**订阅信号沿链向上传播**"时所在的线程。订阅动作最终会触达数据源，所以它决定了**数据源在哪个线程开始生产数据**。链上写多个只有最靠近 subscribe() 的那个生效（更准确说：第一个被传播到的生效）。它只影响"订阅/生产"，不影响下游消费。
- **publishOn**：在链路中间插一个"线程切换中转站"。上游 onNext 把数据丢进队列，一个新 Worker 从队列取数据继续调下游的 onNext——**它改变的是该点之后所有下游操作符的执行线程**，每次调用都生效。
- 一句话：**subscribeOn 管源头在哪生产，publishOn 管中途在哪消费。**

### 四大接口 → 框架关键实现对照

| 规范接口 | 职责 | Reactor/Spring 关键实现 | 在你项目里的对应物 |
|---|---|---|---|
| `Publisher<T>` | 数据源，提供 subscribe(subscriber) | **Mono / Flux** | WebClient 的响应流、你 Controller 返回的链 |
| `Subscriber<T>` | 消费端，onSubscribe/onNext/onError/onComplete | 每个操作符内部类（如 FluxMap 的 MapSubscriber）、LambdaSubscriber；框架侧是 **ChannelSendOperator/响应写出器** | 你的 map/flatMap 逻辑；Netty 的写出处理器 |
| `Subscription` | request(n) + cancel 的句柄，背压契约载体 | `Operators` 下的各类 Subscription 实现、BaseSubscriber | request(n) 就是 requestUnbounded/按需请求 |
| `Processor` | 既是 Publisher 又是 Subscriber（中继/桥接） | 新代码用 **Sinks.Many**（EmitterProcessor 等已过时） | AIGC 排队进度广播：Sinks 是热流，多前端订阅同一任务进度 |

### 面试金句

> "整个请求里我自己从没写过 subscribe——我是中间层：对上游模型响应流我是 Subscriber，对下游框架的响应写出层我是 Publisher。真正的最终订阅者是 Reactor Netty 的写出处理器，它订阅我的 Flux，每收到一个 token 就写一次 Socket。subscribeOn 决定源头生产线程，publishOn 在链路中间做线程切换。"

### 追问快答

- **"为什么不让自己 subscribe？"** 框架要管背压协调、连接生命周期、异常→HTTP状态码映射，自己subscribe会脱离这套管理。
- **"Processor 为什么被 Sinks 取代？"** 老Processor（EmitterProcessor/DirectProcessor）在无订阅者时丢数据、行为反直觉；Sinks API 拆清了"生产视角"和"订阅视角"。
- **"request(n) 谁在调？"** 订阅链上每个操作符作为上游的Subscriber，把下游的request翻译/缓存后传给更上游；对HTTP IO通常就是 Long.MAX_VALUE 无限请求，背压由TCP水位和连接池兜底。





# 个人理解整理

## 对应简历业务的理解

### 一个请求链路的业务逻辑和框架的发布订阅是什么

第一条响应回写：我的接口返回的业务逻辑flux是发布，框架是订阅。 第二条调用模型：上游的webclient是发布，我的业务逻辑链路上的每个操作符都是一个订阅者

