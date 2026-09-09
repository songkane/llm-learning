# 07 · 横向对比：Dynamo vs llm-d vs Mooncake

> **前六篇只讲 Dynamo 自身**，跨项目的对比全部集中在这一篇。
> 对照基线：Dynamo `main @ 946acce` ｜ [llm-d Router](../llm-d/router/) `main @ 90a28bc` ｜ [llm-d WVA](../llm-d/autoscaling/) `release-0.9 @ d5d5864` ｜ [Mooncake](../../kvcache/mooncake/)。
> 读法建议：先读完 00~06 建立 Dynamo 的完整图像，再回到这里；每一节末尾都标了它对应哪一篇。

## 0. 三者的分层位置

先把「谁解什么问题」摆清楚，否则很容易拿不同层的东西硬比：

| 问题 | llm-d | Dynamo | Mooncake |
|------|-------|--------|----------|
| 请求发给谁 | [Router / EPP](../llm-d/router/) | Router（[02 篇](02-核心代码分析-KV感知路由.md)） | — |
| 该有几个副本 | [WVA](../llm-d/autoscaling/) | Planner（[03 篇](03-核心代码分析-SLA-Planner.md)） | — |
| KV 怎么分层卸载 | 交给引擎 + Mooncake | **KVBM**（[04 篇](04-核心代码分析-KVBM分层KV管理.md)） | [Mooncake Store](../../kvcache/mooncake/02-MooncakeStore缓存池.md) |
| KV 怎么跨节点搬 | 引擎的 connector | **NIXL**（[05 篇](05-核心代码分析-PD分离与NIXL.md)） | [Transfer Engine](../../kvcache/mooncake/01-TransferEngine传输引擎.md) |
| Pod 落在哪 | kube-scheduler / [Volcano](../../scheduling/) | 同上；拓扑 gang 另见 Grove | — |
| 谁做 tokenize | 引擎 | **Frontend**（[01 篇](01-请求的一生-主控制流.md)） | — |

**llm-d 与 Dynamo 是同层的两套取舍**，Mooncake 则低一层——它只解 KV 存储与传输，可以被 llm-d 那套栈当组件用。所以下文里 llm-d 的对比是「同一件事的另一种做法」，Mooncake 的对比是「同一件事该不该抽出来做」。

组织方式上两者也相反：

| | llm-d | Dynamo |
|---|-------|--------|
| 仓库形态 | **多仓组合**（Router、WVA 各一个仓库） | **单仓多组件**（Frontend / Router / Planner / KVBM 同仓） |
| 语言 | Go | **Rust 核心 + Python 扩展层**（PyO3 / maturin 绑定） |
| 请求入口 | Gateway API + Envoy ext-proc | **自带 Frontend**；Gateway API 是可选旁路 |
| 依赖的外部件 | Envoy / Gateway、可选 Redis | 1.x 起 etcd 和 NATS 都不再必需 |

## 1. 入口：路由器在不在数据通路上

这是所有分歧的源头。

> **llm-d 的 EPP 从不接触 token 流；Dynamo 的 Frontend 既做 HTTP 又做 tokenize / detokenize，请求和响应都穿过它。**

派生出来的差别是成串的：

| | llm-d EPP | Dynamo Frontend |
|---|---|---|
| 位置 | **控制通路**：Envoy 回调里答一句「发给谁」 | **数据通路**：请求和响应的每个 token 都过它 |
| 工作量 | 与**请求数**成正比 | 与 **token 数**成正比 |
| 因此语言选 | Go 够用 | 必须 Rust |
| 路由时拿得到什么 | 通常**拿不到 token**——要么用近似的字符前缀，要么自己再 tokenize 一遍 | **精确的 token id 序列**（路由发生在 tokenize 之后） |
| 故障半径 | 挂了只影响新请求的选路 | 挂了整条流断 |

Dynamo 能做精确 KV 前缀匹配，本质上是「Frontend 顺手已经 tokenize 过了」这个红利。代价是 Frontend 成了必经之路，也就必须为它的吞吐负责。

反过来说，llm-d 那套的好处是**入口标准化**：走 Gateway API 就能和 Envoy 生态里的其它能力叠加。Dynamo 也提供这条旁路（[06 篇 §4](06-部署与Operator.md#4-gaie-路径) 的 GAIE 路径），但一旦走 standalone EPP 模式，就得自己再 tokenize 一遍——**这是为标准化付的代价**，也正好印证了上表那一行。

> 对应 [01 篇 §5](01-请求的一生-主控制流.md#5-路由决策在最后一环) ｜ 对照 [llm-d Router 01](../llm-d/router/01-请求的一生-主控制流.md)

## 2. 打分：减法 vs 加权求和

两边都要在「KV 命中多」和「负载轻」之间权衡，写法完全不同。

**llm-d**：多个 scorer 加权求和，`score = w1 · overlap + w2 · (1 − load) + ...`。各项无量纲，权重靠部署实测调。

**Dynamo**：一个统一 cost 函数，**所有项的单位都是 block 数**，overlap 从 prefill 工作量里**减掉**（`max(0, B_prefill − C_overlap)`）。

差别不只是形式：

| | 加权求和 | 减法式 cost |
|---|---|---|
| 各项量纲 | 无量纲，靠权重拉平 | 统一为 block 数 |
| 权重怎么定 | 按部署实测调 | `overlap_score_credit` 默认 1.0——**一块命中就是一块不用算，1:1 是正确基线** |
| 结果的物理含义 | 相对偏好，无绝对意义 | 「这个 worker 还得多算多少块」 |
| 命中饱和 | 靠权重上限间接控制 | `max(0, ...)` 截断：全命中即零 prefill 成本，不会变负数去挤兑 decode 项 |

**这不是谁更先进的问题**，而是「有没有把量纲对齐」的问题。Dynamo 敢用 1.0 作默认权重，是因为它把 cost 定义在了一个有物理意义的尺度上；llm-d 的 scorer 之间没有共同尺度，就只能靠调。

有意思的是插件层面两者**是同构的**：Dynamo 的 `Filter / Scorer / Picker` 三段式（`lib/kv-router/src/scheduling/`）和 llm-d EPP 的三段式分工一致，连聚合方式都一样——多个 Scorer 直接求和、不归一化。**区别在于哪条是默认路径**：llm-d 的默认路径就是一堆 scorer 加权求和；Dynamo 的默认是那个统一 cost 函数，三段式是给「你要换算法」准备的逃生口。

> 对应 [02 篇 §1](02-核心代码分析-KV感知路由.md#1-打分公式overlap-不是加分是减免工作量)、[§5](02-核心代码分析-KV感知路由.md#5-插件化filter--scorer--picker) ｜ 对照 [llm-d Router 04](../llm-d/router/04-核心代码分析-KVCache索引与前缀缓存路由.md)、[llm-d Router 02](../llm-d/router/02-核心代码分析-调度框架与插件体系.md)

## 3. KV 索引：同一组权衡，Dynamo 多了一种叠加

「Router 怎么知道谁手上有哪些 KV」，两边面对的是**同一组权衡**：

- **精确事件流**：订阅引擎的 KV 事件（vLLM 的 ZMQ），准，但有延迟，且要求引擎支持。
- **近似路由历史**：Router 记住自己发过什么，零依赖，但会和引擎的真实状态漂移。

llm-d 的 KV Indexer 也是这两条路。**差别在于 Dynamo 把两者做成了可叠加的**：`use_kv_events = true` 的同时设 `router_predicted_ttl_secs`，会额外起一棵短 TTL 的 side tree（`indexer/side.rs:29`），与主索引**取 max overlap**。

这补的是事件的**延迟窗口**——请求刚发出去、引擎还没来得及发 `BlockStored` 的那几十毫秒里，主索引不知情，side tree 先顶上。**同一个请求的连发（比如 agent 的多轮工具调用）最吃这个。**

另一个 llm-d 那边没有的机制是 **conditional disaggregation**：要不要走 P/D 分离是**每个请求现算的**，判据是「净新增 token」而非 prompt 长度。llm-d 的 P/D 分离是部署期的拓扑决定，不逐请求切换。

> 对应 [02 篇 §3](02-核心代码分析-KV感知路由.md#3-有-kv-事件-vs-没有两套维护索引的办法)、[§4](02-核心代码分析-KV感知路由.md#4-pd-分离下的路由要不要分离是每个请求现算的)

## 4. 副本数：正推 vs 倒推

同一个问题（「该开几个副本」），方法论根本不同。

> **WVA 靠「现在挤不挤」倒推；Dynamo Planner 靠「一个副本能扛多少 rps」正推。**

| | llm-d WVA | Dynamo Planner |
|---|---|---|
| 核心量 | KV token 的**供需比** | 单副本**容量**（rps） |
| 判据 | 双阈值死区：`demand/0.85` 扩、`demand/0.70` 缩 | `ceil(需求 rps ÷ 单副本 rps)` |
| 单副本容量从哪来 | 从**当前观测**推 | 从**性能模型**查（离线 profile 或仿真） |
| 需要预先 profile 吗 | 不需要 | **需要** |
| SLA 怎么进入决策 | 间接（阈值是经验值） | **直接**：TTFT / ITL 是容量搜索的约束条件 |
| 投机解码 | 不在模型里 | 进了公式：`itl = forward / accept_length` |

这个差别是结构性的：Dynamo 敢直接拿 TTFT / ITL 当输入，是因为它手上有一条「batch size → 延迟」的性能曲线；WVA 没有，只能用供需阈值近似。**代价是 Dynamo 多了一个前置步骤**——曲线得先搞到手，换硬件或并行配置就作废。

值得注意的是 Dynamo 自己也留了阈值档：`optimization_target` 取 `throughput` / `latency` / `load` 时走 easy mode，不需要 profile。**easy mode 才是和 WVA 同类的做法**，起步阶段用它更现实。

两个细分对比：

- **P/D 联合扩缩**：WVA 用 **Δ_util 匹配**（两侧利用率变化量要配得上），Dynamo 用 **GPU 预算的比例 clamp**。前者约束「效果对齐」，后者约束「资源不超发」。
- **决策怎么落地**：WVA 不自己改副本，产出 `wva_desired_replicas` 指标交给 HPA/KEDA 消费；Dynamo 自己改，但改的是 DGDSA 的 **Scale 子资源**——一个 HPA 也能改的标准接口。**两者是同一个思路的两种实现**：都在避免「两个控制器抢同一个 replicas 字段」。
- **缩容到零**：WVA 是内建的一等公民（含唤醒路径）；Dynamo 允许你配 `min_endpoint = 0`，但唤醒得靠别的组件（如 ModelExpress 的权重流式加载），不是打磨过的完整路径。

> 对应 [03 篇](03-核心代码分析-SLA-Planner.md) ｜ 对照 [llm-d WVA 01](../llm-d/autoscaling/01-核心原理-轮询引擎与双阈值容量模型.md)

## 5. KV 共享：池化 vs 分层

KVBM 和 Mooncake 都在解「KV 放不下 / 想跨实例复用」，但**架构假设相反**：

| | **Mooncake** | **KVBM** |
|---|---|---|
| KV 存在哪 | **集群级共享池**（Mooncake Store），实例把 KV 交出去 | **每个实例自己的 G1~G3**，KV 归实例所有 |
| 跨实例复用 | 从共享池里取 | leader `find_matches` 找到持有者，**RDMA 直接从对端拉** |
| 一致性焦点 | Master 元数据、PutStart/PutEnd 两阶段、租约与选主 | leader 的 hold 阶段、session 生命周期 |
| 有无中心组件 | 有（Master 管元数据，etcd 选主） | **没有集群级中心**；`kv_dc_relay` 是索引发布，不是存储权威 |
| 分层边界 | Store 内部分层（io_uring / 3FS / SPDK） | 实例内部分层（G1→G2→G3），G4 才外部化 |
| 传输 | Transfer Engine（多协议 RDMA、多网卡聚合、拓扑亲和选路） | NIXL |

一句话记：

> **Mooncake 把 KV 从实例里「抽出来」放进公共池；KVBM 让 KV「留在实例里」，需要时点对点搬。**

代价很清楚。Mooncake 的池化换来全局可见性和更高的复用率，代价是多一跳、多一个必须高可用的中心组件。KVBM 的点对点没有中心瓶颈、路径最短，代价是**得先知道谁有**——而这份知识散落在 Router 的 radix 索引和 `kv_dc_relay` 的 catalog 里。

**这解释了为什么 Dynamo 的 Router 那么重。** llm-d 可以把 KV 共享外包给 Mooncake，Router 只管路由；Dynamo 选了点对点，Router 就必须同时是「KV 位置的权威索引」。02 篇那一大坨代码不是路由算法复杂，是**索引维护复杂**。

两者在细节上也有殊途同归的地方：KVBM onboarding 的 `hold` 阶段（先钉住再搬）和 Mooncake 的 `PutStart/PutEnd` 两阶段写入解决的是同一类问题——**分布式搬运里，「我看到了」和「我拿到了」之间总有一个必须加锁的窗口**。

> 上游自己也在做这个比较：`kv_dc_relay/host.rs:11` 的注释里把 Mooncake 作为 multi-issuer 场景的性能对照提过。

> 对应 [04 篇](04-核心代码分析-KVBM分层KV管理.md) ｜ 对照 [Mooncake 00](../../kvcache/mooncake/00-总览与架构.md)

## 6. P/D 交接：pull vs push

有意思的是，这一组对比**在 Dynamo 仓库内部就能看到**——它同时支持两种 connector，语义正好相反：

| | **NIXL（pull）** | **Mooncake（push）** |
|---|---|---|
| 谁发起传输 | decode 侧拿着地址来拉 | prefill 侧直接推 |
| 协商方式 | prefill 填好空位，decode 读元数据后拉 | 请求一开始就生成 `transfer_id` UUID，两侧共用 |
| 往返次数 | 多一个往返（先算完再告诉你在哪） | 少一个往返（先约定单号，再各自动作） |
| 失败暴露时机 | 拉取时 | **构造时**就解析 bootstrap 地址，请求建立阶段即炸 |

所以「Mooncake 是 push 语义 + bootstrap server 协商」这个结论，在 [Mooncake 笔记](../../kvcache/mooncake/01-TransferEngine传输引擎.md)和 Dynamo 的 `MooncakeConnector` 实现里是一致的——**同一套协议的两侧视角**。

> 对应 [05 篇 §2](05-核心代码分析-PD分离与NIXL.md#2-核心分歧pull-还是-push) ｜ 对照 [llm-d Router 06](../llm-d/router/06-核心代码分析-PD分离与Sidecar.md)

## 7. 服务发现与部署

**发现机制**上，两者的抽象层次不同：

- **llm-d**：发现是 K8s InferencePool + label selector，请求面是 Envoy 转发（EPP 不碰），事件面是订阅 vLLM 的 ZMQ。**三件事由三个不同层次的东西各自解决。**
- **Dynamo**：收成一个 runtime 的**三个可插拔平面**（discovery / request / event），每个平面都有一个无外部依赖的默认实现。

Dynamo 这套的好处是「一个中间件都不装也能跑」，坏处是多了一层自研抽象要理解。llm-d 那套的好处是每一层都用生态里的标准件，坏处是三套东西的运维面各不相同。

**GAIE 集成**上两者是同一个协议族的不同实现，不共享代码：Dynamo 的 `deploy/inference-gateway/ext-proc/` 与 llm-d EPP 都实现 Gateway API Inference Extension 的 ext_proc 协议。这意味着**理论上可以互换**——但换过去就会丢掉各自的私有能力（Dynamo 会丢精确 token，llm-d 会丢 flow control）。

> 对应 [00 篇 §3](00-总览与架构.md#3-架构的第一刀三个独立的通信平面)、[06 篇](06-部署与Operator.md) ｜ 对照 [llm-d Router 07](../llm-d/router/07-部署配方与排障.md)

## 8. 四处最值得记住的分歧

| 分歧点 | Dynamo | 对照方 |
|--------|--------|--------|
| **路由器在不在数据通路上** | 在——Frontend 兼做 tokenize，路由拿得到精确 token | llm-d EPP 只答「发给谁」，不碰 token |
| **打分怎么算** | 统一 cost 函数，overlap 从 prefill 工作量里减掉，全部项单位是 block | llm-d 多 scorer 加权求和，无量纲 |
| **副本数怎么定** | 性能模型正推，SLA 是容量搜索的约束 | WVA 用 token 供需双阈值倒推 |
| **KV 怎么共享** | 留在实例里点对点搬（NIXL RDMA） | Mooncake 抽进集群级共享池 |

这四条不是独立的选择，而是**从第一条派生出来的一条链**：因为 Frontend 在数据通路上，所以路由拿得到精确 token；因为有精确 token，所以打分能用 block 数这个统一量纲；因为要自己维护 KV 位置索引，所以 Router 必须重；因为 Router 已经知道谁有什么，所以 KV 不必抽进公共池。

反过来 llm-d 那条链也是自洽的：EPP 只在控制通路上，所以轻、能用 Go 写、故障半径小；拿不到精确 token，所以打分只能加权求和；KV 共享外包给 Mooncake，所以 Router 不用背索引。

**没有一条链是普遍更优的。** 选型时真正该问的是：你的入口能不能接受一个必经的 Rust 进程，以及你有没有精力维护一条性能曲线。

---

> **上一篇** [06 · 部署与 Operator](06-部署与Operator.md) ｜ **回到** [Dynamo 系列首页](README.md)
> **相关系列**：[llm-d](../llm-d/) ｜ [Mooncake](../../kvcache/mooncake/) ｜ [vLLM](../../inference-engine/vllm/) / [SGLang](../../inference-engine/sglang/)
