# llm-learning

大模型（LLM）与 MaaS 平台学习资料合集。

按主题分类整理，记录学习过程中的架构分析、源码剖析、原理笔记与实践经验，偏向「建立直觉 + 对照源码」的深入理解，而非 API 罗列。

## 分类导航

| 分类 | 目录 | 内容 |
|------|------|------|
| 推理引擎 | [`inference-engine/`](inference-engine/) | 大模型推理引擎的架构与源码剖析（vLLM、SGLang） |
| KV Cache 基础设施 | [`kvcache/`](kvcache/) | KV Cache 的跨实例共享与跨节点传输（Mooncake） |
| 推理服务栈 | [`serving/`](serving/) | 引擎之上的集群协调：请求路由 + LLM 语义扩缩容 + P/D 编排（llm-d、NVIDIA Dynamo） |
| 调度与编排 | [`scheduling/`](scheduling/) | 训练/推理作业的资源调度与编排（kube-scheduler、Volcano、Kueue） |
| 弹性扩缩容 | [`autoscaling/`](autoscaling/) | 「该开几个副本」的问题域索引；通用 HPA/KEDA 笔记将落在这里 |

> 更多分类（训练、MaaS 平台、Agent、RAG 等）将持续补充。

## 已有内容

### 推理引擎

聚焦「请求如何高效地从字符串走到 GPU 再吐字返回」。

- [**vLLM 源码学习**](inference-engine/vllm/) —— 从「一个请求的一生」出发，逐层拆解 vLLM v1 的调度、KV Cache（PagedAttention）、Worker 执行、多机与 PD 分离机制。

- [**SGLang 源码学习**](inference-engine/sglang/) —— 从「数据流贯穿全局」出发，拆解 SGLang（srt）的多进程架构、Scheduler 调度、RadixAttention 前缀缓存、启动参数与单机/多机/PD 分离部署。附 **vLLM ↔ SGLang 机制对照表**，便于横向比较两个引擎的设计取舍。

> 两套笔记均沿用统一示例（A/B 两个共享前缀的请求）。想横向对比，推荐先各读 00 篇，再重点对照 vLLM 的 `03-PagedAttention` 与 SGLang 的 `06-RadixCache`——这是二者最核心的设计分歧点（定长 block + 哈希 vs 变长 key + 基数树）。

### KV Cache 基础设施

聚焦「KV Cache 怎么跨实例共享、跨节点搬运、跨介质分层」—— 补的正是上面两套引擎笔记「停住」的地方。

- [**Mooncake 源码学习**](kvcache/mooncake/) —— Kimi 生产实践（FAST'25 论文）的 KVCache-centric 分离式架构：**Transfer Engine**（多协议 RDMA、多网卡聚合、Topology 亲和选路）、**Mooncake Store**（集群级 KV 池、PutStart/PutEnd 两阶段写入、Master 元数据）、分层存储（io_uring / 3FS / SPDK）、租约与高可用（etcd 选主、oplog+snapshot），以及 vLLM KV Connector 与 SGLang HiCache L3 的集成方式。

> 一句话区分：**vLLM / SGLang 管「算」，Mooncake 管「存」与「搬」**。前两者的 PagedAttention / RadixAttention 解决「单卡显存怎么省」，Mooncake 解决「整个集群的 KV 怎么共享」—— 三者是叠加关系，不是替代关系。
>
> Mooncake 系列沿用与 vLLM / SGLang **完全一致的 A/B 请求示例**，读完可把三套笔记串成一条完整链路：请求进来 → 引擎算 KV → Mooncake 搬运与存储 → 下个请求命中前缀。详见 [kvcache/README](kvcache/README.md)。

### 调度与编排

聚焦「一堆 GPU、一堆队列、一堆作业，怎么在 Kubernetes 上被公平且高效地分配」。**不管请求发给哪个副本**——那是下一节服务栈的事。

- [**kube-scheduler 源码学习**](scheduling/kube-scheduler/) —— K8s 原生调度器，一切的地基：调度框架 15 个扩展点、三队列与 QueueingHint、增量快照与 assume、过滤采样与打分归一化、抢占六轮打分、DRA，以及 **v1.36 新引入的原生 gang 调度与拓扑感知 Placement**（Alpha）。另附两篇扩展实战：**自建插件**（Framework Plugin，每个扩展点一个可编译 demo）与**免编译扩展**（Extender / SchedulingGates / Webhook / DRA，用官方镜像零编译）。

- [**Volcano 源码学习**](scheduling/volcano/) —— Pod 级批调度器：Session/Action/Plugin 框架、Gang 调度事务机制、队列配额、拓扑感知、GPU 共享。

- [**Kueue 源码学习**](scheduling/kueue/) —— 作业级准入控制器：ClusterQueue/Cohort 配额借用、ResourceFlavor 异构机型、TAS 拓扑感知、MultiKueue 多集群。

> 一句话区分上面三者：**Kueue 决定「作业什么时候可以开始」，kube-scheduler 与 Volcano 决定「Pod 落到哪个节点」**（后两者按 `schedulerName` 分流、互斥）。建议先读 kube-scheduler 的 00~01 建立地基，再看另两个补了什么缺口。详见 [scheduling/README](scheduling/README.md)。

### 推理服务栈

聚焦「怎么把一堆引擎副本变成一个服务」：请求发给谁、开几个副本、P/D 怎么编排。**落盘按项目，检索按问题域**——同项目的 Router 与 Scaler 放在一起，[调度](scheduling/) 与 [扩缩容](autoscaling/) 只留索引。分类理由见 [serving/README](serving/README.md)。

- [**llm-d Router 源码学习**](serving/llm-d/router/) —— llm-d 的推理请求路由器（Endpoint Picker），工作在 request 层，以 Envoy **ext-proc** 形式给出每个请求的目标 pod。与传统 L7 LB 的区别在于它懂 LLM 的成本结构（**KV Cache 让后端强状态化**）：**插件化的调度框架**（8 类扩展点、Filter/Scorer/Picker 三段式、加权求和不归一化）、**Data Layer 指标采集**（每 endpoint 一个 goroutine、每 50ms 抓一次，按 `engine-type` 适配 vLLM/SGLang 的指标名差异）、**KV 前缀缓存亲和路由**（近似的路由历史 vs 精确的 vLLM ZMQ 事件流，`kvblock.Index` 与分片保序）、**Flow Control 多租户流控**（actor 模型、FlowKey 公平性、429/503 的精确语义、fail-closed 的饱和检测）、**P/D 分离编排**（sidecar 跑在 decode pod 里、6 种 KV connector 对照、E/P/D 多模态扇出）。末篇附**三个官方配方逐行讲 + 完整 sizing 数据 + 20 条静默失效模式总表**。

> Router 的全部代码在 `llm-d/llm-d-router` 一个仓库里（EPP、KV 索引、P/D sidecar、Coordinator），GIE 只提供 `InferencePool` API 与 Endpoint Picker 协议；组件构成见 [该系列 README](serving/llm-d/router/README.md#组件构成)。

- [**llm-d WVA 源码学习**](serving/llm-d/autoscaling/) —— llm-d 的 Workload Variant Autoscaler，工作在 scale 层，产出 `wva_desired_replicas` 指标供 HPA/KEDA 消费。带 LLM 语义的容量模型：**token 供需双阈值**（`RC = max(0, demand/0.85 − anticipated)` / `SC = max(0, supply − demand/0.70)`，0.70~0.85 是结构性死区）、**异构机型成本优化**（同模型多变体按 `cost/每副本容量` 择优）、**P/D 分离联合扩容**（Δ_util 匹配，避免单侧超扩）、**多 analyzer 投票与容量来源选择**（saturation / throughput、`T-sfz` 历史容量复用；QM 排队论实现当前禁用）、**缩容到零与 100ms 冷启动唤醒**。附一份[计算逻辑可视化速查（HTML）](serving/llm-d/autoscaling/wva-autoscaling-logic.html)。

> **WVA 决定「该有几个副本」，Router 决定「这个请求发给哪个副本」**。两者互不依赖，建议先读 Router —— WVA 的容量模型建立在对 KV token 供需的理解上，读过 Router 的 Data Layer（03 篇）会更顺。

- [**NVIDIA Dynamo 源码学习**](serving/dynamo/) —— 与 llm-d 同层的另一套服务栈（NVIDIA，Rust 核心 + Python 扩展）。单仓覆盖 Frontend、Router、Planner、KVBM、Operator：**三个可独立换传输的通信平面**（discovery / request / event，1.x 起 etcd 与 NATS 都不再必需）、**双向回环的 pipeline 算子链**（路由决策在 tokenize 之后，所以拿得到精确 token）、**overlap 作减法的打分公式**（各项单位统一为 block 数，命中即减免 prefill 工作量）、**SLA 驱动的容量搜索**（TTFT/ITL 是 batch size 搜索的约束而非阈值）、**弹性扩缩容的五阶段插件管道**（类型化提案合并 + 四道约束 clamp + 三道下发安全阀）、**KVBM 的 G1~G4 分层**（leader 只认 hash、worker 只搬字节）、**pull vs push 两种 KV 交接语义**，以及 **EndpointSlice × CR join 式的 K8s 原生发现**。

> Dynamo 与 llm-d 是同层的两种取舍。扩缩容这一维度的完整对比在 [08 · 扩缩容方法对比](serving/dynamo/08-扩缩容方法对比-与llm-d-WVA.md)：**正推（单副本容量 → 副本数）vs 倒推（供需比 → 缺口）**、单副本容量是**性能模型**给的还是**当前观测**推的、SLA 是**直接约束**还是**隐含在阈值里**、防抖动靠**模型预演**还是**静态死区**。其余维度（路由是否在数据通路上、打分是减法还是加权和、KV 是否抽进共享池）散见于各篇正文。

### 弹性扩缩容（问题域索引）

聚焦「推理服务到底该开几个副本、加在哪个机型上」—— 与调度是两个问题域：**扩缩容改 `spec.replicas`，调度写 `pod.spec.nodeName`**。LLM 语义的实现正文在 [`serving/`](serving/)；本分类保留 [索引与 HPA 失效点说明](autoscaling/README.md)，通用 HPA/KEDA 笔记将直接落在这里。

## 本地源码对照（`sources/`）

所有笔记都钉在具体版本上，建议把上游代码库同步到本地一起看：

```bash
./scripts/sync-sources.sh          # 检查清单，缺失的自动拉取（已存在的跳过）
./scripts/sync-sources.sh sglang   # 只检查指定的
./scripts/sync-sources.sh --list   # 只看清单，不做任何操作
```

| 代码库 | 目录 | 分析基线 |
|--------|------|---------|
| vLLM | `sources/vllm` | `main`（v1 架构 `vllm/v1/`） |
| SGLang | `sources/sglang` | `v0.5.16` |
| Mooncake | `sources/mooncake` | `v0.3.12.post1` |
| Kubernetes（kube-scheduler） | `sources/kubernetes` | `v1.36.3` |
| Volcano | `sources/volcano` | `v1.15.1` |
| Kueue | `sources/kueue` | `v0.19.1` |
| llm-d WVA | `sources/llm-d-autoscaling` | `release-0.9 @ d5d5864` |
| llm-d Router | `sources/llm-d-router` | `main @ 90a28bc` |
| NVIDIA Dynamo | `sources/dynamo` | `main @ 946acce` |

> WVA 使用 `release-0.9`（核对提交 `d5d5864`），与 `v0.9.0` tag 不同；详见 [版本对照](serving/llm-d/autoscaling/README.md#版本对照与复现)。
>
> Router 使用 `main`（核对提交 `90a28bc`），覆盖 `pkg/kvcache`、`pkg/kvevents`、`pkg/coordinator`；详见 [版本与复现](serving/llm-d/router/README.md#版本与复现)。
>
> `llm-d-autoscaling` 的 `go.mod` module 路径是 `github.com/llm-d/llm-d-workload-variant-autoscaler`，与仓库名不一致，`go get` 时以 module 路径为准。

**脚本只管「有没有」，不管「是哪个版本」**：已存在的仓库一律跳过，不做 `fetch`/`checkout`/`reset`；
缺失的用标准 `git clone` 拉全（完整历史 + 全部 tag + 全部远端分支），并切到上表基线作为起点。
之后的更新与版本对照都由你自己在目录里用正常 git 操作，脚本不会干扰分支状态和本地批注：

```bash
cd sources/kubernetes
git fetch --tags                                     # 跟进上游
git log --oneline -- pkg/scheduler/schedule_one.go   # 看某文件演进
git diff v1.35.0..v1.36.3 -- pkg/scheduler           # 跨版本对照
git blame pkg/scheduler/schedule_one.go              # 追某行来历
git checkout master                                  # 切分支或任意 tag
```

代码实体默认放在 `~/Desktop/code/icloud-code`，`sources` 是指向它的软链接（已在 `.gitignore` 忽略）。
换位置用 `LLM_SOURCES_DIR=/your/path ./scripts/sync-sources.sh`，脚本会自动重建软链接。

## 使用建议

每个子专题内部一般都有一篇 `00-` 开头的总览文档，建议从它读起，再按编号顺序深入。

全部笔记采用统一的分析范式：

- **统一示例贯穿全局**：每个专题用同一组示例，追踪同一份数据/作业在每一层如何被封装、拆解、变形。
- **真实源码 + 逐行注释**：摘录真实代码，注释分 `【逻辑】`（系统在做什么、为什么）和 `【Python】`/`【Go】`（语法讲解）两类。
- **图示优先**：架构图、进程模型图、时序图、状态机图，帮助建立空间直觉。
- **每篇附速查表**：语法速查 + 必记要点。
- **刻意横向对照**：同类项目（vLLM ↔ SGLang、kube-scheduler ↔ Volcano ↔ Kueue、llm-d Router ↔ Dynamo Router、WVA ↔ Dynamo Planner）使用一致的示例与结构，便于比较设计取舍。
