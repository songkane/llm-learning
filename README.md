# llm-learning

大模型（LLM）与 MaaS 平台学习资料合集。

按主题分类整理，记录学习过程中的架构分析、源码剖析、原理笔记与实践经验，偏向「建立直觉 + 对照源码」的深入理解，而非 API 罗列。

## 分类导航

| 分类 | 目录 | 内容 |
|------|------|------|
| 推理引擎 | [`inference-engine/`](inference-engine/) | 大模型推理引擎的架构与源码剖析（vLLM、SGLang） |
| 调度与编排 | [`scheduling/`](scheduling/) | 训练/推理作业的资源调度与编排（kube-scheduler、Volcano、Kueue） |

> 更多分类（训练、MaaS 平台、Agent、RAG 等）将持续补充。

## 已有内容

### 推理引擎

聚焦「请求如何高效地从字符串走到 GPU 再吐字返回」。

- [**vLLM 源码学习**](inference-engine/vllm/) —— 从「一个请求的一生」出发，逐层拆解 vLLM v1 的调度、KV Cache（PagedAttention）、Worker 执行、多机与 PD 分离机制。

- [**SGLang 源码学习**](inference-engine/sglang/) —— 从「数据流贯穿全局」出发，拆解 SGLang（srt）的多进程架构、Scheduler 调度、RadixAttention 前缀缓存、启动参数与单机/多机/PD 分离部署。附 **vLLM ↔ SGLang 机制对照表**，便于横向比较两个引擎的设计取舍。

> 两套笔记均沿用统一示例（A/B 两个共享前缀的请求）。想横向对比，推荐先各读 00 篇，再重点对照 vLLM 的 `03-PagedAttention` 与 SGLang 的 `06-RadixCache`——这是二者最核心的设计分歧点（定长 block + 哈希 vs 变长 key + 基数树）。

### 调度与编排

聚焦「一堆 GPU、一堆队列、一堆作业，怎么在 Kubernetes 上被公平且高效地分配」。

- [**kube-scheduler 源码学习**](scheduling/kube-scheduler/) —— K8s 原生调度器，一切的地基：调度框架 15 个扩展点、三队列与 QueueingHint、增量快照与 assume、过滤采样与打分归一化、抢占六轮打分、DRA，以及 **v1.36 新引入的原生 gang 调度与拓扑感知 Placement**（Alpha）。另附两篇扩展实战：**自建插件**（Framework Plugin，每个扩展点一个可编译 demo）与**免编译扩展**（Extender / SchedulingGates / Webhook / DRA，用官方镜像零编译）。

- [**Volcano 源码学习**](scheduling/volcano/) —— Pod 级批调度器：Session/Action/Plugin 框架、Gang 调度事务机制、队列配额、拓扑感知、GPU 共享。

- [**Kueue 源码学习**](scheduling/kueue/) —— 作业级准入控制器：ClusterQueue/Cohort 配额借用、ResourceFlavor 异构机型、TAS 拓扑感知、MultiKueue 多集群。

> 一句话区分：**Kueue 决定「作业什么时候可以开始」，kube-scheduler 与 Volcano 决定「Pod 落到哪个节点」**（后两者按 `schedulerName` 分流、互斥）。建议先读 kube-scheduler 的 00~01 建立地基，再看另两个补了什么缺口。详见 [scheduling/README](scheduling/README.md)。

## 使用建议

每个子专题内部一般都有一篇 `00-` 开头的总览文档，建议从它读起，再按编号顺序深入。

全部笔记采用统一的分析范式：

- **统一示例贯穿全局**：每个专题用同一组示例，追踪同一份数据/作业在每一层如何被封装、拆解、变形。
- **真实源码 + 逐行注释**：摘录真实代码，注释分 `【逻辑】`（系统在做什么、为什么）和 `【Python】`/`【Go】`（语法讲解）两类。
- **图示优先**：架构图、进程模型图、时序图、状态机图，帮助建立空间直觉。
- **每篇附速查表**：语法速查 + 必记要点。
- **刻意横向对照**：同类项目（vLLM ↔ SGLang、kube-scheduler ↔ Volcano ↔ Kueue）使用一致的示例与结构，便于比较设计取舍。
