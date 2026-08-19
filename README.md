# llm-learning

大模型（LLM）与 MaaS 平台学习资料合集。

按主题分类整理，记录学习过程中的架构分析、源码剖析、原理笔记与实践经验，偏向「建立直觉 + 对照源码」的深入理解，而非 API 罗列。

## 分类导航

| 分类 | 目录 | 内容 |
|------|------|------|
| 推理引擎 | [`inference-engine/`](inference-engine/) | 大模型推理引擎的架构与源码剖析（vLLM、SGLang） |
| KV Cache 基础设施 | [`kvcache/`](kvcache/) | KV Cache 的跨实例共享与跨节点传输（Mooncake） |
| 调度与编排 | [`scheduling/`](scheduling/) | 训练/推理作业的资源调度与编排（kube-scheduler、Volcano、Kueue） |

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

聚焦「一堆 GPU、一堆队列、一堆作业，怎么在 Kubernetes 上被公平且高效地分配」。

- [**kube-scheduler 源码学习**](scheduling/kube-scheduler/) —— K8s 原生调度器，一切的地基：调度框架 15 个扩展点、三队列与 QueueingHint、增量快照与 assume、过滤采样与打分归一化、抢占六轮打分、DRA，以及 **v1.36 新引入的原生 gang 调度与拓扑感知 Placement**（Alpha）。另附两篇扩展实战：**自建插件**（Framework Plugin，每个扩展点一个可编译 demo）与**免编译扩展**（Extender / SchedulingGates / Webhook / DRA，用官方镜像零编译）。

- [**Volcano 源码学习**](scheduling/volcano/) —— Pod 级批调度器：Session/Action/Plugin 框架、Gang 调度事务机制、队列配额、拓扑感知、GPU 共享。

- [**Kueue 源码学习**](scheduling/kueue/) —— 作业级准入控制器：ClusterQueue/Cohort 配额借用、ResourceFlavor 异构机型、TAS 拓扑感知、MultiKueue 多集群。

> 一句话区分：**Kueue 决定「作业什么时候可以开始」，kube-scheduler 与 Volcano 决定「Pod 落到哪个节点」**（后两者按 `schedulerName` 分流、互斥）。建议先读 kube-scheduler 的 00~01 建立地基，再看另两个补了什么缺口。详见 [scheduling/README](scheduling/README.md)。

## 本地源码对照（`sources/`）

所有笔记都钉在具体版本上，建议把上游代码库同步到本地一起看：

```bash
./scripts/sync-sources.sh          # 检查清单，缺失的自动拉取（已存在的跳过）
./scripts/sync-sources.sh sglang   # 只检查指定的
./scripts/sync-sources.sh --list   # 只看清单，不做任何操作
```

| 代码库 | 目录 | 版本基线 | commits | 占用 |
|--------|------|---------|---------|------|
| vLLM | `sources/vllm` | `main`（v1 架构 `vllm/v1/`） | 20140 | 377M |
| SGLang | `sources/sglang` | `v0.5.16` | 15476 | 388M |
| Mooncake | `sources/mooncake` | `v0.3.12.post1` | 1660 | 129M |
| Kubernetes（kube-scheduler） | `sources/kubernetes` | `v1.36.3` | 137129 | 1.6G |
| Volcano | `sources/volcano` | `v1.15.1` | 6620 | 152M |
| Kueue | `sources/kueue` | `v0.19.1` | 6987 | 267M |

合计约 2.9G。

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

**存放位置**：代码实体在 **`~/Desktop/code/icloud-code`**（iCloud 之外），仓库内的 `sources` 是指向它的软链接，日常仍用 `sources/xxx` 路径。
换位置用 `LLM_SOURCES_DIR=/your/path ./scripts/sync-sources.sh`，脚本会自动重建软链接。

> 为什么不放 iCloud 里：2.8G 会被上传；且 iCloud「优化存储」会把不常用文件驱逐成占位符，`.git/objects` 一旦被驱逐，git 会卡顿甚至报错。
> `sources` 已在 `.gitignore` 忽略，不纳入版本管理；换机器时重新跑一次脚本即可。

## 使用建议

每个子专题内部一般都有一篇 `00-` 开头的总览文档，建议从它读起，再按编号顺序深入。

全部笔记采用统一的分析范式：

- **统一示例贯穿全局**：每个专题用同一组示例，追踪同一份数据/作业在每一层如何被封装、拆解、变形。
- **真实源码 + 逐行注释**：摘录真实代码，注释分 `【逻辑】`（系统在做什么、为什么）和 `【Python】`/`【Go】`（语法讲解）两类。
- **图示优先**：架构图、进程模型图、时序图、状态机图，帮助建立空间直觉。
- **每篇附速查表**：语法速查 + 必记要点。
- **刻意横向对照**：同类项目（vLLM ↔ SGLang、kube-scheduler ↔ Volcano ↔ Kueue）使用一致的示例与结构，便于比较设计取舍。
