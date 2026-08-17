# 调度与编排（Scheduling & Orchestration）

大模型训练 / 推理的**资源调度与作业编排**学习资料。聚焦「一堆 GPU、一堆队列、一堆作业，怎么在 Kubernetes 上被公平且高效地分配」这一核心问题。

## 目录

| 项目 | 目录 | 源码基线 | 定位 | 说明 |
|------|------|---------|------|------|
| kube-scheduler | [`kube-scheduler/`](kube-scheduler/) | `v1.36.3` | **K8s 原生 Pod 级调度器** | 一切的地基：调度框架、扩展点、队列与缓存、过滤打分、抢占、DRA，以及 v1.36 新增的原生 gang / 拓扑感知（Alpha） |
| Volcano | [`volcano/`](volcano/) | `v1.15.1` | **Pod 级批调度器** | CNCF 项目，替代 kube-scheduler：Gang 调度、队列配额、拓扑感知、GPU 共享 |
| Kueue | [`kueue/`](kueue/) | `v0.19.1` | **作业级准入控制器** | Kubernetes SIG 项目：ClusterQueue/Cohort 配额借用、ResourceFlavor、TAS、MultiKueue |

> 三套文档都**钉在具体 release tag** 而非 master，所有函数名、字段名、代码片段都能在对应 tag 上逐字找到。`git checkout <tag>` 后即可边读边对照。版本差异与升级复查方法见各系列 00 篇的「附录：版本说明」。

## 建议阅读顺序

```
kube-scheduler 00~01   ← 先建立"调度器到底在做什么"的基础
      ↓
Volcano 00  +  Kueue 00   ← 各读一遍总览，理解它们分别补了什么缺口
      ↓
按需深入 02~03（源码细节）与 04~05（落地）
```

**为什么先读 kube-scheduler**：Volcano 直接复用了它的插件实现（`predicates`/`nodeorder` 插件 import 了 `k8s.io/kubernetes/pkg/scheduler/framework/plugins`），Kueue 则把 Pod 交还给它调度。不了解 `Filter`/`Score`/`Permit`/`PreEnqueue` 这套框架，读另外两个会缺一层地基。

## 三者的分工

一句话概括：

> **Kueue 决定「作业什么时候可以开始」**（改 `suspend` / 摘 scheduling gate）
> **kube-scheduler 与 Volcano 决定「Pod 落到哪个节点」**（写 `pod.spec.nodeName`，二者按 `schedulerName` 分流，互斥）

```mermaid
flowchart LR
    A["作业提交"] --> B["Kueue：够配额吗？<br/>（作业级准入）"]
    B -->|放行| C{"schedulerName?"}
    C -->|default-scheduler| D["kube-scheduler<br/>逐 Pod 选节点 + DRA"]
    C -->|volcano| E["Volcano<br/>成组落位 + GPU 共享 + NUMA"]
    D --> F["kubelet 拉起容器"]
    E --> F
```

### 能力矩阵

| 能力 | kube-scheduler v1.36 | Volcano v1.15.1 | Kueue v0.19.1 |
|------|---------------------|-----------------|---------------|
| 层次 | Pod 级调度 | Pod 级调度 | 作业级准入 |
| 调度单元 | 一个 Pod（v1.36 Alpha 可为 PodGroup） | PodGroup | Workload |
| 是否替换 kube-scheduler | — | ✅ 替换 | ❌ 不替换 |
| Gang | ⚠️ Alpha（Permit 型，**等待占资源**） | ✅ 事务型（Discard 立即释放） | ✅ 结构性（Pod 根本不创建） |
| 多租户配额 | ❌ 仅 `ResourceQuota`（硬上限不可借） | ✅ Queue（deserved/capability/guarantee） | ✅ ClusterQueue + Cohort 树 |
| 闲时借用 / 忙时抢回 | ❌ | ✅ | ✅ borrowingLimit / lendingLimit |
| 网络拓扑感知 | ⚠️ Alpha（Placement 机制） | ✅ HyperNode | ✅ TAS |
| GPU 显存/算力共享 | ❌ | ✅ deviceshare vGPU | ❌ |
| NUMA 感知 | ❌（kubelet 侧 TopologyManager） | ✅ numaaware | ❌ |
| DRA | ✅ **GA（v1.35 锁定）** | 配额侧对接 | 配额侧对接 |
| 多集群 | ❌ | ❌ | ✅ MultiKueue |
| 弹性作业 | ❌ | ✅ minAvailable<replicas | ✅ workload slice |
| 节点打分插件生态 | ✅ **标准来源** | ✅ 复用 + 自有 | ❌ 不做 |
| 抢占「白杀」风险 | ⚠️ 存在（单 Pod 视角） | ✅ 无（JobPipelined 才 Commit） | ✅ 无 |
| 生产成熟度 | ✅✅✅ | ✅✅ | ✅✅ |

### 选型建议

| 场景 | 方案 |
|------|------|
| 小规模、单租户、作业不大 | kube-scheduler + 装箱 profile + `ImageLocality` 加权 |
| 多租户配额 + 作业排队 + 异构机型 + 多集群 | **Kueue**（+ 开 TAS 规避碎片） |
| 节点级精细调度：GPU 共享、NUMA、自定义打分 | **Volcano** |
| 大型 MaaS 平台 | **Kueue（准入）+ Volcano（落位）** 叠加 |

叠加时的三个注意事项（详见 [Kueue 04 篇 §4.3](kueue/04-面向大模型训练与推理的能力地图.md)）：抢占只在一边做、拓扑只用一套、gang 叠加正好互补。

## kube-scheduler 学习路线

| 篇 | 主题 | 核心问题 |
|----|------|---------|
| [00](kube-scheduler/00-kube-scheduler总览与架构.md) | 总览与架构 | 定位、三方对比、组件、Pod 的一生、**v1.36 原生 gang / 拓扑感知** |
| [01](kube-scheduler/01-核心原理-调度周期与扩展点.md) | 调度周期与扩展点 | 15 个扩展点何时跑、七种 Status 语义、CycleState、MultiPoint |
| [02](kube-scheduler/02-核心代码分析-调度队列与缓存.md) | 调度队列与缓存 | 三队列流转、QueueingHint（GA）、增量快照、assume/forget/expire |
| [03](kube-scheduler/03-核心代码分析-过滤打分与抢占.md) | 过滤打分与抢占 | 节点采样公式、打分归一化、抢占五步与六轮打分 |
| [04](kube-scheduler/04-面向大模型场景的能力与局限.md) | 大模型场景 | 能做好什么（装箱/DRA/打散）、做不到什么、三种落地形态 |
| [05](kube-scheduler/05-实战Demo.md) | 实战 Demo | kind 集群 + 假 GPU：装箱、多 profile、抢占、拓扑打散、**开 gang gate 实测** |

## Volcano 学习路线

沿用统一示例（64 卡 GPU 集群 + `train`/`serve` 两队列 + 训练作业 T / 推理服务 S / 调试 Pod D）：

| 篇 | 主题 | 核心问题 |
|----|------|---------|
| [00](volcano/00-Volcano总览与架构.md) | 总览与架构 | 为什么 kube-scheduler 不够用，组件与 CRD 模型，作业的一生 |
| [01](volcano/01-核心原理-Session与Action-Plugin框架.md) | Session / Action / Plugin | 每秒的调度循环、37 个扩展点、Gang 的事务机制 |
| [02](volcano/02-核心代码分析-Actions.md) | Actions | enqueue / allocate / preempt / reclaim / backfill / gangpreempt |
| [03](volcano/03-核心代码分析-关键插件.md) | 关键插件 | gang / capacity / proportion / deviceshare / network-topology-aware |
| [04](volcano/04-面向大模型训练与推理的能力地图.md) | 大模型能力地图 | 训练/推理/训推混部配置，坑与取舍 |
| [05](volcano/05-实战Demo.md) | 实战 Demo | 队列 → PyTorch 训练 → vLLM 推理 → vGPU 共享 → 抢占回收 |

## Kueue 学习路线

沿用与 Volcano **刻意一致**的示例（on-demand / spot 两种机型，`team-a`/`team-b` 共享一个 Cohort）：

| 篇 | 主题 | 核心问题 |
|----|------|---------|
| [00](kueue/00-Kueue总览与架构.md) | 总览与架构 | Kueue 是什么、与 Volcano 的边界、CRD 模型、作业的一生 |
| [01](kueue/01-核心原理-Workload生命周期与配额模型.md) | Workload 与配额模型 | 借用/借出账本、抢占策略矩阵、FlavorFungibility、公平共享 |
| [02](kueue/02-核心代码分析-调度器与准入.md) | 调度器与准入 | `schedule()` 六步、flavorassigner、preemption |
| [03](kueue/03-核心代码分析-缓存快照与控制器.md) | 缓存与控制器 | 资源账本树、等待队列、jobframework、TAS / MultiKueue / Elastic Jobs |
| [04](kueue/04-面向大模型训练与推理的能力地图.md) | 大模型能力地图 | 训练/推理/混部，与 Volcano 的三种组合形态 |
| [05](kueue/05-实战Demo.md) | 实战 Demo | 配额 → JobSet+TAS 训练 → LWS 推理 → 借用回收 → 部分准入 |

---

> 各系列内部：00~01 建立框架直觉；02~03 是源码细节，可按需查阅；04~05 面向落地。
