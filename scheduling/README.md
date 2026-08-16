# 调度与编排（Scheduling & Orchestration）

大模型训练 / 推理的**资源调度与作业编排**学习资料。聚焦「一堆 GPU、一堆队列、一堆作业，怎么在 Kubernetes 上被公平且高效地分配」这一核心问题。

## 目录

| 项目 | 目录 | 定位 | 说明 |
|------|------|------|------|
| Volcano | [`volcano/`](volcano/) | **Pod 级批调度器** | CNCF 批量计算调度系统：Gang 调度、队列配额、拓扑感知、GPU 共享 |
| Kueue | [`kueue/`](kueue/) | **作业级准入控制器** | Kubernetes SIG 项目：ClusterQueue/Cohort 配额借用、ResourceFlavor、TAS 拓扑感知、MultiKueue 多集群 |

## 先搞清楚两者的分工

这是最常见的困惑，一句话概括：

> **Kueue 决定「作业什么时候可以开始」（改 `suspend` / 摘 scheduling gate），Volcano 决定「Pod 落到哪个节点」（自己实现调度器并 Bind）。**

| 维度 | Kueue | Volcano |
|------|-------|---------|
| 层次 | 作业级准入（admission） | Pod 级调度（scheduling） |
| 是否替换 kube-scheduler | 不替换 | 替换（`schedulerName: volcano`） |
| Gang 语义 | 结构性保证（Workload 不可分割，装不下则 Pod 一个都不建） | 事务式（`Statement` + `JobReady` 才 Commit） |
| 配额模型 | `nominalQuota` / `borrowingLimit` / `lendingLimit` + Cohort 树 | `deserved` / `capability` / `guarantee` + 层级队列 |
| 异构资源 | ResourceFlavor（一等公民，可 fallback） | 多队列 + nodegroup 间接实现 |
| 节点级能力 | 无（不做 predicates / 打分 / GPU 共享） | 有（predicates、binpack、NUMA、vGPU 共享） |
| 多集群 | MultiKueue 原生支持 | 无 |
| 二者关系 | **互补，可叠加**：Kueue 管准入，放行后的 Pod 交给 Volcano 调度 | |

选型建议：

- 需要**多租户配额 + 作业排队 + 异构机型 fallback + 多集群** → Kueue
- 需要**节点级精细调度**（GPU 共享、NUMA、binpack、自定义打分插件） → Volcano
- 大型 MaaS 平台 → 两者叠加（详见 [Kueue 04 篇 §4.3](kueue/04-面向大模型训练与推理的能力地图.md)）

## Volcano 学习路线

全套沿用统一示例（64 卡 GPU 集群 + `train`/`serve` 两个队列 + 训练作业 T / 推理服务 S / 调试 Pod D）：

| 篇 | 主题 | 核心问题 |
|----|------|---------|
| [00](volcano/00-Volcano总览与架构.md) | 总览与架构 | 为什么 kube-scheduler 不够用，组件与 CRD 模型，一个作业的一生 |
| [01](volcano/01-核心原理-Session与Action-Plugin框架.md) | Session / Action / Plugin 框架 | 每一秒的调度循环里发生了什么，37 个扩展点怎么串起来，Gang 的事务机制 |
| [02](volcano/02-核心代码分析-Actions.md) | Actions 源码分析 | enqueue / allocate / preempt / reclaim / backfill / gangpreempt 的实现 |
| [03](volcano/03-核心代码分析-关键插件.md) | 关键插件源码分析 | gang / capacity / proportion / deviceshare / network-topology-aware |
| [04](volcano/04-面向大模型训练与推理的能力地图.md) | 大模型能力地图 | 训练要什么、推理要什么、训推混部怎么配，坑与取舍 |
| [05](volcano/05-实战Demo.md) | 实战 Demo | 安装 → 队列 → PyTorch 训练 → vLLM 推理 → vGPU 共享 → 抢占回收 → 排障 |

## Kueue 学习路线

沿用与 Volcano 系列**刻意一致**的示例（64 卡集群，on-demand / spot 两种机型，`team-a`/`team-b` 两个队列共享一个 Cohort），便于横向对比：

| 篇 | 主题 | 核心问题 |
|----|------|---------|
| [00](kueue/00-Kueue总览与架构.md) | 总览与架构 | Kueue 是什么、与 Volcano 的边界、CRD 模型、作业的一生 |
| [01](kueue/01-核心原理-Workload生命周期与配额模型.md) | Workload 生命周期与配额模型 | 借用/借出账本怎么算、抢占策略矩阵、FlavorFungibility、公平共享 |
| [02](kueue/02-核心代码分析-调度器与准入.md) | 调度器与准入 | `schedule()` 六步、flavorassigner、preemption 的实现 |
| [03](kueue/03-核心代码分析-缓存快照与控制器.md) | 缓存/快照与控制器 | 资源账本树、等待队列、jobframework、TAS / MultiKueue / Elastic Jobs |
| [04](kueue/04-面向大模型训练与推理的能力地图.md) | 大模型能力地图 | 训练要什么、推理要什么、与 Volcano 的三种组合形态 |
| [05](kueue/05-实战Demo.md) | 实战 Demo | 安装 → 配额 → JobSet+TAS 训练 → LWS 推理 → 借用回收 → 部分准入 → 排障 |

> 阅读建议：00~01 建立整体框架直觉；02~03 是源码细节，可按需查阅；04~05 面向落地。两个项目的 00 篇都包含对彼此的边界说明，建议先各读一遍 00。
