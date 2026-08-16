# 调度与编排（Scheduling & Orchestration）

大模型训练 / 推理的**资源调度与作业编排**学习资料。聚焦「一堆 GPU、一堆队列、一堆作业，怎么在 Kubernetes 上被公平且高效地分配」这一核心问题。

## 目录

| 项目 | 目录 | 说明 |
|------|------|------|
| Volcano | [`volcano/`](volcano/) | CNCF 批量计算调度系统源码学习：Gang 调度、队列配额、拓扑感知、GPU 共享 |

## Volcano 学习路线

推荐按编号顺序阅读，全套沿用统一示例（64 卡 GPU 集群 + `train`/`serve` 两个队列 + 训练作业 T / 推理服务 S / 调试 Pod D）：

| 篇 | 主题 | 核心问题 |
|----|------|---------|
| [00](volcano/00-Volcano总览与架构.md) | 总览与架构 | 为什么 kube-scheduler 不够用，组件与 CRD 模型，一个作业的一生 |
| [01](volcano/01-核心原理-Session与Action-Plugin框架.md) | Session / Action / Plugin 框架 | 每一秒的调度循环里发生了什么，37 个扩展点怎么串起来，Gang 的事务机制 |
| [02](volcano/02-核心代码分析-Actions.md) | Actions 源码分析 | enqueue / allocate / preempt / reclaim / backfill / gangpreempt 的实现 |
| [03](volcano/03-核心代码分析-关键插件.md) | 关键插件源码分析 | gang / capacity / proportion / deviceshare / network-topology-aware |
| [04](volcano/04-面向大模型训练与推理的能力地图.md) | 大模型能力地图 | 训练要什么、推理要什么、训推混部怎么配，坑与取舍 |
| [05](volcano/05-实战Demo.md) | 实战 Demo | 安装 → 队列 → PyTorch 训练 → vLLM 推理 → vGPU 共享 → 抢占回收 → 排障 |

> 00~01 建立整体框架直觉；02~03 是源码细节，可按需查阅；04~05 面向落地。
