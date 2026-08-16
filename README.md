# llm-learning

大模型（LLM）与 MaaS 平台学习资料合集。

按主题分类整理，记录学习过程中的架构分析、源码剖析、原理笔记与实践经验，偏向「建立直觉 + 对照源码」的深入理解，而非 API 罗列。

## 分类导航

| 分类 | 目录 | 内容 |
|------|------|------|
| 推理引擎 | [`inference-engine/`](inference-engine/) | 大模型推理引擎的架构与源码剖析（vLLM 等） |
| 调度与编排 | [`scheduling/`](scheduling/) | 大模型训练/推理的资源调度与作业编排（Volcano、Kueue 等） |

> 更多分类（训练框架、MaaS 平台、Agent、RAG 等）将持续补充。

## 已有内容

### 推理引擎

- [**vLLM 源码学习**](inference-engine/vllm/) —— 从「一个请求的一生」出发，逐层拆解 vLLM v1 的调度、KV Cache、Worker 执行、多机与 PD 分离机制。全套沿用统一示例（A/B 两个请求），建立可贯穿全局的直觉。

### 调度与编排

- [**Volcano 源码学习**](scheduling/volcano/) —— 从「kube-scheduler 为什么不够用」出发，拆解 Volcano 的 Session/Action/Plugin 三层框架、Gang 调度的事务机制、队列配额与抢占回收、网络拓扑感知与 GPU 共享，并给出训练 + 推理 + 训推混部的完整 Demo。
- [**Kueue 源码学习**](scheduling/kueue/) —— 从「作业级准入」这一定位出发，拆解 Workload 生命周期、ClusterQueue/Cohort 的借用与借出账本、flavorassigner 与抢占算法、TAS 拓扑感知与 MultiKueue 多集群，并给出训练 + 推理 + 借用回收的完整 Demo。与 Volcano 系列沿用同一示例，可横向对比。

## 使用建议

每个子专题内部一般都有一篇 `00-` 开头的总览文档，建议从它读起，再按编号顺序深入。
