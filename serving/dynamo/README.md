# NVIDIA Dynamo 源码学习

> **源码基线**：[`main @ 946acce`](https://github.com/ai-dynamo/dynamo/tree/946accea5edfd778f5120a3096b082e54e5bce2b)（2026-09-07 提交，2026-09-08 拉取核对，workspace 版本 `1.5.0`）。
> 本地对照：`sources/dynamo`（`./scripts/sync-sources.sh dynamo`）
> 上游定位：引擎之上的数据中心级推理编排层——**不替代** vLLM / SGLang / TensorRT-LLM，把它们协调成可路由、可扩缩、可做 P/D 分离的系统。

## 一句话定位

> **Dynamo 是引擎之上的编排层，形态是单仓多组件**：Frontend / Router / Planner / KVBM / Operator 住在同一个仓库、共享同一套运行时。Rust 写核心，Python 做扩展层。1.x 起 etcd 和 NATS 都不再是必需依赖。

最具决定性的一条设计：**Frontend 自带 HTTP 与 tokenize / detokenize，请求和响应都穿过它**。由此派生出「路由拿得到精确 token」「打分能用统一量纲」「Router 必须兼任 KV 位置索引」这一整条链。

> ⚠️ **两个最容易被误解的默认值**（详见 [03 篇 §0](03-核心代码分析-SLA-Planner.md#0-一句话概括)）：Planner 的 `optimization_target` 默认是 `throughput`（**静态阈值 easy mode**，不是 SLA 性能模型）；`enable_load_scaling` 默认 `False`（**默认只跑 180s 的慢环**）。「装上 Planner 就自动有 SLA 驱动的弹性扩缩容」是错的，两者都要显式打开。

## 为什么不拆成 router/ 和 planner/ 两个目录

Router、Planner、KVBM 在同一个仓库里共享发现、消息面和部署模型，拆开会把「一条请求怎么从 Frontend 走到 Worker、指标又怎么回到 Planner」写断。相关索引另见 [`scheduling/`](../../scheduling/) 与 [`autoscaling/`](../../autoscaling/)。

## 学习路线

**00~06 只讲 Dynamo 自身的代码**，跨项目的对比集中在 07 篇。统一示例沿用本仓库的 A/B 共享前缀请求，加上 P/D 分离的 worker 池（prefill `P1/P2` + decode `D1/D2/D3`），角色命名与其余几套笔记一致。

| 篇 | 主题 | 核心问题 |
|----|------|---------|
| [00](00-总览与架构.md) | 总览与架构 | 整仓地图、**三个独立通信平面**、四级寻址、请求的九步 |
| [01](01-请求的一生-主控制流.md) | 请求的一生 | Frontend 装配线、模型怎么被发现、**双向回环 pipeline**、TCP call-home |
| [02](02-核心代码分析-KV感知路由.md) | KV-aware Router | **打分公式（overlap 作减法）**、radix 索引、事件 vs 预测、conditional disagg、插件三段式 |
| [03](03-核心代码分析-SLA-Planner.md) | SLA Planner | **TTFT/ITL 作为容量搜索的约束**、弹性扩缩容全景（七段链 / 三类指标 / 五道防振荡）、双环扩缩、P/D 预算 clamp、DGDSA 落地、DGDR 零配置 |
| [04](04-核心代码分析-KVBM分层KV管理.md) | KVBM | G1~G4 分层、Leader/Worker 分工、offload 流水线、块的身份 |
| [05](05-核心代码分析-PD分离与NIXL.md) | P/D 分离与 NIXL | **pull（NIXL）vs push（Mooncake）两种交接语义**、三后端差异、sidecar、E/P/D |
| [06](06-部署与Operator.md) | 部署与 Operator | 六个 CRD、DGD reconcile、**EndpointSlice × CR 的 join 式发现**、GAIE |
| [07](07-横向对比-与llm-d和Mooncake.md) | **横向对比** | 与 [llm-d](../llm-d/) / [Mooncake](../../kvcache/mooncake/) 的四处结构性分歧 |

### 横向对比在最后一篇

[07 · 横向对比](07-横向对比-与llm-d和Mooncake.md) 集中回答「Dynamo 和 llm-d / Mooncake 差在哪」，四个落点：

| 问题 | Dynamo | 对面 |
|------|--------|------|
| 路由器在不在数据通路上 | **在**（Frontend 兼做 tokenize，路由拿得到精确 token） | llm-d EPP 只答「发给谁」，不碰 token |
| 打分怎么算 | **统一 cost 函数**，overlap 从 prefill 工作量里减掉，全部项单位是 block | llm-d 多 scorer 加权求和，无量纲 |
| 副本数怎么定 | **性能模型正推**：SLA 是容量搜索的约束（`optimization_target: sla`；默认档是静态阈值） | WVA 用 token 供需双阈值倒推 |
| KV 怎么共享 | **留在实例里点对点搬**（NIXL RDMA） | Mooncake 抽进集群级共享池 |

这四条不是独立选择，而是从第一条派生出来的一条链——07 篇 §8 展开这个推导。

## 复现

```bash
./scripts/sync-sources.sh dynamo
cd sources/dynamo && git log -1 --format='%h %cd'   # 946accea5e Mon Sep 7 22:01:03 2026 +0000
```

Dynamo 迭代很快，目录结构隔几个月就会动——看外部资料前先核对文中提到的路径是否还在。

## 不在本系列里的上游仓库

| 仓库 | 做什么 | 若要分析，放哪 |
|------|--------|----------------|
| [`ai-dynamo/grove`](https://github.com/ai-dynamo/grove) | K8s 拓扑感知 gang 调度（NVL72 等） | [`scheduling/`](../../scheduling/) |
| [`ai-dynamo/modelexpress`](https://github.com/ai-dynamo/modelexpress) | GPU 间权重流式加载、加速冷启动 | 可另开专题，或作为本系列附录 |

---

> **相关系列**：[llm-d](../llm-d/) ｜ [Mooncake](../../kvcache/mooncake/) ｜ [vLLM](../../inference-engine/vllm/) / [SGLang](../../inference-engine/sglang/)
> **回到** [推理服务栈](../README.md)
