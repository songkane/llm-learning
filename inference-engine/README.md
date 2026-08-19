# 推理引擎（Inference Engine）

大模型推理引擎的架构与源码学习资料。聚焦「请求如何高效地从字符串走到 GPU 再吐字返回」这一核心问题，剖析调度、显存管理、批处理、分布式部署等关键机制。

## 分析的代码库版本

| 引擎 | 仓库 | 分析版本 |
|------|------|---------|
| SGLang | [`sgl-project/sglang`](https://github.com/sgl-project/sglang) | tag `v0.5.16`（2026-07-24） |
| vLLM | [`vllm-project/vllm`](https://github.com/vllm-project/vllm) | v1 架构（`vllm/v1/`） |

```bash
git clone https://github.com/sgl-project/sglang.git
cd sglang && git checkout v0.5.16
```

## 目录

| 引擎 | 目录 | 说明 |
|------|------|------|
| vLLM | [`vllm/`](vllm/) | vLLM v1 源码学习，从请求生命周期到多机 / PD 分离 |
| SGLang | [`sglang/`](sglang/) | SGLang（srt）源码学习，从数据流贯穿到 RadixAttention 与 PD 分离 |

> 两套笔记采用**统一的分析范式**（统一示例 A/B 请求、真实源码逐行注释、`【逻辑】`/`【Python】`双类注释、Python 语法速查表），便于横向对照两个引擎的设计取舍。

## vLLM 学习路线

推荐按编号顺序阅读，全套沿用统一示例（请求 A、B），层层递进：

| 篇 | 主题 | 核心问题 |
|----|------|---------|
| [00](vllm/00-全局架构与数据流总览.md) | 全局架构与数据流总览 | 整体分层、进程模型、统一示例与启动参数 |
| [01](vllm/01-请求的一生-主控制流.md) | 请求的一生 · 主控制流 | 请求怎么从字符串走到 GPU，step 心跳是什么 |
| [02](vllm/02-调度器-连续批处理.md) | 调度器 · 连续批处理 | 每步「挑谁进 batch」，显存不够怎么抢占 |
| [03](vllm/03-PagedAttention与KVCache.md) | PagedAttention 与 KV Cache | 显存怎么像操作系统分页一样管理，前缀怎么复用 |
| [04](vllm/04-Worker与模型执行.md) | Worker 与模型执行 | 变长请求怎么打平成张量，分页 attention 怎么落地 |
| [05](vllm/05-多机推理与PD分离.md) | 多机推理与 PD 分离 | 一台机器装不下怎么并行，prefill/decode 怎么拆到不同实例 |

> 01~04 篇讲**单实例、单机**内部机制；05 篇把镜头拉远到**多卡、多机、跨实例**部署。

## SGLang 学习路线

同样按编号顺序阅读，沿用统一示例（请求 A、B 共享前缀）：

| 篇 | 主题 | 核心问题 |
|----|------|---------|
| [00](sglang/00-导读与索引.md) | 导读与索引 | 全局地图、统一示例、**vLLM ↔ SGLang 机制对照表** |
| [01](sglang/01-全局架构总览.md) | 全局架构总览 | 多进程 + ZMQ 三段式架构，Scheduler 心跳是什么 |
| [02](sglang/02-数据流贯穿全局-Demo实例.md) | 数据流贯穿全局 · Demo | 文本如何一步步变成回复，每层数据结构怎么变形 |
| [03](sglang/03-核心模块源码深度分析.md) | 核心模块源码深度分析 | 调度决策怎么做，overlap 怎么重叠，前向怎么分派 |
| [04](sglang/04-启动参数详解.md) | 启动参数详解 | 每个参数什么意义、在哪个模块生效、怎么调优 |
| [05](sglang/05-部署场景-单机-多机-PD分离.md) | 部署场景 · 单机/多机/PD 分离 | 并行与 PD 分离的区别，多种机制如何叠加 |
| [06](sglang/06-RadixCache深度剖析.md) | RadixCache 深度剖析 ★ | 基数树前缀缓存怎么实现，lock_ref 如何保证共享安全 |

> 06 篇是 SGLang 的**招牌技术专篇**（对标 vLLM 的 03 PagedAttention 篇），是理解 SGLang 核心竞争力的关键。

## 两个引擎的核心差异（速览）

| 维度 | vLLM | SGLang |
|------|------|--------|
| 引擎心跳 | `EngineCore.step()` | `Scheduler.event_loop_normal/overlap()` |
| 前缀缓存 | **PagedAttention**：定长 block（16 token）+ 哈希链匹配 | **RadixAttention**：变长 key 的基数树 + 最长公共前缀路径 |
| 防误删机制 | `ref_cnt` 引用计数 | `lock_ref` 引用计数 + protected/evictable |
| Detokenize | 主进程内 | 独立子进程（拆分更彻底） |
| 天然优势 | 显存零碎片、实现规整 | 多轮/多分支的层级前缀共享更自然 |

> 详细对照见 [SGLang 00 篇第六节](sglang/00-导读与索引.md)。

## 再往下一层：KV Cache 基础设施

两套笔记的 PD 分离篇（vLLM 05 / SGLang 05）都停在「KV 通过 RDMA 传给 Decode 实例」；SGLang 的 RadixCache 也只活在**单实例 GPU 显存**里。这两个缺口由 [`kvcache/`](../kvcache/) 分类补上：

- [**Mooncake 源码学习**](../kvcache/mooncake/) —— KV 怎么跨实例搬运（Transfer Engine）、怎么跨实例与跨重启共享（Mooncake Store）。沿用同一组 A/B 请求示例，可直接与本目录笔记对照。
