# 04 · KVBM：显存放不下的 KV 去哪了

> **源码基线**：`main @ 946acce`。本篇讲 KVBM 怎么解「显存放不下 KV」：四层存储怎么分、leader 与 worker 怎么分工、块怎么一层层降下去。
> 主要文件：`lib/kvbm-{common,config,logical,physical,engine,kernels,consolidator}/`、`lib/llm/src/block_manager/`
> **先读上游自带的设计文档**：`lib/kvbm-engine/docs/`（architecture / offload / onboarding / leader / worker 等 11 篇），本篇大量与之交叉验证。

## 0. 先把七个 crate 的关系理清

KVBM 正从 `lib/llm/src/block_manager/` 拆成七个独立 crate。拆分的依据在 `docs/onboarding.md` 里说得很直白：

> The central design tension is between **logical** and **physical**. Leaders think in sequence hashes and block identities — they never touch raw memory. Workers think in layout handles, transfer managers, and DMA descriptors — they never make placement decisions.

**「决定搬什么」和「实际搬字节」被彻底分开**，七个 crate 就是按这条线切的：

| crate | 站在哪一边 | 内容 |
|---|---|---|
| `kvbm-common` | 中间 | 就三个类型定义：`BlockId`、`SequenceHash`、`LogicalLayoutHandle` |
| `kvbm-config` | — | 缓存容量、NIXL、事件、messenger 配置 |
| `kvbm-logical` | **逻辑** | 块元数据、`BlockRegistry`、`BlockManager` |
| `kvbm-physical` | **物理** | `PhysicalLayout`、`TransferManager`、NIXL / memcpy / CUDA 执行 |
| `kvbm-engine` | **缝合** | Leader / Worker、offload 流水线、G4 对象存储、runtime |
| `kvbm-kernels` | 物理 | 校验等 kernel |
| `kvbm-consolidator` | 逻辑 | 逻辑层合并 |

演进轨迹（`git log`）：`3998fdcb28`（2025-10）「KVBM V2 Initial Migration」先在 `lib/llm/src/block_manager/v2/` 里搭起 v2；`008683d6e0`、`9ab148dcb9`（2026-04）才把它拆成独立 workspace crate。

> **注意 `lib/llm/src/block_manager/` 还在**，v1 和 v2 实现都留着。新的分布式引擎走 `kvbm-engine`，旧路径和大量测试仍引用 `dynamo_llm::block_manager::v2`。**这是拆分进行中的状态，不是两套并行方案**——读代码时以 `kvbm-*` 为准，`block_manager/` 当作过渡期的集成面。

## 1. 四层：G1 到 G4

```13:27:lib/kvbm-common/src/lib.rs
/// KVBM manages G1, G2 and G3 layouts directly. G4 is managed by an external service.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LogicalLayoutHandle {
    /// Representation of GPU / Device Memory
    G1,
    /// Representation of CPU / Host Memory
    G2,
    /// Representation of Disk Storage (Local or AttachedStorage)
    G3,
    /// Representation of Blocks held in an external service
    G4,
}
```

官方给的量级表（`kvbm-engine/docs/architecture.md`）：

| 层 | 介质 | 延迟 | 容量 | 角色 |
|---|---|---|---|---|
| **G1** | GPU HBM | ~ns | 最小 | attention kernel 正在用的活跃 KV |
| **G2** | Pinned DRAM | ~μs | 中 | **RDMA 传输的中转站**、层间提升的暂存区 |
| **G3** | NVMe / SSD | ~ms | 大 | 温块持久化 |
| **G4** | S3 / MinIO | ~100ms | 无限 | 冷块归档 |

三个要点：

1. **G4 不归 KVBM 管**（注释里写死了）。KVBM 直接管 G1~G3，G4 是外部服务，只通过 `kvbm-engine/src/object/` 的客户端访问。
2. **G2 是枢纽，不只是「比 G1 慢一档的缓存」**。pinned DRAM 是 RDMA 能直接读写的内存，所有跨节点搬运都必须落地 G2 —— 这解释了为什么 offload 流水线只有 `G2→G3` 和 `G2→G4` 两条（见 §3），而没有 G1 直接到 G3。
3. 延迟跨度从 ns 到 100ms，**七个数量级**。这就是 [02 篇](02-核心代码分析-KV感知路由.md#13-分层缓存的折价) 里 Router 那三个折价系数（device 1.0 / host 0.75 / disk 0.25）的物理来源。注意 Router 只给到 disk，**G4 命中根本不参与路由打分**——从 S3 拉回来的 100ms 已经和重算 prefill 一个量级了。

配置入口在 `environment_names.rs` 的 `kvbm` 模块（[00 篇 §3.1](00-总览与架构.md#31-配置全景在哪一个文件里)）：`DYN_KVBM_CPU_CACHE_GB`、`DYN_KVBM_DISK_CACHE_GB`、`DYN_KVBM_OBJECT_*`。

## 2. Leader / Worker：想和做的分离

```
                    +-----------------+
                    | InstanceLeader  |   ← 只认 sequence hash 和 block 身份
                    |  (find_matches) |     从不碰裸内存
                    +--------+--------+
                             |
               +-------------+-------------+
      +--------v--------+        +--------v--------+
      | CoordinatedWorker|       | CoordinatedWorker|   ← rank 0 / rank 1
      +--------+---------+       +--------+---------+
      +--------v--------+        +--------v--------+
      | PhysicalWorker   |       | PhysicalWorker   |   ← 只认 layout handle 和
      | (TransferManager)|       | (TransferManager)|     DMA 描述符，不做决策
      +-----------------+        +-----------------+
```

`PhysicalWorker` 持有（`docs/onboarding.md`）：
- **`TransferManager`**——`kvbm-physical` 的引擎，真正搬字节，走 NIXL（RDMA/UCX）、NVMe 或对象存储 API
- 三个 **layout handle**（`g1_handle` / `g2_handle` / `g3_handle`），即本进程各层的物理内存注册
- 一张 **remote handle 表**，从对端 worker 导入的物理句柄，用来做 RDMA pull

Worker 实现两个 trait，`WorkerTransfers` 的四个方法基本概括了所有可能的数据流向：

| 方法 | 干什么 |
|---|---|
| `execute_local_transfer(src, dst, block_ids, ...)` | 本进程内跨层，如 G2 → G1 |
| `execute_remote_onboard(remote_desc, dst, ...)` | **从远端 RDMA 拉进来** |
| `execute_remote_offload(src, remote_desc, ...)` | 推到远端 |
| `connect_remote(instance_id, metadata)` | 导入对端 NIXL 元数据，建立 RDMA 能力 |

所有传输返回 `TransferCompleteNotification`——一个 await 就知道搬完了的异步句柄。**传输是非阻塞的，GPU 的 forward pass 不用停下来等**。

### 2.1 Onboarding 的四个阶段

请求需要的 KV 不在本地 G1 时，leader 开一个 session 走四步（`docs/architecture.md`）：

```
search   →   hold   →   prepare (G3→G2)   →   pull (远端 G2 →本地 G2, RDMA)
```

**【逻辑】** `hold` 这一步是关键：**先把找到的块钉住，再去搬**。不钉住的话，搬运期间源端可能把它淘汰掉，搬到一半发现源没了。**分布式搬运里，「我看到了」和「我拿到了」之间总有一个必须加锁的窗口**，`hold` 就是这个锁。

## 3. Offload：降级流水线

`kvbm-engine/src/offload/`，四级流水线（`docs/offload.md`）：

```
PolicyEvaluator → PreconditionAwaiter → Batcher → TransferExecutor
                          ▲                  ▲
                   CancellableQueue   CancellableQueue
                          └──── CancelSweeper ────┘
```

| 阶段 | 干什么 |
|---|---|
| **PolicyEvaluator** | 按策略过滤：哪些块值得搬 |
| **PreconditionAwaiter** | **等 forward pass 结束**再动手 |
| **Batcher** | 按总块数攒批 |
| **TransferExecutor** | 升级块引用，执行传输 |

第二级 `PreconditionAwaiter` 是这条流水线里最容易被忽略、但最要紧的一环：**KV 块正在被 attention kernel 读的时候不能搬**。所以每个 `OffloadContainer` 带一个 `precondition: Option<EventHandle>`（CUDA event），等它 signal 了才继续。

整条流水线**全程可取消**：每个 container 带自己的 `CancellationToken`，攒进同一批也能单独撤。`CancelSweeper` 负责清理队列里已经取消的。这是必要的——攒批意味着排队，排队期间那个块可能又被命中了，这时候就不该再降级它。

### 3.1 Offload 策略

```18:23:lib/kvbm-engine/src/offload/policy.rs
//! - `PresenceFilter<Src, Dst>`: Skip blocks already present in destination tier
//! - `PresenceAndLFUFilter<Src, Dst>`: Presence check + LFU count threshold
//! - `PassAllPolicy`: No filtering (pass all blocks)
//! - `AllOfPolicy`: Composite AND policy
//! - `AnyOfPolicy`: Composite OR policy
```

淘汰用的是 **LFU（按访问频次）**，不是 LRU。对 KV 前缀缓存这是更合理的选择：**一个被反复命中的系统提示前缀，即使最近一次访问稍早，也比一个刚用过一次的长尾请求前缀更值得留**。

策略实现里有个值得学的 Rust 技巧（`policy.rs:9~16` 注释）：不用 `#[async_trait]`，而是返回 `Either<Ready<T>, BoxFuture<T>>`。`PresenceFilter` 这类纯本地同步判断走 `Either::Left(ready(...))`，**零堆分配**；只有真需要异步的策略才 `Box::pin`。offload 判定是每块都要跑一遍的热路径，这个优化有意义。

## 4. 块的身份

```6:7:lib/kvbm-common/src/lib.rs
pub type BlockId = usize;
pub type SequenceHash = dynamo_tokens::PositionalLineageHash;
```

**`PositionalLineageHash`**（`lib/tokens/src/lib.rs:549`）由 `sequence_hash + parent + position` 扩展而来（`:1127`）——**「血统哈希」**：一个块的身份不只取决于自己的内容，还取决于它前面是什么、它在第几位。这正是前缀缓存需要的语义：同样的 16 个 token，接在不同前缀后面就是不同的块。

和 [02 篇](02-核心代码分析-KV感知路由.md#22-两种-hash) 的 Router 索引是**同一套语义、不同实现阶段**：`lib/tokens/src/lib.rs:163` 的注释明确指出应用层的 request salt 要用 `dynamo_kv_hashing::Request::salt_hash`。KVBM 消费的是**已经算好的块级 hash**，不重复实现。Router 那边的 radix 树目前仍用 `LocalBlockHash` 作边键，`lib/kv-hashing/` 是把两边统一起来的方向。

## 5. 引擎侧怎么接进来

**vLLM（主要路径）**：`components/src/dynamo/vllm/args.py:566`——`--kv-transfer-config` 里写 `kvbm` 时映射到

```python
"kv_connector_module_path": "kvbm.vllm_integration.connector"
```

Rust 实现在 `lib/bindings/kvbm/src/block_manager/vllm/connector/worker.rs`：注册 vLLM 的 KV tensor、交换 NIXL 元数据。多 connector 场景用 Dynamo 的 `PdConnector` 包一层（`args.py:578`）。

**SGLang**：`components/src/dynamo/sglang/` 下**搜不到 kvbm**。SGLang 侧目前只做 KV 事件发布（供 02 篇的 Router 用），没有 vLLM 那种 connector 接入。这与官方 README 的特性矩阵一致——KVBM 那一行 SGLang 是 🚧。

**TRT-LLM**：README 标 ✅，走的是 `dynamo.nixl_connect`（05 篇讲）。

> 所以现阶段的实际情况是：**KVBM 的完整分层能力主要在 vLLM 上跑通**。选型时这一点比特性矩阵上的对勾更重要。

## 6. 两个容易认错的组件

| 组件 | 是什么 | **不是**什么 |
|---|---|---|
| `lib/llm/src/kv_dc_relay/` | **DC 级 KV 索引 relay**。每个物理池一个串行化 actor，把 worker 本地的 KV 状态发布成数据中心级 catalog（Cuckoo filter + delta stream），供 Router 做跨池 placement（`host.rs:4~28`） | 不是 KV 数据面。它搬的是**索引**，不是 block |
| `components/src/dynamo/kv_state_agent/` | 集中托管多 slot 的 KV 状态 ingress，做 attachment 与 recovery（配合 `lib/llm/src/kv_router/publisher/state_agent_host.rs`） | 不是存储引擎，是**给 Router 用的**（见 02 篇 §3.1） |

另外 `lib/gpu_memory_service/` 是**模型权重**的 GPU 内存服务（含 NIXL snapshot），和 KV cache 分层是两个子系统，别混。

## 7. 「KV 归实例所有」是个有代价的假设

KVBM 的整个设计压在一个前提上：**KV 留在产生它的实例里，需要时点对点搬**，没有集群级的存储中心——`kv_dc_relay` 发布的是索引，不是存储权威。

好处是路径最短、没有中心瓶颈（[00 篇 §5](00-总览与架构.md#5-一条请求的九步官方口径) 里那句 "no shared storage bottlenecks"）。代价是**得先知道谁有**：这份「谁手上有哪些块」的知识散落在 Router 的 radix 索引和 `kv_dc_relay` 的 catalog 里，它们的准确性直接决定跨实例复用率——索引漏了一条，那份 KV 就等于不存在。

**这也解释了 Dynamo 的 Router 为什么那么重。** KV 位置的权威索引这件事没有别的组件做，只能由 Router 兼任。02 篇那一大坨代码不是路由算法复杂，是**索引维护复杂**。

## 8. 必记要点

1. **七个 crate 按「逻辑 vs 物理」切**：leader 只认 hash 从不碰内存，worker 只搬字节从不做决策。
2. **`lib/llm/src/block_manager/` 仍在，是拆分过渡态**，以 `kvbm-*` 为准。
3. **G1~G3 归 KVBM，G4 是外部服务**。G2（pinned DRAM）是枢纽，所有 RDMA 必经，所以没有 G1 直达 G3 的路径。
4. **Router 的三个折价系数（1.0/0.75/0.25）对应 G1/G2/G3 的延迟量级**；G4 不参与路由打分。
5. **Onboarding 四阶段 search → hold → prepare → pull**，`hold` 防的是「搬到一半源端淘汰了」。
6. **Offload 流水线的 `PreconditionAwaiter` 等 CUDA event**——正在被 attention 读的块不能搬。
7. **淘汰用 LFU 不是 LRU**，因为高频前缀比新近前缀更值钱。
8. **块身份是 `PositionalLineageHash`**（内容 + 父 + 位置），与 Router 同语义。
9. **KVBM 的完整能力目前主要在 vLLM 上**，SGLang 只有事件发布。
10. **KV 归实例所有、点对点搬**，没有集群级存储中心——代价是 Router 必须兼任 KV 位置的权威索引。

---

> **上一篇** [03 · SLA Planner](03-核心代码分析-SLA-Planner.md) ｜ **下一篇** [05 · P/D 分离与 NIXL](05-核心代码分析-PD分离与NIXL.md)
> **横向对比**见 [07 · 与 llm-d / Mooncake 的横向对比](07-横向对比-与llm-d和Mooncake.md)
