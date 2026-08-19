# KV Cache 基础设施（KVCache Infrastructure）

大模型推理的 **KV Cache 存储与传输**基础设施学习资料。聚焦「KV Cache 怎么跨实例共享、跨节点搬运、跨介质分层」这一核心问题。

## 目录

| 项目 | 目录 | 源码基线 | 定位 | 说明 |
|------|------|---------|------|------|
| Mooncake | [`mooncake/`](mooncake/) | `v0.3.12.post1` | **KVCache-centric 分离式架构** | Kimi 生产实践（FAST'25）：Transfer Engine 多协议 RDMA 传输、集群级 KV 缓存池、分层存储、租约与高可用 |

> 文档**钉在具体 release tag** 而非 main，所有类名、字段名、代码片段都能在对应 tag 上逐字找到。`git checkout v0.3.12.post1` 后即可边读边对照。

## 为什么单独开一个分类

本仓库的 [`inference-engine/`](../inference-engine/) 两套笔记（vLLM、SGLang）都在同一个地方「收住了」：

- vLLM 05 篇、SGLang 05 篇讲 PD 分离，都停在「KV 通过 RDMA 传给 Decode 实例」—— **这个「传」由谁实现？**
- SGLang 06 篇的 RadixCache 前缀缓存只活在**单实例 GPU 显存**里 —— 实例重启即失效，实例 A 算过的前缀实例 B 用不上。

**KV Cache 基础设施就是补这两个缺口的**：它不做推理，只做「搬」与「存」。与推理引擎是**叠加关系**，不是替代关系。

一句话区分：

> **vLLM / SGLang 管「算」，Mooncake 管「存」与「搬」。**
>
> vLLM 的 PagedAttention 与 SGLang 的 RadixAttention 解决「**单卡显存怎么省**」；
> Mooncake 解决「**整个集群的 KV 怎么共享**」。

## Mooncake 学习路线

推荐按编号顺序阅读，全套沿用**与 vLLM / SGLang 笔记完全一致的统一示例**（请求 A、B 两个共享前缀的请求），便于三套笔记横向串联：

| 篇 | 主题 | 核心问题 |
|----|------|---------|
| [00](mooncake/00-总览与架构.md) | 总览与架构 | KVCache-centric 是什么、补什么缺口、统一示例、**Mooncake ↔ vLLM ↔ SGLang 对照表** |
| [01](mooncake/01-TransferEngine传输引擎.md) | Transfer Engine 传输引擎 ★ | 数据怎么搬：Slice 切分、多网卡聚合、Topology 亲和选路、`submitTransfer` → `ibv_post_send` 全链路 |
| [02](mooncake/02-MooncakeStore缓存池.md) | Mooncake Store 缓存池 ★ | 存哪里、谁记得住：对象/副本模型、**PutStart/PutEnd 两阶段写入**、Master 元数据服务 |
| [03](mooncake/03-分层存储与持久化.md) | 分层存储与持久化 | 内存装不下之后：Bucket/offset 后端、io_uring / 3FS / SPDK、副本降级读取顺序 |
| [04](mooncake/04-高可用与元数据管理.md) | 高可用与元数据管理 | 生产必读：**租约机制**、soft/hard pin、双水位淘汰、etcd 选主、oplog + snapshot |
| [05](mooncake/05-与推理引擎集成.md) | 与推理引擎集成 | 怎么接进来：vLLM KV Connector、SGLang HiCache L3、prefetch / write-back 策略 |

> **01 与 02 是地基篇**：01 讲清「怎么搬」，02 讲清「存哪里」，理解这两篇后 03/04/05 都是自然延伸。
> **只想快速上手**：可以先读 00 建立地图，再直接跳 05 看配置方式。

### 按目的选择路线

```
只想知道怎么用            → 00 → 05
想搞懂性能来源            → 00 → 01（重点：Slice 切分与多网卡选路）→ 03
要上生产                  → 00 → 02 → 04（租约、淘汰水位、Master 故障转移）
对标 SGLang RadixCache    → 00 → 02 → 05（HiCache 那一节）
```

## 三个必须记住的设计要点

1. **控制面与数据面彻底分离**：Master 只保存「key → 副本位置」元数据，客户端拿到位置后**直接 RDMA 点对点读写目标节点内存**，数据一个字节都不经过 Master。这是它能扩展的根本原因，也是「切主期间数据面不受影响」的原因。

2. **存储池由客户端「捐献」**：没有专门的存储服务器，每个推理实例启动时 `mount_segment()` 把自己一部分 DRAM 贡献出来，聚合成全局池。谁都是消费者，谁也都是提供者 —— 代价是「segment 随时可能消失」（实例重启），所以它适合 KV Cache 这类「丢了能重算」的数据。

3. **所有「看起来奇怪」的机制都是「分布式」这个前提的产物**：
   - 用**租约（带 TTL）**而不是引用计数 —— 因为读者可能永久消失；
   - 用**两阶段写入**而不是一步 Put —— 因为要防止读到半成品、防止崩溃泄漏；
   - 用**水位淘汰**而不是分配失败时淘汰 —— 因为要避免抖动；
   - 用 **view_version**（≈ Raft term）—— 因为要防脑裂。

## 与其他分类的关系

```
inference-engine/    ← 推理引擎：请求怎么从字符串走到 GPU（算）
       │
       │  KV 需要跨实例传输 / 跨实例共享
       ▼
kvcache/             ← 本分类：KV 怎么搬、怎么存（存 + 搬）
       │
       │  这些实例与存储节点跑在哪
       ▼
scheduling/          ← 调度编排：一堆 GPU 怎么被公平高效分配（调）
```

## 阅读前提

- **建议先读** [`inference-engine/`](../inference-engine/) 的 vLLM 或 SGLang 系列（至少各自的 00 篇 + PD 分离篇），否则会缺少「为什么需要这层基础设施」的问题意识。
- **C++ 不熟没关系**：全系列每篇都附「C++ 语法速查表」，代码注释分 `【逻辑】`（系统在做什么、为什么）与 `【C++】`（语法讲解）两类。Mooncake 是 C++ 项目，大量使用 `tl::expected`、`std::variant`、移动语义、RAII 等现代特性，速查表会逐个交代。
