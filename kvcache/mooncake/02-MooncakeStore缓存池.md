# Mooncake 源码学习系列 · 02 Mooncake Store 缓存池

> **代码版本**：`kvcache-ai/Mooncake` @ **tag `v0.3.12.post1`**。详见 [00 篇 · 分析的代码库版本](./00-总览与架构.md#九分析的代码库版本)。
>
> 本篇目标：讲清统一示例中 **t2 / t4 两步** —— P0 把 A 的 KV `Put` 进池子；B 到达后命中共享前缀直接 `Get` 回来。核心是**对象模型、两阶段写入协议、Master 元数据服务**。
>
> 行号会随版本漂移，如与你的仓库不符，请用类名/函数名检索定位。

---

## 一、从「一块内存」到「一个对象」

01 篇的 Transfer Engine 提供的能力是：**把本地地址 X 的 N 字节搬到远端 segment S 的偏移 O**。但上层想要的是：

```python
store.put("kvcache:hash(SYSTEM)", kv_bytes)   # 我不管存哪，你帮我找地方
store.get("kvcache:hash(SYSTEM)")             # 我不管在哪，你帮我找回来
```

中间这层「找地方 / 找回来」就是 Mooncake Store。它要解决四个问题：

| 问题 | 机制 | 章节 |
|------|------|------|
| 池子里的空间从哪来？ | 客户端 `mount_segment()` 捐献 DRAM | §3 |
| `key` 对应的数据放哪了？ | Master 维护 `key → Replica[]` | §4 |
| 写的过程中别人读到半成品怎么办？ | **PutStart / PutEnd 两阶段协议** | §5 |
| 空间怎么分配、副本怎么放？ | Allocator + AllocationStrategy | §6 |

**一句话架构**：Master 是「图书馆的目录卡片柜」，只告诉你书在几号书架第几层；真正取书是你自己走过去拿（Transfer Engine 点对点 RDMA），**目录柜不搬书**。

---

## 二、核心数据结构

### 2.1 对象、副本、缓冲区的三层关系

```
Object（一个 key）
  └── ReplicaList = unordered_map<uint32_t, Replica>   ← 多个副本
        └── Replica
              ├── MemoryReplicaData   { unique_ptr<AllocatedBuffer> }   ← 内存副本
              ├── DiskReplicaData     { file_path, object_size }        ← 共享盘副本
              ├── LocalDiskReplicaData{ client_id, transport_endpoint } ← 本地盘副本
              └── NoFReplicaData      { unique_ptr<AllocatedBuffer> }   ← NVMe-oF SSD 副本
                    └── AllocatedBuffer → 落在某个 segment 的某个偏移
```

类型别名（`mooncake-store/include/types.h:182-190`）：

```cpp
using ObjectKey = std::string;
using Version = uint64_t;
using SegmentId = int64_t;
using BufHandleList = std::vector<std::shared_ptr<AllocatedBuffer>>;
using ReplicaList = std::unordered_map<uint32_t, Replica>;   // 【逻辑】★ 副本表
using BufferResources =
    std::map<SegmentId, std::vector<std::shared_ptr<BufferAllocatorBase>>>;
```

### 2.2 ReplicaStatus —— 副本状态机

`include/replica.h:51-58`：

```cpp
enum class ReplicaStatus {
    UNDEFINED = 0,  // Uninitialized                        未初始化
    INITIALIZED,    // Space allocated, waiting for write    空间已分配，等待写入
    PROCESSING,     // Write in progress                     写入进行中
    COMPLETE,       // Write complete, replica is available  写完，可被读取
    REMOVED,        // Replica has been removed              已删除
    FAILED,         // Failed state (can be used for reassignment) 失败（可重分配）
};
```

**这个状态机是两阶段写入的核心**（详见 §5）：

```
             PutStart()                  数据传输完成 + PutEnd()
UNDEFINED ──────────────▶ INITIALIZED ──────────────────────────▶ COMPLETE
                              │                                      │
                              │ PutRevoke()（写失败/放弃）             │ Remove()
                              ▼                                      ▼
                           FAILED                                REMOVED
```

**关键约束：只有 `COMPLETE` 状态的副本才会被 `GetReplicaList` 返回给读者。** 这就杜绝了「读到写了一半的数据」。

### 2.3 ReplicaType —— 四种介质

`include/replica.h:33-46`：

```cpp
    static const std::unordered_map<ReplicaType, std::string_view>
        replica_type_strings{{ReplicaType::MEMORY, "MEMORY"},
                             {ReplicaType::DISK, "DISK"},
                             {ReplicaType::LOCAL_DISK, "LOCAL_DISK"},
                             {ReplicaType::NOF_SSD, "NOF_SSD"},
                             {ReplicaType::ALL, "ALL"}};
```

| 类型 | 数据在哪 | 读取路径 |
|------|---------|---------|
| `MEMORY` | 某节点捐献的 DRAM segment | Transfer Engine RDMA READ（最快） |
| `DISK` | 共享文件系统（3FS / NFS） | 走 StorageBackend（03 篇） |
| `LOCAL_DISK` | 某客户端的本地盘 | 需经该客户端 `transport_endpoint` 中转 |
| `NOF_SSD` | NVMe-oF 远端 SSD | RDMA 直达 SSD |
| `ALL` | 非真实类型，用于批量操作的「所有类型」参数 | — |

数据载体（`replica.h:167-184`）：

```cpp
struct MemoryReplicaData    { std::unique_ptr<AllocatedBuffer> buffer; };
struct NoFReplicaData       { std::unique_ptr<AllocatedBuffer> buffer; };
struct DiskReplicaData      { std::string file_path; uint64_t object_size = 0; };
struct LocalDiskReplicaData { UUID client_id; uint64_t object_size = 0;
                              std::string transport_endpoint; };
```

### 2.4 Replica —— 用 variant 实现「多态而不虚函数」

`include/replica.h:209-278`（节选）：

```cpp
class Replica {
   public:
    // memory replica constructor
    Replica(std::unique_ptr<AllocatedBuffer> buffer, ReplicaStatus status)
        : id_(next_id_.fetch_add(1)),                 // 【逻辑】全局自增 ID
          data_(MemoryReplicaData{std::move(buffer)}),
          status_(status),
          refcnt_(0) {}

    // disk replica constructor
    Replica(std::string file_path, uint64_t object_size, ReplicaStatus status)
        : id_(next_id_.fetch_add(1)),
          data_(DiskReplicaData{std::move(file_path), object_size}),
          status_(status), refcnt_(0) {
        // Automatic update allocated_file_size via RAII
        MasterMetricManager::instance().inc_allocated_file_size(object_size);
    }

    ~Replica() {
        if (status_ == ReplicaStatus::UNDEFINED) return;   // 【逻辑】已被移走，不重复减
        if (is_disk_replica()) {
            MasterMetricManager::instance().dec_allocated_file_size(
                std::get<DiskReplicaData>(data_).object_size);
        }
    }

    // Copy-construction is not allowed.
    Replica(const Replica&) = delete;                 // 【逻辑】★ 禁止拷贝
    Replica& operator=(const Replica&) = delete;

    // Move-construction is allowed.
    Replica(Replica&& src) noexcept
        : id_(src.id_), data_(std::move(src.data_)),
          status_(src.status_), refcnt_(src.refcnt_.exchange(0)) {
        // Mark the source as moved-from so its destructor doesn't
        // double-decrement metrics.
        src.status_ = ReplicaStatus::UNDEFINED;       // 【逻辑】★ 关键：标记已搬走
    }
```

**【逻辑】三个设计细节值得学**：

1. **`data_` 是 `std::variant`**：一个 Replica 要么内存要么磁盘，用 variant 而非继承+虚函数，省掉指针跳转和 vtable，且 `std::get<T>` 类型安全。
2. **禁止拷贝、只允许移动**：它持有 `unique_ptr<AllocatedBuffer>`（独占内存所有权），拷贝没有语义。
3. **移动后把 `src.status_` 设为 `UNDEFINED`**：配合析构里那句 `if (status_ == UNDEFINED) return;`。否则「移动后的空壳」析构时会把指标再减一次 —— 这是 RAII + 移动语义的经典陷阱。

> **【C++】`std::variant<A, B, C>`** 是类型安全的联合体（C++17）。对比 01 篇 `Slice` 里的裸 `union`：variant 记住「当前是哪个类型」，取错会抛异常；裸 union 全靠程序员保证。这里用 variant 是因为 Replica 数量远少于 Slice，安全比极致性能重要。

### 2.5 ReplicateConfig —— 写入策略声明

`include/replica.h:81-98`：

```cpp
struct ReplicateConfig {
    size_t replica_num{1};              // 【逻辑】内存副本数
    size_t nof_replica_num{0};          // 【逻辑】NVMe-oF SSD 副本数
    bool with_soft_pin{false};          // 【逻辑】软钉：优先保留（04 篇）
    bool with_hard_pin{false};          // Hard pin: object cannot be evicted
    std::vector<std::string> preferred_segments{};   // 优先分配的 segment
    std::string preferred_segment{};                 // Deprecated
    std::vector<std::string> preferred_nof_segments{};
    bool prefer_alloc_in_same_node{false};  // 【逻辑】★ 优先分配在本机（省网络）
    ObjectDataType data_type{ObjectDataType::UNKNOWN};
    std::string host_id{};
    std::optional<std::vector<std::string>> group_ids{};
```

**`prefer_alloc_in_same_node` 对 KV Cache 特别重要**：P0 算出的 KV 若分配在 P0 自己捐献的 segment 上，「写」就是一次本地 memcpy，完全不过网络；后续别的实例来读才走 RDMA。典型的「写本地、读远程」优化。

`data_type` 给差异化策略留了口子（`types.h:141-153`）：

```cpp
enum class ObjectDataType : uint8_t {
    UNKNOWN = 0,  KVCACHE = 1,  TENSOR = 2,  WEIGHT = 3,
    SAMPLE = 4,   ACTIVATION = 5, GRADIENT = 6, OPTIMIZER_STATE = 7,
    METADATA = 8, GENERAL = 9,
    // 10-255 reserved for future types
};
```

**【逻辑】这个枚举暴露了 Mooncake 的野心**：不只服务推理的 `KVCACHE`，还想接训练（`GRADIENT` / `OPTIMIZER_STATE` / `SAMPLE`）—— 这解释了仓库里为什么有 `mooncake-rl/`（RL 训练）和 `mooncake-p2p-store/`（权重分发）。

### 2.6 写入模式的自动判定

`include/replica.h:150-165`：

```cpp
enum class ReplicaWriteMode {
    SINGLE_REPLICA, FLEXIBLE_DUAL_REPLICA, RELIABLE_MULTI_REPLICA,
};

inline ReplicaWriteMode DetermineReplicaWriteMode(
    const ReplicateConfig& config) {
    if (config.replica_num == 1 && config.nof_replica_num == 1) {
        return ReplicaWriteMode::FLEXIBLE_DUAL_REPLICA;
        // 【逻辑】一内存 + 一 SSD：灵活双写
    }
    if (config.replica_num > 1 || config.nof_replica_num > 1) {
        return ReplicaWriteMode::RELIABLE_MULTI_REPLICA;
        // 【逻辑】多副本：语义更严格
    }
    return ReplicaWriteMode::SINGLE_REPLICA;
}
```

**【逻辑】KV Cache 场景绝大多数用 `SINGLE_REPLICA`**：缓存丢了可以重算，不值得付多副本的写放大代价。多副本主要给「重算代价极高」的数据。

---

## 三、池子从哪来：Segment 的挂载

Mooncake **没有独立的存储服务器进程**，池子是所有客户端「捐献」出来的。

### 3.1 客户端启动即捐献

```python
store = MooncakeDistributedStore()
store.setup(
    local_hostname="node-P0",
    metadata_server="P2PHANDSHAKE",
    global_segment_size=16 * 1024**3,    # ★ 捐献 16 GB 给全局池
    local_buffer_size=1 * 1024**3,       # 本地暂存缓冲（读写中转）
    protocol="rdma",
    rdma_devices="mlx5_0,mlx5_1",
    master_server_addr="127.0.0.1:50051",
)
```

C++ 签名（`include/real_client.h:82-95`）：

```cpp
    int setup_real(
        const std::string &local_hostname, const std::string &metadata_server,
        size_t global_segment_size = 1024 * 1024 * 16,  // 【逻辑】默认才 16MB，生产必调大
        size_t local_buffer_size = 1024 * 1024 * 16,
        const std::string &protocol = "tcp",
        const std::string &rdma_devices = "",
        const std::string &master_server_addr = "127.0.0.1:50051",
        const std::shared_ptr<TransferEngine> &transfer_engine = nullptr,
        const std::string &ipc_socket_path = "",
        bool enable_ssd_offload = false,                 // 【逻辑】03 篇
        const std::string &ssd_offload_path = "",
        const std::string &tenant_id = "default", ...);
```

**两个 size 的区别**（最容易混）：

| | `global_segment_size` | `local_buffer_size` |
|--|----------------------|---------------------|
| 用途 | **捐献给全局池**，别人可往里写 | **自己读写的中转缓冲** |
| 谁能访问 | 集群所有客户端（RDMA 可达） | 只有自己 |
| 类比 | 你贡献给图书馆的书架 | 你自己的书桌 |

### 3.2 Master 侧的挂载 API

`include/master_service.h:142-197`：

```cpp
    auto MountSegment(const Segment& segment, const UUID& client_id)
        -> tl::expected<void, ErrorCode>;

    auto ReMountSegment(const std::vector<Segment>& segments,
                        const UUID& client_id) -> tl::expected<void, ErrorCode>;
    //  【逻辑】★ ReMount 用于「客户端存活但 Master 重启/切主」后重新登记

    auto UnmountSegment(const UUID& segment_id, const UUID& client_id)
        -> tl::expected<void, ErrorCode>;

    auto GracefulUnmountSegment(const UUID& segment_id, const UUID& client_id,
                                uint64_t grace_period_ms)
        -> tl::expected<void, ErrorCode>;
    //  【逻辑】优雅下线：给 grace_period 让在读的请求收尾
```

> **【C++】`auto f(args) -> tl::expected<void, ErrorCode>;`** 是尾置返回类型。`tl::expected<T, E>` 是 C++23 `std::expected` 的第三方实现（类似 Rust `Result<T, E>`）：要么持有 `T`，要么持有错误 `E`。用法：`if (!r) return r.error(); auto v = r.value();`。**Store 模块统一用它替代异常与错误码，这是读 Store 代码前必须先适应的一点。**

**`ReMountSegment` 揭示了一个重要设计**：Master 的元数据是**可重建的**。Master 崩溃重启后，客户端通过心跳发现 view 变了，自动 ReMount 自己的 segment —— 但**已存对象的元数据会丢**（除非启用 snapshot/oplog，见 04 篇）。对 KV Cache 可接受：丢了重算。

---

## 四、Master：只管目录，不碰数据

`MasterService`（`include/master_service.h` 2191 行 + `src/master_service.cpp` 8956 行）是元数据中枢。

### 4.1 API 全景

| 分类 | 方法 | 行号 | 语义 |
|------|------|------|------|
| **Segment** | `MountSegment` / `ReMountSegment` | `:142` `:168` | 登记/重新登记捐献内存 |
| | `UnmountSegment` / `GracefulUnmountSegment` | `:192` `:195` | 下线 |
| **查询** | `ExistKey(key, tenant_id)` | `:212` | 是否存在（★ 前缀命中判断用它） |
| | `BatchExistKey(keys, tenant_id)` | `:215` | 批量存在性 |
| | `GetReplicaList(key, tenant_id)` | `:346` | ★ 取副本位置 **并授予租约** |
| | `BatchGetReplicaList(keys, tenant_id)` | `:362` | 批量取 |
| **写入** | `PutStart(client_id, key, tenant_id, slice_length, config)` | `:382` | ★ 阶段一：分配空间 |
| | `PutEnd(client_id, key, tenant_id, replica_type)` | `:393` | ★ 阶段二：提交 |
| | `PutRevoke(client_id, key, tenant_id, replica_type)` | `:410` | 阶段二：放弃 |
| | `BatchPutEnd` / `BatchPutRevoke` | `:419` `:428` | 批量版 |
| **删除** | `Remove` / `BatchRemove` | `:566` `:600` | 删除 |
| **心跳** | `Ping(client_id)` | `:617` | ★ 客户端保活，返回 view 版本 |

**注意 `GetReplicaList` 不是只读操作**（`:346-347`）：

```cpp
    auto GetReplicaList(const std::string& key, const TenantId& tenant_id)
        -> tl::expected<GetReplicaListResponse, ErrorCode>;
```

内部会给对象**授予租约**（见 04 篇）。这是容易踩的点：「查一下位置」这个动作本身有副作用。需要纯只读查询要用 admin 接口（`:350` 附近注释提到 "Read-only single-key replica list query for admin use"）。

### 4.2 数据面完全不经过 Master

```
        ①  PutStart(key, len)
Client ─────────────────────────▶ Master        （小消息，几十字节）
        ◀───────────────────────
           返回 Replica::Descriptor[]
           = {segment_name, offset, size, rkey...}

        ②  Transfer Engine WRITE                （★ 大数据，GB 级）
Client ══════════════════════════▶ 目标节点内存   （RDMA，Master 完全不知情）

        ③  PutEnd(key)
Client ─────────────────────────▶ Master        （小消息）
```

**Master 的 QPS 压力只和「对象数量」有关，与「数据量」无关。** 这是它能撑住大规模集群的根本原因。

---

## 五、两阶段写入：为什么不能一步到位

本篇最核心的机制。

### 5.1 如果只有一步会怎样

设想朴素的 `Put(key, data)`：Master 分配空间 → 客户端写数据 → 完成。问题在于**「Master 分配完」到「客户端写完」之间有时间窗**：

- 另一客户端此时 `Get(key)`，Master 说「有啊，在 segment0 偏移 1024」→ 读到**未初始化的垃圾**；
- 客户端写到一半崩了 → 这块空间**永久泄漏**，Master 以为有个好对象。

### 5.2 两阶段协议

```
阶段一：PutStart
  Client                                Master
    │  PutStart(key, slice_length, cfg)  │
    ├───────────────────────────────────▶│
    │                                    ├─ 检查参数、配额
    │                                    ├─ AllocationStrategy 分配空间
    │                                    ├─ 创建 Replica，状态 = INITIALIZED
    │                                    │     ★ 此状态不会被 GetReplicaList 返回
    │◀───────────────────────────────────┤
    │  vector<Replica::Descriptor>       │

阶段间：数据传输（Transfer Engine，Master 不参与）
    │═══════════ RDMA WRITE ════════════▶ 目标 segment

阶段二：PutEnd（成功）           或    PutRevoke（失败）
    ├───────────────────────────────────▶│  ├──────────────────▶
    │                                    ├─ INITIALIZED → COMPLETE
    │                                    ├─ GrantLease(0, soft_ttl)
    │                                    ├─ PublishKvStored(...) 发 KV 事件
    │                                    │      └─ 释放空间，状态 → FAILED
```

### 5.3 PutStart 源码

`src/master_service.cpp:2640-2700`（节选）：

```cpp
auto MasterService::PutStart(const UUID& client_id, const std::string& key,
                             const TenantId& tenant_id,
                             const uint64_t slice_length,
                             const ReplicateConfig& config)
    -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> {
    auto normalized_tenant_result = ResolveTenantIdForWrite(tenant_id);
    if (!normalized_tenant_result) {
        return tl::make_unexpected(normalized_tenant_result.error());
    }
    const ObjectIdentity object_id{std::move(normalized_tenant_result.value()),
                                   key};
    if ((config.replica_num == 0 && config.nof_replica_num == 0) ||
        key.empty() || slice_length == 0) {
        // 【逻辑】参数校验：副本数 0、key 空、长度 0 都非法
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.prefer_alloc_in_same_node && config.nof_replica_num > 0) {
        // 【逻辑】本机优先 与 NoF SSD 副本 语义冲突，禁止组合
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    UpdateClientHostId(client_id, config.host_id);

    if ((memory_allocator_type_ == BufferAllocatorType::CACHELIB) &&
        (slice_length > kMaxSliceSize)) {
        // 【逻辑】★ cachelib 分配器有单次上限，超了要用 offset 分配器
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    [[maybe_unused]] auto object_operation_lock =
        AcquireObjectOperationLock(object_id.tenant_id, object_id.user_key);
    //  【逻辑】★ 按 key 加锁（分片锁），保证同一 key 的并发 Put 串行化
    const uint64_t requested_quota_charge =
        RequestedMemoryQuotaCharge(slice_length, config);
    // ... 后续：配额检查 → AllocationStrategy::Allocate → 创建 INITIALIZED 副本
```

> **【C++】`[[maybe_unused]] auto lock = AcquireObjectOperationLock(...)`**
> 这是 **RAII 锁守卫**：构造加锁，析构（函数返回）自动解锁。`[[maybe_unused]]` 告诉编译器「我知道后面没用它，别警告」—— 它的价值全在构造/析构的副作用里。等价于 Python 的 `with lock:`。

`kMaxSliceSize` 的来源（`types.h:425-427`）：

```cpp
const static uint64_t kMinSliceSize = facebook::cachelib::Slab::kMinAllocSize;
const static uint64_t kMaxSliceSize =
    facebook::cachelib::Slab::kSize - 16;  // should be lower than limit
```

**【逻辑】Mooncake 直接复用了 Facebook CacheLib 的 Slab 分配器**。Slab 特点是「按固定档位分配、几乎无外部碎片」，但单次分配不能超过一个 Slab。所以大对象要走另一个分配器（§6）。

### 5.4 PutEnd 源码：租约的起点

`src/master_service.cpp:2902-2908`：

```cpp
    // 1. Set lease timeout to now, indicating that the object has no lease
    // at beginning. 2. If this object has soft pin enabled, set it to be soft
    // pinned.
    // 【中译】① 把租约超时设为「现在」，表示对象初始没有租约。
    //         ② 如果启用了软钉，则标记为软钉状态。
    metadata.GrantLease(0, default_kv_soft_pin_ttl_);
    PublishKvStored(key, replica_type, metadata, metadata.tenant_id);
    return {};
```

**【逻辑】`GrantLease(0, soft_ttl)` 里那个 `0` 很讲究**：刚写完的对象**故意不给硬租约**（ttl=0，立刻过期）。为什么？

写入者写完就走了，没人在读。若给 10 秒租约，这 10 秒它就不能被淘汰 —— 万一池子正紧张，会挡住真正需要空间的写入。**租约只在「有人正在读」时才有意义**，所以留到 `GetReplicaList` 时再授予（04 篇详述）。

`PublishKvStored` 向外发布「有新 KV 落盘」事件（`include/kv_event/`），供外部路由器做前缀感知调度 —— 比如让下次带相同前缀的请求优先路由到这个节点。

### 5.5 客户端侧完整 Put 流程（统一示例 t2）

```
Client::put_from(key, buffer, size, config)          real_client.h:188
  │
  ├─① master_client_->PutStart(key, size, config)     ← RPC 到 Master
  │     └─ 返回 vector<Replica::Descriptor>
  │        每个含：segment_name / offset / size / rkey
  │
  ├─② 对每个副本构造 TransferRequest 并提交：            ← 01 篇内容
  │     TransferRequest{WRITE, buffer, seg_handle, offset, size}
  │     TransferEngine::submitTransfer() → 轮询到 COMPLETED
  │
  └─③ 成功 → PutEnd(key, replica_type)
      失败 → PutRevoke(key, replica_type)
```

**【逻辑】② 的 `buffer` 必须是注册过的内存**。`real_client.h:132-134` 的注释：

```cpp
     * @note The buffer address must resolve to Store-managed registered memory
     * for zero-copy operations
     * 【中译】buffer 地址必须解析到 Store 管理的已注册内存，才能零拷贝。
```

传了未注册内存要么报错，要么退化成「先 memcpy 到 local_buffer 再传」（多一次拷贝）。

---

## 六、空间怎么分配

### 6.1 两套分配器

| 分配器 | 位置 | 算法 | 适用 |
|-------|------|------|------|
| `CachelibBufferAllocator` | `include/cachelib_memory_allocator/` | Facebook CacheLib **Slab**（固定档位） | 中小对象，碎片极低 |
| `OffsetBufferAllocator` | `include/offset_allocator/`（`src/offset_allocator.cpp` 723 行） | 偏移量分配 | 大对象，无 `kMaxSliceSize` 限制 |

**【逻辑】为什么需要两套？** KV Cache 对象大小分布很宽：一个 token block 的 KV 可能几十 KB，长上下文的完整 KV 可能几百 MB。Slab 对前者极好（档位对齐、无碎片），对后者无能（超 Slab 上限）。

### 6.2 副本放置策略

`include/allocation_strategy.h:140-206`：

```cpp
class AllocationStrategy {
   public:
    virtual ~AllocationStrategy() = default;

    /**
     * The allocation follows best-effort semantics: if the full requested
     * replica count cannot be satisfied, the method will allocate as many
     * replicas as possible across different segments. For each slice, replicas
     * are guaranteed to be placed on different segments to ensure redundancy.
     * 【中译】尽力而为语义：无法满足完整副本数时，尽可能多地在【不同 segment】上分配。
     *    对每个 slice，保证副本落在不同 segment 上以确保冗余。
     */
    virtual tl::expected<std::vector<Replica>, ErrorCode> Allocate(
        const AllocatorManager& allocator_manager, const size_t slice_length,
        const size_t replica_num = 1,
        const std::vector<std::string>& preferred_segments = {},
        const std::set<std::string>& excluded_segments = {},
        const ReplicaType replica_type = ReplicaType::MEMORY) = 0;

    virtual tl::expected<Replica, ErrorCode> AllocateFrom(
        const AllocatorManager& allocator_manager, const size_t slice_length,
        const std::string& segment_name) = 0;
};
```

默认实现 `RandomAllocationStrategy`（`:222`）的行为说明（`:208-221`）：

```cpp
/**
 * @brief Random batch allocation strategy with local preference and
 *        replication guarantees support using best-effort semantics.
 *
 * This strategy ensures that for each slice, its replicas are placed in
 * different segments. Different slices may use the same segments.
 */
```

**三条规则**：
1. **随机**打散到各 segment（天然负载均衡，无需中心化统计）；
2. **同一 slice 的多副本必须在不同 segment**（否则一个节点挂了副本全丢，冗余失效）；
3. **尽力而为**：只有 2 个 segment 却要 3 副本，就给 2 个，不报错。

`preferred_segments` / `excluded_segments` 分别对应「本机优先」和「避开刚失败的 segment」。

---

## 七、读取路径：统一示例的 t4

请求 B 到达，判断共享前缀是否已缓存：

```python
# ① 快速判断（只查存在性，最轻量）
if store.is_exist("kvcache:hash(SYSTEM)"):
    # ② 真正读回来
    n = store.get_into("kvcache:hash(SYSTEM)", buffer, size)
```

内部流程：

```
Client::get_into(key, buffer, size)                  real_client.h:135
  │
  ├─① master_client_->GetReplicaList(key)             ← RPC
  │     ├─ Master 只返回 status == COMPLETE 的副本      ★ 不会读到半成品
  │     ├─ ★ 授予租约 GrantLease(kv_lease_ttl, ...)   ← 保护读取期不被淘汰
  │     └─ 返回 Replica::Descriptor[]
  │
  ├─② 选副本（优先 MEMORY > NOF_SSD > DISK，且优先本机） ← 03 篇降级顺序
  │
  └─③ TransferRequest{READ, buffer, seg, offset, size}
        TransferEngine::submitTransfer() → 数据直接落到 buffer
```

副本选择逻辑在 `include/replica_selection.h`；`batch_get_into_multi_buffers(..., bool prefer_same_node)`（`real_client.h:170-174`）把「本机优先」暴露给上层 —— 本机命中就是一次 memcpy。

### 7.1 批量接口：为什么必须有

```cpp
    std::vector<int64_t> batch_get_into(const std::vector<std::string> &keys,
                                        const std::vector<void *> &buffers,
                                        const std::vector<size_t> &sizes);

    std::vector<int> batch_put_from(
        const std::vector<std::string> &keys,
        const std::vector<void *> &buffers, const std::vector<size_t> &sizes,
        const ReplicateConfig &config = ReplicateConfig{});

    std::vector<tl::expected<QueryResult, ErrorCode>> batch_query(
        const std::vector<std::string> &keys) override;
```

**【逻辑】KV Cache 的访问模式天然是批量的**：一个请求的 KV 按 token block 切成几十上百个 key，逐个 `Get` 意味着几十上百次 RPC 往返（每次 ~100 μs），总延迟无法接受。批量接口把延迟从「N × RTT」降到「1 × RTT + max(传输时间)」。

`batch_query` 更进一步：**只取元数据不取数据**，配合 `QueryResultCache` 复用 —— 这正是 SGLang HiCache 的 prefetch 线程判断「L3 命中多少 token」时用的接口（05 篇）。

---

## 八、Python 绑定

`mooncake-integration/store/store_py.cpp` 用 **pybind11** 暴露。主类 `MooncakeDistributedStore`（`:2100`）：

```cpp
    py::class_<MooncakeStorePyWrapper>(m, "MooncakeDistributedStore")
        .def(py::init<>())
        .def("setup", ...,
             py::arg("local_hostname"), py::arg("metadata_server"),
             py::arg("global_segment_size") = ..., py::arg("local_buffer_size") = ...,
             py::arg("protocol") = "tcp", py::arg("rdma_devices") = "",
             py::arg("master_server_addr") = ..., py::arg("engine") = nullptr,
             py::arg("enable_ssd_offload") = false, py::arg("ssd_offload_path") = "",
             py::arg("tenant_id") = "default", ...)
        .def("init_all", ...)
        .def("mount_segment", ..., py::arg("path"), py::arg("size"),
             py::arg("offset"), py::arg("protocol") = "tcp", py::arg("location"))
        .def("unmount_segment", ..., py::arg("segment_ids"),
             py::arg("grace_period_seconds"))
        .def("allocate_and_mount_segment", ..., py::arg("size"), ...)
        // ... put / get / get_into / put_from / batch_* / is_exist / remove
```

同时暴露的辅助类型：

| Python 类 | C++ 对应 | 用途 |
|-----------|---------|------|
| `ReplicateConfig` | `ReplicateConfig` (`:1830`) | 写入策略 |
| `ReplicaDescriptor` | `Replica::Descriptor` (`:1868`) | 副本位置，带 `is_memory_replica()` / `is_disk_replica()` |
| `MemoryDescriptor` / `DiskDescriptor` | 同名 (`:1860` `:1864`) | 各介质描述 |
| `BufferHandle` | `BufferHandle` (`:1943`) | 零拷贝缓冲句柄，有 `ptr()` / `size()` |
| `MooncakeHostMemAllocator` | (`:1982`) | 直接分配已注册的 host 内存 |

另有 `store/async_store.py` 提供 asyncio 封装；`transfer_engine/transfer_engine_py.cpp` 单独暴露 Transfer Engine（不用 Store 也能直接用传输能力 —— vLLM 的 PD 分离 connector 走的就是这条路，见 05 篇）。

---

## 九、与 vLLM / SGLang 的对照

| 机制 | vLLM PagedAttention | SGLang RadixCache | Mooncake Store |
|------|--------------------|-------------------|----------------|
| 索引结构 | Block 哈希表 | 基数树 | **哈希表**（`key → ReplicaList`）在 Master 内存 |
| key 是什么 | block 内容哈希 | token 序列（树路径） | **上层自定**，通常是前缀哈希字符串 |
| 「正在使用」的保护 | `ref_cnt++` | `lock_ref++` | **租约（带 TTL）** |
| 写入原子性 | 无需（进程内） | 无需（进程内） | **PutStart/PutEnd 两阶段** |
| 空间分配 | 预分配 block 池 | 预分配 token 池 | **Slab / offset 分配器 + 随机放置** |
| 多副本 | 无概念 | 无概念 | `replica_num`，不同 segment |
| 作用域 | 单实例显存 | 单实例显存 | **整个集群** |
| 粒度 | 16 token 定长 | 变长节点 | 上层决定（通常按 token block） |

**为什么租约必须带 TTL，而 `ref_cnt` 不用？** 因为 Mooncake 是分布式的：读者可能崩溃、网络可能分区。如果用纯引用计数，一个崩掉的读者会让对象**永久无法淘汰**。TTL 是分布式系统对「持有者可能消失」的标准应对。这是单机数据结构与分布式数据结构最本质的差别之一。

---

## 十、C++ 语法速查表

| 语法 | 含义 | 本篇出处 |
|------|------|---------|
| `tl::expected<T, E>` | 要么值要么错误，类似 Rust `Result` | 几乎所有 Store API |
| `tl::make_unexpected(err)` | 构造错误返回值 | `PutStart` 校验分支 |
| `auto f() -> Ret;` | 尾置返回类型 | `MasterService` 全部方法 |
| `std::variant<A,B,C>` + `std::get<A>(v)` | 类型安全联合体 | `Replica::data_` |
| `std::unique_ptr<T>` | 独占所有权智能指针 | `MemoryReplicaData::buffer` |
| `Replica(const Replica&) = delete;` | 显式禁用拷贝 | `Replica` |
| `Replica(Replica&&) noexcept` | 移动构造 | `Replica` |
| `std::move(x)` | 转为右值，触发移动 | 各构造函数 |
| `[[maybe_unused]] auto lock = ...` | RAII 守卫 + 抑制未用警告 | `PutStart` |
| `std::optional<T>` | 可能为空的值 | `group_ids`、`soft_pin_timeout` |
| `enum class X : uint8_t` | 限定作用域枚举 + 指定底层类型 | `ObjectDataType` |
| `inline` 函数在头文件 | 允许多处定义，避免链接冲突 | `DetermineReplicaWriteMode` |
| `std::string_view` | 零拷贝字符串视图 | 枚举名映射表 |
| `virtual ... = 0` + `override` | 纯虚 + 覆盖 | `AllocationStrategy` |
| `YLT_REFL(T, f1, f2)` | yalantinglibs 反射宏，自动生成序列化 | 各 `*Descriptor` |

---

## 十一、必记要点

1. **三层模型**：Object（key）→ Replica（副本，四种介质）→ AllocatedBuffer（落在某 segment 的偏移）。
2. **只有 `COMPLETE` 状态的副本会被返回给读者** —— 这是「读不到半成品」的保证。
3. **两阶段写入 PutStart/PutEnd 是核心协议**：中间的数据传输 Master 完全不参与。
4. **`PutEnd` 里 `GrantLease(0, soft_ttl)`**：刚写完故意不给硬租约，租约留到有人读时才授予。
5. **池子由客户端 `mount_segment` 捐献**，`global_segment_size`（给别人用）≠ `local_buffer_size`（自己用）。
6. **Master 只存元数据**，压力与数据量无关，只与对象数量有关。
7. **两套分配器**：CacheLib Slab（中小对象）/ offset（大对象，突破 `kMaxSliceSize`）。
8. **副本放置：随机 + 同 slice 副本必须跨 segment + 尽力而为**。
9. **批量 API 是必需品**，不是优化项 —— KV Cache 一次访问几十上百个 key。
10. **`Replica` 禁拷贝只可移动，且移动后置 `UNDEFINED`** 防止指标重复扣减。
11. **租约必须带 TTL**（对比 vLLM `ref_cnt`）：分布式环境下持有者可能永久消失。

---

> 上一篇：[01 · Transfer Engine 传输引擎](./01-TransferEngine传输引擎.md)
> 下一篇：[03 · 分层存储与持久化](./03-分层存储与持久化.md) —— 内存池满了之后：SSD offload、io_uring、3FS、SPDK，以及副本降级读取顺序。
