# Mooncake 源码学习系列 · 01 Transfer Engine 传输引擎

> **代码版本**：`kvcache-ai/Mooncake` @ **tag `v0.3.12.post1`**。详见 [00 篇 · 分析的代码库版本](./00-总览与架构.md#九分析的代码库版本)。
>
> 本篇目标：把统一示例中 **t2 / t3 那一步**（KV 从 P0 内存搬到目标 segment、再被 D0 读走）拆到底 —— 从 `submitTransfer()` 一路追到 `ibv_post_send()`，再追到完成事件被轮询回来。
>
> 行号会随版本漂移，如与你的仓库不符，请用类名/函数名检索定位。

---

## 一、Transfer Engine 要解决的问题

回到 00 篇的统一示例 t2：P0 算完请求 A 的 KV，要把它写进 Store 分配好的目标位置。这件事的**朴素做法**是 `socket.send()`，但在推理场景里完全不可接受：

| 朴素做法的问题 | Transfer Engine 的对策 |
|---------------|----------------------|
| TCP 走内核协议栈，一次拷贝到内核缓冲区，CPU 打满 | **RDMA 零拷贝**：网卡直接读写用户态内存，CPU 不参与搬运 |
| 单张网卡带宽打满（200 Gbps 也就 25 GB/s） | **多网卡聚合**：8 张 RoCE 卡并行，实测峰值 142.25 GB/s |
| 一台机器上 8 张卡、8 个 GPU，随便挑网卡会跨 NUMA / 跨 PCIe switch | **Topology 亲和选路**：按内存所在位置选「最近」的网卡 |
| 上层代码要为 RDMA / TCP / NVLink 各写一套 | **Transport 抽象**：上层只发 `TransferRequest`，底层自动路由 |
| 一次传 1 GB，网卡 WR 队列撑不住，且失败要整体重传 | **Slice 切分**：切成默认大小的片，独立重试 |

所以它的定位是：**一个面向大块数据、多介质、多网卡的异步搬运引擎**。

---

## 二、五个核心数据结构（先认人，再看流程）

这五个结构定义在 `mooncake-transfer-engine/include/transport/transport.h`，是理解全篇的地基。

### 2.1 TransferRequest —— 上层唯一需要填的东西

`include/transport/transport.h:60-71`：

```cpp
    struct TransferRequest {
        enum OpCode { READ, WRITE };        // 【逻辑】只有两种语义：从远端读、往远端写

        OpCode opcode;
        void *source;                      // 【逻辑】本地内存地址（必须已注册！）
        SegmentID target_id;               // 【逻辑】目标 segment 句柄，openSegment() 得到
        uint64_t target_offset;            // 【逻辑】目标 segment 内的偏移
        size_t length;                     // 【逻辑】搬多少字节
        int advise_retry_cnt = 0;          // 【逻辑】建议的起始重试次数（选网卡时会跳过失败过的）
        // Per-request transport pin, TENT only.
        int transport_hint = 0;            // 【逻辑】强制指定 transport（仅新 TENT 实现用）
    };
```

**【逻辑】注意这个设计有多克制**：没有「目标 IP」、没有「用哪张网卡」、没有「用什么协议」。只有「本地地址 → 远端 segment 的某个偏移」。所有路由决策都在引擎内部完成 —— 这正是它作为抽象层的价值。

> **【C++】`enum OpCode { READ, WRITE };`** 定义在 struct 内部的枚举，使用时写 `TransferRequest::READ`。这是 C++98 风格的非限定枚举（现代写法是 `enum class`），这里保留是为了 C ABI 兼容（见 `transfer_engine_c.h`）。

### 2.2 Slice —— 真正被投递给网卡的单元

一个 `TransferRequest` 会被切成多个 `Slice`。`include/transport/transport.h:112-185`（节选）：

```cpp
    // Slice must be allocated on heap, as it will delete self on markSuccess
    // or markFailed.
    // 【中译】Slice 必须堆分配，因为它会在 markSuccess/markFailed 时自我销毁。
    struct Slice {
        enum SliceStatus { PENDING, POSTED, SUCCESS, TIMEOUT, FAILED };

        void *source_addr;                 // 【逻辑】这一片的本地起始地址
        size_t length;                     // 【逻辑】这一片的长度（默认 = slice_size）
        TransferRequest::OpCode opcode;
        SegmentID target_id;
        std::string peer_nic_path;         // 【逻辑】★ 对端网卡路径，形如 "节点名@mlx5_1"
        std::string source_location;
        SliceStatus status;
        TransferTask *task;                // 【逻辑】反向指针，完成时用来累加计数

        union {                            // 【逻辑】★ 不同协议各用一套私有字段，省内存
            struct {
                uint64_t dest_addr;
                mr_key_t source_lkey;      // 【逻辑】本地内存注册后的 lkey
                mr_key_t dest_rkey;        // 【逻辑】远端内存的 rkey（授权凭证）
                int lkey_index;
                int rkey_index;
                std::atomic<int> *qp_depth;
                uint32_t retry_cnt;
                uint32_t max_retry_cnt;
                RdmaEndPoint *endpoint;    // Endpoint used for this transfer
            } rdma;
            struct { uint64_t dest_addr; } tcp;
            struct { void *dest_addr; void *cuda_stream; } local;
            struct { uint64_t offset; int cufile_desc; ... } nvmeof;
            // ... 还有 ub / cxl / hccl / ascend_direct / ubshmem 等
        };
```

> **【C++】`union`**：所有成员共用同一块内存，同一时刻只有一个有效。这里用它做「协议私有数据」——一个 Slice 要么走 rdma 要么走 tcp，不会同时。这比每个协议都留字段省一大截内存（Slice 在高吞吐下会有几十万个实例）。

**完成回调**（`transport.h:188-202`）是整个状态机的心脏：

```cpp
        void markSuccess() {
            status = Slice::SUCCESS;
            __atomic_fetch_add(&task->transferred_bytes, length,
                               __ATOMIC_RELAXED);      // 【逻辑】累加已传字节
            __atomic_fetch_add(&task->success_slice_count, 1, __ATOMIC_RELAXED);
            check_batch_completion(false);             // 【逻辑】看看整个 batch 是否完成
        }

        void markFailed() {
            status = Slice::FAILED;
            __atomic_fetch_add(&task->failed_slice_count, 1, __ATOMIC_RELAXED);
            check_batch_completion(true);
        }
```

> **【C++】`__atomic_fetch_add(..., __ATOMIC_RELAXED)`** 是 GCC 内建原子操作。`RELAXED` 表示只保证操作本身原子，不提供内存序保证 —— 这里够用，因为最终的「完成」判定靠的是**计数相等**（见 2.4），而不是靠这几个计数之间的顺序。用最弱的内存序是为了性能：这段代码在每个 slice 完成时都会跑。

### 2.3 ThreadLocalSliceCache —— 为什么不直接 new/delete

`transport.h:262-301`：

```cpp
    struct ThreadLocalSliceCache {
        Slice *allocate() {
            Slice *slice;
            if (head_ - tail_ == 0) {          // 【逻辑】缓存空了，只能真 new
                slice = new Slice();
                slice->from_cache = false;
            } else {                            // 【逻辑】命中缓存，复用旧对象
                slice = lazy_delete_slices_[tail_ % kLazyDeleteSliceCapacity];
                tail_++;
                slice->from_cache = true;
            }
            return slice;
        }

        void deallocate(Slice *slice) {
            if (head_ - tail_ == kLazyDeleteSliceCapacity) {  // 【逻辑】缓存满，真 delete
                delete slice;
                return;
            }
            lazy_delete_slices_[head_ % kLazyDeleteSliceCapacity] = slice;
            head_++;                                          // 【逻辑】否则留着复用
        }

        const static size_t kLazyDeleteSliceCapacity = 4096;
        std::vector<Slice *> lazy_delete_slices_;
        uint64_t head_, tail_;
    };
```

**【逻辑】这是典型的对象池 + 环形缓冲**。`head_`/`tail_` 只增不减，靠 `% capacity` 取模定位 —— 无需处理回绕，因为 `head_ - tail_` 的差值才是有效元素数。**thread_local** 意味着每个工作线程一个池子，完全无锁。

传 1 GB 数据按 64 KB 切片就是 16384 个 Slice，如果每次都 `new/delete`，光内存分配就能把性能吃掉一半。

### 2.4 TransferTask 与 BatchDesc —— 两级计数

`transport.h:303-363`（节选）：

```cpp
    struct TransferTask {
        volatile uint64_t slice_count = 0;          // 【逻辑】这个 task 切了多少片
        volatile uint64_t success_slice_count = 0;   // 【逻辑】成功几片
        volatile uint64_t failed_slice_count = 0;    // 【逻辑】失败几片
        volatile uint64_t transferred_bytes = 0;
        volatile bool is_finished = false;
        uint64_t total_bytes = 0;
        BatchID batch_id = 0;
        Transport *transport_ = nullptr;   // 由 MultiTransport::submitTransfer() 设置
        const TransferRequest *request = nullptr;
        std::vector<Slice *> slice_list;
        ~TransferTask() {
            for (auto &slice : slice_list)
                Transport::getSliceCache().deallocate(slice);   // 【逻辑】归还对象池
        }
    };

    struct BatchDesc {
        BatchID id;
        size_t batch_size;
        std::vector<TransferTask> task_list;   // 【逻辑】一个 batch 含多个 task
        void *context;
        int64_t start_timestamp;
        std::atomic<bool> has_failure{false};
        std::atomic<bool> is_finished{false};
        std::atomic<uint64_t> finished_transfer_bytes{0};
    };
```

三层包含关系，务必记牢：

```
BatchDesc（一批）
  └── TransferTask（一个 TransferRequest 对应一个）
        └── Slice（按 slice_size 切分，一个 RDMA WR 一片）
```

**完成判定就是比计数**：`success_slice_count + failed_slice_count == slice_count` 则 task 完成；所有 task 完成则 batch 完成。

> **【C++】`volatile`** 在这里的用法其实是有争议的（现代 C++ 应该用 `std::atomic`）。作者用 `volatile` + `__atomic_*` 内建函数的组合：`volatile` 阻止编译器把变量缓存到寄存器，`__atomic_*` 保证读改写的原子性。这是偏底层的老派写法，性能好但可移植性差。

### 2.5 BatchID 的黑魔法

`transport.h:97-110` 有一段值得注意的注释：

```cpp
    // NOTE ABOUT BatchID → BatchDesc conversion:
    // BatchID is an opaque 64-bit unsigned integer that carries a
    // BatchDesc pointer value. For performance reasons, this helper
    // reinterprets the integral handle directly as a BatchDesc reference.
    // The conversion intentionally bypasses any map or lookup to
    // minimize overhead on hot paths.
    // 【中译】BatchID 是一个不透明的 64 位整数，实际承载的是 BatchDesc 指针值。
    //   出于性能考虑，这个 helper 直接把整型句柄重新解释为 BatchDesc 引用，
    //   刻意绕开任何 map 查找，以最小化热路径开销。
    static inline BatchDesc &toBatchDesc(BatchID id) {
        return *reinterpret_cast<BatchDesc *>(id);
    }
```

**【逻辑】`BatchID` 就是 `BatchDesc*` 强转成的整数**。好处是 O(1) 且零查找开销；代价是调用方必须保证对象存活（用完才能 `freeBatchID`）。在 `MultiTransport::allocateBatchID` 里能看到这个赋值（`src/multi_transport.cpp:86`）：`batch_desc->id = BatchID(batch_desc);`

---

## 三、对外 API 全景

`include/transfer_engine.h:53-215` 定义了门面类。核心 API 按使用顺序列出：

| 阶段 | 方法 | 行号 | 作用 |
|------|------|------|------|
| **初始化** | `init(metadata_conn_string, local_server_name, ip_or_host_name, rpc_port)` | `:85` | 连元数据服务、绑端口、拓扑发现、装 transport |
| **注册内存** | `registerLocalMemory(addr, length, location, remote_accessible, update_metadata)` | `:110` | ★ RDMA 前提：pin 住内存并拿到 lkey/rkey |
| | `registerLocalMemoryBatch(buffer_list, location)` | `:144` | 批量注册 |
| **发现对端** | `openSegment(segment_name)` → `SegmentHandle` | `:102` | 按名字解析远端 segment |
| **传输** | `allocateBatchID(batch_size)` → `BatchID` | `:149` | 开一个批次 |
| | `submitTransfer(batch_id, entries)` | `:117` | ★ 异步提交，立即返回 |
| | `getTransferStatus(batch_id, task_id, status)` | `:163` | 轮询单个 task 状态 |
| | `getBatchTransferStatus(batch_id, status)` | `:166` | 轮询整批状态 |
| | `freeBatchID(batch_id)` | `:151` | 释放（未完成会拒绝） |
| **辅助** | `sendNotifyByID / getNotifies` | `:153,155` | 带外小消息通知 |
| | `probePeerAliveByID(target_id)` | `:161` | 探活 |
| | `getNicLoadStats(stats)` | `:168` | 网卡负载（inflight 字节、EWMA 带宽） |

**典型调用序列**（也就是 Store 内部做的事）：

```cpp
TransferEngine engine(true);                        // auto_discover = true
engine.init("P2PHANDSHAKE", "node-P0", "", 12345);
engine.registerLocalMemory(kv_buf, kv_len);         // 必须先注册！
auto seg = engine.openSegment("node-D0");           // 拿到目标句柄
auto batch = engine.allocateBatchID(1);
TransferRequest req{TransferRequest::WRITE, kv_buf, seg, target_off, kv_len};
engine.submitTransfer(batch, {req});                // 异步，立刻返回
TransferStatus st;
do { engine.getBatchTransferStatus(batch, st); }    // 轮询
while (st.s == TransferStatusEnum::WAITING);
engine.freeBatchID(batch);
```

> **【逻辑】为什么必须先 `registerLocalMemory`？** RDMA 网卡通过 DMA 直接访问内存，必须保证：① 物理页不被换出（pin）；② 网卡的 IOMMU 页表里有映射。注册就是干这两件事，返回的 `lkey`（本地）/`rkey`（远端授权）是后续每个 WR 的必填参数。这也是 Mooncake 要求上层用「注册过的 buffer」的根本原因 —— 你会在 02 篇看到 Store 的 `register_buffer()` 就是转发到这里。

---

## 四、多协议路由：MultiTransport

`submitTransfer` 进来后第一站是 `MultiTransport`，它负责**决定这个请求该交给哪个 Transport**。

### 4.1 支持的协议清单

`src/multi_transport.cpp:314-407` 是一个大 `if-else` 工厂，全部由编译期宏控制：

```cpp
Transport* MultiTransport::installTransport(const std::string& proto,
                                            std::shared_ptr<Topology> topo) {
    Transport* transport = nullptr;
    if (std::string(proto) == "rdma") {
        transport = new RdmaTransport();      // 【逻辑】唯一无条件编译的：RDMA 是主力
    }
#ifdef USE_TCP
    else if (std::string(proto) == "tcp") {
        transport = new TcpTransport();
    }
#endif
#ifdef USE_NVMEOF
    else if (std::string(proto) == "nvmeof") { transport = new NVMeoFTransport(); }
#endif
#ifdef USE_MNNVL
    else if (std::string(proto) == "nvlink") { transport = new NvlinkTransport(); }
#endif
#ifdef USE_CXL
    else if (std::string(proto) == "cxl") { transport = new CxlTransport(); }
#endif
    // ... ascend / hip / maca / efa / cxi / barex / ub / ubshmem / sunrise_link
    if (!transport) {
        LOG(ERROR) << "Unsupported transport " << proto
                   << ", please rebuild Mooncake";
        return nullptr;
    }
    if (transport->install(local_server_name_, metadata_, topo)) {
        delete transport;
        return nullptr;
    }
    transport_map_[proto] = std::shared_ptr<Transport>(transport);
    return transport;
}
```

完整协议表（含硬件归属）：

| proto | 类 | 硬件/场景 | 编译宏 |
|-------|-----|----------|-------|
| `rdma` | `RdmaTransport` | InfiniBand / RoCE，**主力路径** | 无条件 |
| `tcp` | `TcpTransport` | 无 RDMA 时兜底 | `USE_TCP` |
| `nvlink` | `NvlinkTransport` | NVIDIA MNNVL 多节点 NVLink | `USE_MNNVL` |
| `nvlink_intra` | `IntraNodeNvlinkTransport` | 单机内 GPU 直连 | `USE_INTRA_NVLINK` |
| `cxl` | `CxlTransport` | CXL 内存池（共享地址空间） | `USE_CXL` |
| `nvmeof` | `NVMeoFTransport` | NVMe over Fabrics + GPUDirect Storage | `USE_NVMEOF` |
| `efa` | `EfaTransport` | AWS EFA（libfabric，64 位 mr_key） | `USE_EFA` |
| `cxi` | `CxiTransport` | HPE Slingshot | `USE_CXI` |
| `ascend` | `HcclTransport` / `AscendDirectTransport` | 华为昇腾 NPU | `USE_ASCEND*` |
| `hip` / `maca` | `HipTransport` / `MacaTransport` | AMD / 沐曦 GPU IPC | `USE_HIP` / `USE_MACA` |
| `ub` / `ubshmem` | `UbTransport` / `UBShmemTransport` | 鲲鹏 UB 总线 | `USE_UB*` |

### 4.2 路由决策：selectTransport

`src/multi_transport.cpp:452-541`。基础逻辑非常简单 —— **看目标 segment 声明了什么协议**：

```cpp
Status MultiTransport::selectTransport(const TransferRequest& entry,
                                       Transport*& transport) {
    auto target_segment_desc = metadata_->getSegmentDescByID(entry.target_id);
    if (!target_segment_desc) {
        return Status::InvalidArgument("Invalid target segment ID " +
                                       std::to_string(entry.target_id));
    }
    auto proto = target_segment_desc->protocol;   // 【逻辑】★ 协议由目标 segment 决定
    // ... 多协议分支见下
    if (!transport_map_.count(proto)) {
        return Status::NotSupportedTransport("Transport " + proto +
                                             " not installed");
    }
    transport = transport_map_[proto].get();
    return Status::OK();
}
```

**多协议 segment 的优先级**（`multi_transport.cpp:468-525`，`ENABLE_MULTI_PROTOCOL` 下生效）才是有意思的部分。一个 segment 的 protocol 字段可以是 `"rdma,hip"` 这种复合值：

```cpp
    if (proto.find(',') != std::string::npos) {
        auto protocol_priority = [](const std::string& p) {
            // hip is intra-node GPU-IPC only. On a cross-node request a
            // hip+rdma segment must fall through to rdma
            if (p == "hip") return std::getenv("MC_DISABLE_HIP") ? 0 : 4;
            if (p == "maca") return std::getenv("MC_DISABLE_MACA") ? 0 : 4;
            if (p == "cxl") return 3;
            if (p == "rdma") return 2;
            if (p == "tcp") return 1;
            return 0;
        };
        const bool hip_reachable =
            isHipReachableTarget(target_segment_desc->name, local_server_name_);
        std::string chosen;
        int chosen_priority = -1;
        for (const auto& buffer : target_segment_desc->buffers) {
            uint64_t start = (buffer.protocol == "cxl")
                    ? buffer.offset + target_segment_desc->cxl_base_addr
                    : buffer.addr;
            if (entry.target_offset >= start &&
                entry.target_offset < start + buffer.length) {   // 【逻辑】地址落在这个 buffer 内
                if (buffer.protocol == "hip" && !hip_reachable) continue;
                int priority = protocol_priority(buffer.protocol);
                if (priority > chosen_priority) {   // 【逻辑】取优先级最高的
                    chosen = buffer.protocol;
                    chosen_priority = priority;
                }
            }
        }
```

**【逻辑】这段代码解决的是一个非常实际的问题**：同一块 GPU 内存可能同时注册给 `hip`（GPU IPC，单机内极快）和 `rdma`（跨机可达）。那么：
- 目标在**同一台机器** → 选 `hip`（优先级 4），走 GPU IPC，零网络开销；
- 目标在**另一台机器** → `hip_reachable` 为 false，跳过 hip，落到 `rdma`（优先级 2）。

**一个 segment 声明两种协议，就能自动实现「机内走捷径、跨机走网络」**，运维不需要手工配置。这是很漂亮的设计。

> **【C++】`auto protocol_priority = [](const std::string& p) { ... };`** 这是 lambda 表达式（匿名函数），`[]` 是捕获列表（这里为空，不捕获外部变量）。相当于在函数内部定义一个小工具函数。

---

## 五、RDMA 主路径：从 submitTransfer 到网卡

现在进入正题。`RdmaTransport::submitTransferTask` 是**整篇最重要的函数**，它完成「请求 → 切片 → 选网卡 → 入队」。

### 5.1 切片：为什么要切、怎么切

`src/transport/rdma_transport/rdma_transport.cpp:571-698`（分段讲解）。先看准备阶段：

```cpp
Status RdmaTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
    std::unordered_map<std::shared_ptr<RdmaContext>, std::vector<Slice *>>
        slices_to_post;                             // 【逻辑】按网卡分组待投递的 slice
    auto local_segment_desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    const size_t kBlockSize = globalConfig().slice_size;        // 【逻辑】切片大小
    const int kMaxRetryCount = globalConfig().retry_cnt;
    const size_t kFragmentSize = globalConfig().fragment_limit; // 【逻辑】末尾碎片阈值
    const size_t kSubmitWatermark =
        globalConfig().max_wr * globalConfig().num_qp_per_ep;   // 【逻辑】批量投递水位
```

然后是切片主循环：

```cpp
        for (uint64_t offset = 0; offset < request.length;
             offset += kBlockSize) {
            Slice *slice = getSliceCache().allocate();     // 【逻辑】从 thread_local 池取
            if (!slice->from_cache) { nr_slices++; }

            bool merge_final_slice =
                request.length - offset <= kBlockSize + kFragmentSize;
            //  【逻辑】★ 关键优化：如果剩余长度 ≤ 一片 + 碎片阈值，就把它并成最后一片，
            //     避免产生一个极小的尾片（小 WR 的开销占比过高）

            slice->source_addr = (char *)request.source + offset;
            slice->length =
                merge_final_slice ? request.length - offset : kBlockSize;
            slice->opcode = request.opcode;
            slice->rdma.dest_addr = request.target_offset + offset;
            slice->rdma.retry_cnt = request.advise_retry_cnt;
            slice->rdma.max_retry_cnt = kMaxRetryCount;
            slice->task = &task;
            slice->target_id = request.target_id;
            slice->status = Slice::PENDING;
            slice->ts = 0;
            task.slice_list.push_back(slice);
```

**【逻辑】`merge_final_slice` 的意义**：假设 `slice_size = 64KB`、`fragment_limit = 16KB`，传 130 KB。朴素切法得到 `64 + 64 + 2`，最后那个 2 KB 的 WR 几乎全是开销。有了这个判断，第二片会变成 `64 + 66`，只用两个 WR。

### 5.2 选网卡：Topology 亲和 + 失败重试

紧接着为每个 slice 选一张网卡（`rdma_transport.cpp:623-680`）：

```cpp
            int buffer_id = -1, device_id = -1,
                retry_cnt = request.advise_retry_cnt;
            bool found_device = false;
            if (request_buffer_id >= 0 && request_device_id >= 0) {
                auto &request_context = context_list_[request_device_id];
                if (request_context && request_context->active()) {
                    found_device = true;       // 【逻辑】快路径：整个 request 已选好网卡，直接复用
                    buffer_id = request_buffer_id;
                    device_id = request_device_id;
                }
            }
            while (retry_cnt < kMaxRetryCount && !found_device) {
                if (selectDevice(local_segment_desc.get(),
                                 (uint64_t)slice->source_addr, slice->length,
                                 buffer_id, device_id, retry_cnt++))
                    continue;                  // 【逻辑】选失败，retry_cnt++ 后换一张
                auto &context = context_list_[device_id];
                if (!context->active()) continue;   // 【逻辑】网卡不健康，跳过
                found_device = true;
                break;
            }
            if (!found_device) {
                // 【逻辑】所有网卡都不行 → 内存根本没注册，或全部网卡挂了
                LOG(ERROR)
                    << "Memory region not registered by any active device(s): "
                    << source_addr;
                return Status::AddressNotRegistered(...);
            } else {
                auto &context = context_list_[device_id];
                slice->rdma.source_lkey =
                    local_segment_desc->buffers[buffer_id].lkey[device_id];
                //  【逻辑】★ 注意：lkey 是【每张网卡一个】的数组！
                //     同一块内存注册到 8 张卡上，会得到 8 个不同的 lkey
                slices_to_post[context].push_back(slice);
                task.total_bytes += slice->length;
                __sync_fetch_and_add(&task.slice_count, 1);
            }

            if (nr_slices >= kSubmitWatermark) {
                for (auto &entry : slices_to_post)
                    entry.first->submitPostSend(entry.second);   // 【逻辑】达到水位就先投一批
                slices_to_post.clear();
                nr_slices = 0;
            }
```

**【逻辑】`lkey[device_id]` 这个细节值得停下来体会**：多网卡聚合的本质是「同一块内存向每张网卡都注册一次」，每次注册得到独立的 lkey。传输时选定网卡 → 取对应 lkey → 该网卡才能 DMA 这块内存。这就是为什么 `BufferDesc::lkey` 是 `std::vector<uint32_t>` 而不是单个值，也是代码里那句 `assert(local_segment_desc->buffers[buffer_id].lkey.size() == context_list_.size())` 的含义（网卡数 == lkey 数）。

`selectDevice` 的实现（`rdma_transport.cpp:843-889`）核心是查 Topology：

```cpp
int RdmaTransport::selectDevice(SegmentDesc *desc, uint64_t offset,
                                size_t length, std::string_view hint,
                                int &buffer_id, int &device_id,
                                int retry_count) {
    const auto &buffers = desc->buffers;
    for (buffer_id = 0; buffer_id < static_cast<int>(buffers.size());
         ++buffer_id) {
        const auto &buffer = buffers[buffer_id];
        // Check if offset is within buffer range
        if (offset < buffer.addr || length > buffer.length ||
            offset - buffer.addr > buffer.length - length) {
            continue;                          // 【逻辑】地址不落在这个 buffer 里
        }
        std::string location = buffer.name;    // 【逻辑】内存的「位置标签」，如 "cpu:0" / "cuda:0"
        // ...
        device_id = desc->topology.selectDevice(location, retry_count);
        if (device_id >= 0) return 0;          // 【逻辑】① 先按亲和性选
        device_id = desc->topology.selectDevice(kWildcardLocation, retry_count);
        if (device_id >= 0) return 0;          // 【逻辑】② 退化到通配（任意可用网卡）
    }
    return ERR_ADDRESS_NOT_REGISTERED;
}
```

**两级降级**：先找与内存位置亲和的网卡，找不到就用通配。`retry_count` 传给 Topology 用于**跳过已失败的候选**（见下节）。

### 5.3 Topology：亲和矩阵怎么表达

`include/topology.h:38-62`：

```cpp
struct TopologyEntry {
    std::string name;
    std::vector<std::string> preferred_hca;   // 【逻辑】首选网卡（同 NUMA / 同 PCIe switch）
    std::vector<std::string> avail_hca;       // 【逻辑】次选网卡（能用但更远）

    Json::Value toJson() const {
        Json::Value matrix(Json::arrayValue);
        Json::Value hca_list(Json::arrayValue);
        for (auto &hca : preferred_hca) { hca_list.append(hca); }
        matrix.append(hca_list);              // 【逻辑】[0] = preferred
        hca_list.clear();
        for (auto &hca : avail_hca) { hca_list.append(hca); }
        matrix.append(hca_list);              // 【逻辑】[1] = available
        return matrix;
    }
};

using TopologyMatrix =
    std::unordered_map<std::string /* storage type */, TopologyEntry>;
```

对应的 JSON 长这样（`storage type` → `[[首选], [次选]]`）：

```json
{
  "cuda:0": [["mlx5_0", "mlx5_1"], ["mlx5_2", "mlx5_3"]],
  "cuda:4": [["mlx5_4", "mlx5_5"], ["mlx5_0", "mlx5_1"]],
  "cpu:0":  [["mlx5_0"],           ["mlx5_1", "mlx5_2"]]
}
```

**【逻辑】读法**：GPU 0 上的内存优先用 `mlx5_0/1`（同一个 PCIe switch 下，不过 CPU），不行再用 `mlx5_2/3`。这正是「多网卡聚合」能真正跑出 142 GB/s 的前提 —— 如果 GPU 0 的数据从 `mlx5_7` 发出去，得跨 NUMA 走 UPI，带宽直接腰斩。

`Topology` 的公开接口（`topology.h:64-101`）：

```cpp
class Topology {
   public:
    int discover();                                   // 【逻辑】自动发现（扫 /sys 设备树）
    int discover(const std::vector<std::string> &filter);  // 【逻辑】带网卡白名单
    int parse(const std::string &topology_json);       // 【逻辑】从 JSON 手工指定
    int disableDevice(const std::string &device_name); // 【逻辑】摘掉坏网卡
    int selectDevice(const std::string storage_type, int retry_count = 0);
    int selectDeviceByLocalHca(const std::string storage_type,
                               std::string_view local_hca, int retry_count = 0);
    const std::vector<std::string> &getHcaList() const { return hca_list_; }
   private:
    TopologyMatrix matrix_;
    std::vector<std::string> hca_list_;
    bool use_round_robin_;                             // 【逻辑】同优先级内轮询，均衡负载
```

**`retry_count` 的作用**：`selectDevice(location, retry_count)` 内部大致是「在候选列表里取第 `(base + retry_count) % N` 个」。所以重试时自然换到下一张卡 —— 一张网卡挂了，流量自动漂移到其他卡，这就是**故障时的自动 failover**。

### 5.4 投递：Worker 线程与 CQ 轮询

Slice 入队后，真正的 `ibv_post_send` 由**独立的 worker 线程**完成。主循环在 `src/transport/rdma_transport/worker_pool.cpp:892-932`：

```cpp
void WorkerPool::transferWorker(int thread_id) {
    bindToSocket(numa_socket_id_);                  // 【逻辑】★ 绑 NUMA，避免跨节点访存
    const static uint64_t kWaitPeriodInNano = 100000000;  // 100ms
    uint64_t last_wait_ts = getCurrentTimeInNano();
    const bool can_post = workerCanPost(thread_id);
    const bool can_poll = workerCanPoll(thread_id);
    while (workers_running_.load(std::memory_order_relaxed)) {
        auto processed_slice_count = processed_slice_count_.load(...);
        auto submitted_slice_count = submitted_slice_count_.load(...);
        if (processed_slice_count == submitted_slice_count &&
            !hasOutstandingCq(thread_id)) {
            // 【逻辑】没活干：忙等 100ms 后进入条件变量休眠，省 CPU
            uint64_t curr_wait_ts = getCurrentTimeInNano();
            if (curr_wait_ts - last_wait_ts > kWaitPeriodInNano) {
                std::unique_lock<std::mutex> lock(cond_mutex_);
                parked_worker_count_.fetch_add(1, std::memory_order_acq_rel);
                // Double-check condition after acquiring lock to avoid lost wakeup.
                if (processed_slice_count_.load(...) == submitted_slice_count_.load() &&
                    !hasOutstandingCq(thread_id)) {
                    cond_var_.wait_for(lock, std::chrono::seconds(1));
                }
                parked_worker_count_.fetch_sub(1, std::memory_order_acq_rel);
                last_wait_ts = curr_wait_ts;
            }
            continue;
        }
        if (can_post)  { performPostSend(thread_id); }   // 【逻辑】① 投递
        if (can_poll)  { performPollCq(thread_id); }     // 【逻辑】② 收割
        last_wait_ts = getCurrentTimeInNano();
    }
}
```

**【逻辑】这个循环的设计取舍**：有活干时**纯忙轮询**（不 sleep，追求最低延迟）；连续 100 ms 没活才进入条件变量休眠（省 CPU）。这是延迟敏感系统的典型做法 —— 对照 SGLang 的 `event_loop_normal()`，那边是「阻塞在 ZMQ recv」，因为 Python 场景下 GIL 和调度开销远大于唤醒延迟。

**双重检查（double-check）**是并发编程的必要细节：先设 `parked_worker_count_`，再在持锁状态下重新检查条件。否则可能出现「检查完 → 生产者提交并 notify → 自己才 wait」的丢失唤醒。

`performPostSend`（`worker_pool.cpp:328-499`）负责建连并投递，核心片段：

```cpp
    for (auto &entry : local_slice_queue) {
        if (entry.second.empty()) continue;
        // 【逻辑】entry.first 是对端网卡路径（"节点@mlx5_x"），entry.second 是待投 slice
        if (!isRailAvailable(entry.first) ||
            context_.isConnectPaused(entry.first)) {
            for (auto &slice : entry.second) failed_slice_list.push_back(slice);
            entry.second.clear();
            continue;                       // 【逻辑】这条「轨道」暂停中，直接判失败等重试
        }
        auto endpoint = context_.endpoint(entry.first);   // 【逻辑】取（或新建）QP 端点
        if (!endpoint) { /* 全部转 failed */ continue; }
        if (!endpoint->connected()) {
            int setup_ret = endpoint->setupConnectionsByActive();  // 【逻辑】主动握手建 QP
            if (setup_ret) {
                // 【逻辑】建连失败：优先换对端网卡（peer rail），否则换本地网卡
                bool local_context_inactive = !context_.active();
                bool has_peer_alternative = false;
                for (auto &slice : entry.second) {
                    if (hasAvailablePeerRailAlternative(slice, entry.first)) {
                        has_peer_alternative = true; break;
                    }
                }
                if (has_peer_alternative) {
                    markRailFailed(entry.first, true);
                    redispatch_counter_++;      // 【逻辑】触发其他线程重新分派
                } else if (local_context_inactive) {
                    context_.set_active(false);
                    refreshPublishedLocalTopology();  // 【逻辑】★ 把「我这张卡坏了」发布到元数据
                    redispatch_counter_++;
                }
                context_.deleteEndpointByPtr(endpoint.get());
                // ...
            }
        }
        for (auto &slice : entry.second) { slice->rdma.endpoint = endpoint.get(); }
        endpoint->submitPostSend(entry.second, failed_slice_list);   // 【逻辑】★ 真正 post
    }
```

**【逻辑】「rail」（轨道）这个概念**：在多轨网络（multi-rail）里，本地 8 张卡 × 对端 8 张卡 = 64 条可能的路径。一条 rail 坏了（对端某张卡故障），只需把该 rail 标记失败并把 slice 重新分派到其他 rail，不影响整体吞吐。`refreshPublishedLocalTopology()` 更进一步 —— **把自己网卡的故障状态发布到元数据服务**，让其他节点也别再往这张卡发。这是分布式系统里的「主动降级广播」。

`performPollCq`（`worker_pool.cpp:523-...`）收割完成事件：

```cpp
void WorkerPool::performPollCq(int thread_id) {
    // ... 统计 poll 间隔（用于诊断「CQ 饿死」）
    const static size_t kPollCount = 64;
    for (int cq_index = 0; cq_index < context_.cqCount(); cq_index++) {
        ibv_wc wc[kPollCount];
        int nr_poll = context_.poll(kPollCount, wc, cq_index);  // 【逻辑】批量取完成事件
        if (nr_poll < 0) {
            LOG(ERROR) << "Worker: Failed to poll completion queues";
            continue;
        }
        for (int i = 0; i < nr_poll; ++i) {
            Transport::Slice *slice = (Transport::Slice *)wc[i].wr_id;
            //  【逻辑】★ wr_id 就是 Slice 指针！这是 RDMA 编程的标准技巧：
            //     post 时把上下文指针塞进 wr_id，完成时原样取回
            if (wc[i].status != IBV_WC_SUCCESS) {
                if (wc[i].status == IBV_WC_WR_FLUSH_ERR) {
                    // Flush errors are generated when QPs transition to ERR state
                    // during normal endpoint destruction. They are not real
                    // network errors.
                    // 【中译】QP 正常销毁时会产生 flush 错误，这不是真的网络错误，
                    //    不应触发 rail 故障处理与端点删除。
                }
                // ... 其余情况：加入 failed_slice_list，走重试或标记失败
            }
            // 成功则 slice->markSuccess()
        }
    }
}
```

**【逻辑】`wr_id` 携带指针**是 RDMA 编程的经典手法。`ibv_wc` 只回传一个 64 位 `wr_id`，把 `Slice*` 塞进去，完成时强转回来，就能 O(1) 找到上下文 —— 无需任何查找表。

### 5.5 一次 WRITE 的完整调用链

把上面串起来（统一示例 t2，P0 把 A 的 KV 写到目标 segment）：

```
Store 层 Client::Put()
  │
  ▼
TransferEngine::submitTransfer(batch, [req])            transfer_engine.cpp:...
  │
  ▼
MultiTransport::submitTransfer()                        multi_transport.cpp:115
  ├─ selectTransport(req) → RdmaTransport                multi_transport.cpp:452
  │     └─ 看 target_segment_desc->protocol
  └─ RdmaTransport::submitTransferTask(tasks)            rdma_transport.cpp:571
        ├─ 按 slice_size 切片（末片可合并）
        ├─ selectDevice() → Topology::selectDevice()     rdma_transport.cpp:843
        │     └─ 亲和优先 → 通配降级 → retry 换卡
        ├─ 取 lkey[device_id]（每卡一个 lkey）
        └─ RdmaContext::submitPostSend(slices)  ← 入队，函数返回（异步！）
              │
              │   ══════ 线程边界：以下在 worker 线程 ══════
              ▼
        WorkerPool::transferWorker()                     worker_pool.cpp:892
          ├─ performPostSend()                           worker_pool.cpp:328
          │     ├─ context_.endpoint(peer_nic_path) 取 QP
          │     ├─ endpoint->setupConnectionsByActive() 首次握手
          │     └─ endpoint->submitPostSend() → ibv_post_send()   ★ 数据出网卡
          └─ performPollCq()                             worker_pool.cpp:523
                ├─ context_.poll(64, wc, cq_index) → ibv_poll_cq()
                ├─ slice = (Slice*)wc[i].wr_id
                └─ slice->markSuccess() → task 计数 +1 → batch 完成判定
                      │
                      ▼
        调用方 getBatchTransferStatus() 观察到 COMPLETED
```

---

## 六、元数据与握手：对端在哪、rkey 从哪来

RDMA 要求发起方知道**对端的 QP 号、GID、以及目标内存的 rkey**。这些由 `TransferMetadata` 负责交换。

### 6.1 支持的元数据后端

`init()` 的第一个参数 `metadata_conn_string` 决定后端：

| 取值 | 后端 | 说明 |
|------|------|------|
| `"etcd://host:2379"` | etcd | 生产推荐，强一致 |
| `"redis://host:6379"` | Redis | 轻量 |
| `"http://host:8080/metadata"` | HTTP | 自带简易服务器（`mooncake_http_metadata_server`） |
| `"P2PHANDSHAKE"` | 点对点 | **不需要任何外部服务**，直接 TCP 握手交换。测试/小规模首选 |

每个节点启动时把自己的 `SegmentDesc`（含所有已注册 buffer 的 addr/length/lkey/rkey、以及本机 topology）发布上去；`openSegment(name)` 就是按名字把对端的 `SegmentDesc` 拉下来。

### 6.2 为什么 rkey 也是数组

`SegmentDesc::buffers[i]` 里的 `lkey` 和 `rkey` 都是 `std::vector` —— 原因和 5.2 说的一样：**一块内存注册到 N 张网卡，就有 N 组 key**。发起方选定「用本地 `mlx5_2` 发往对端 `mlx5_3`」后，要取的是对端 buffer 在 `mlx5_3` 上的那个 rkey。Slice 里的 `rkey_index` / `dest_rkeys` 就是干这个的。

---

## 七、关键配置项

来自 `globalConfig()`（`include/config.h`），全部可用环境变量覆盖：

| 环境变量 | 默认 | 含义 | 调优建议 |
|---------|------|------|---------|
| `MC_SLICE_SIZE` | 64 KB 量级 | 切片大小 | 大块传输可增大以减少 WR 数量 |
| `MC_FRAGMENT_LIMIT` | — | 末片合并阈值 | 见 5.1 |
| `MC_RETRY_CNT` | — | 单 slice 最大重试次数 | 网络抖动多可增大 |
| `MC_MAX_WR` | — | 每 QP 最大 outstanding WR | 与 `num_qp_per_ep` 共同决定投递水位 |
| `MC_NUM_QP_PER_EP` | — | 每个端点的 QP 数 | 多 QP 提升并行度 |
| `MC_NUM_CQ_PER_CTX` | — | 每个网卡上下文的 CQ 数 | 与 worker 线程数匹配 |
| `MC_MAX_CQE` | — | CQ 深度 | 太小会丢完成事件 |
| `MC_HANDSHAKE_PORT` | — | 握手监听端口 | 容器环境需固定 |
| `MC_TCP_BIND_ADDRESS` | — | 绑定网卡地址 | 多网段环境必设 |
| `MC_SLICE_TIMEOUT` | 0（关） | slice 超时秒数 | 见 `multi_transport.cpp:212` 的 `checkSliceTimeout` |
| `MC_USE_TENT` | 未设 | 切到新 TENT 实现 | 本系列不分析该路径 |
| `MC_DISABLE_HIP` / `MC_DISABLE_MACA` | 未设 | 禁用 GPU IPC 路径 | 见 4.2 |

超时检查在 `MultiTransport::getTransferStatus`（`multi_transport.cpp:211-225`）：

```cpp
    auto checkSliceTimeout = [&](const Transport::TransferTask& t) -> bool {
        if (globalConfig().slice_timeout <= 0) return false;   // 【逻辑】默认关闭
        auto current_ts = getCurrentTimeInNano();
        const int64_t kPacketDeliveryTimeout =
            globalConfig().slice_timeout * 1000000000;
        for (auto& slice : t.slice_list) {
            auto ts = slice->ts;                    // 【逻辑】slice 投递时记的时间戳
            if (ts > 0 && current_ts > ts &&
                current_ts - ts > kPacketDeliveryTimeout) {
                LOG(INFO) << "Slice timeout detected";
                return true;
            }
        }
        return false;
    };
```

**【逻辑】注意这是「懒检查」**：只在调用方查询状态时才扫一遍 slice 的时间戳，没有独立的定时器线程。省了一个线程，代价是超时发现不及时 —— 对于「调用方本来就要轮询」的场景是合理取舍。

---

## 八、与 vLLM / SGLang 的对照

| 关注点 | vLLM / SGLang 里的对应物 | Transfer Engine |
|-------|------------------------|-----------------|
| 数据搬运的最小单位 | Block（16 token）/ RadixTree 节点 | **Slice**（按字节切，与 token 无关） |
| 谁决定搬到哪 | 调度器 | 上层（Store）决定，引擎只管搬 |
| 异步模型 | `asyncio` / CUDA stream | **提交 + 轮询**（`submitTransfer` / `getTransferStatus`） |
| 完成通知 | Python future / event | 原子计数 + 可选条件变量 |
| 多卡并行 | NCCL（集合通信，同步语义） | **多网卡点对点**（无集合语义，各 slice 独立） |
| 失败处理 | 请求重试 | **slice 级重试 + rail failover + 拓扑降级广播** |

**一个重要区别**：NCCL 是集合通信（all-reduce 那类，要求所有 rank 同步参与），Transfer Engine 是**点对点单边操作**（RDMA READ/WRITE，对端 CPU 完全不参与）。PD 分离场景下后者才合适 —— Prefill 实例和 Decode 实例的生命周期完全独立，没法搞集合同步。

---

## 九、C++ 语法速查表

| 语法 | 含义 | 本篇出处 |
|------|------|---------|
| `union { struct {...} rdma; struct {...} tcp; }` | 联合体，成员共享内存，同时只有一个有效 | Slice 协议私有字段 |
| `reinterpret_cast<BatchDesc*>(id)` | 无检查的指针重解释 | `toBatchDesc` |
| `thread_local` | 每线程一份独立实例 | `ThreadLocalSliceCache` |
| `volatile uint64_t` | 阻止编译器缓存到寄存器 | `TransferTask` 计数 |
| `__atomic_fetch_add(p, v, __ATOMIC_RELAXED)` | GCC 内建原子加，最弱内存序 | `markSuccess` |
| `std::atomic<bool>` + `memory_order_acquire/release` | 标准原子量 + 内存序 | `BatchDesc::is_finished` |
| `auto f = [](const T& x) { ... };` | lambda 匿名函数 | `protocol_priority` |
| `virtual ... = 0;` | 纯虚函数，派生类必须实现 | `Transport::submitTransfer` |
| `std::shared_ptr<T>` / `std::weak_ptr<T>` | 引用计数智能指针 / 弱引用 | `RdmaContext` 管理 |
| `tl::expected<T, E>` | 「要么值要么错误」，类似 Rust `Result` | Store 层大量使用（02 篇） |
| `#ifdef USE_XXX` | 条件编译 | 各协议开关 |
| `std::string_view` | 不拥有所有权的字符串视图，零拷贝 | `selectDevice(..., hint)` |
| `struct S { int a; }; S s{1};` | 聚合初始化 | `TransferRequest` 构造 |
| `override` | 显式标记覆盖虚函数 | 各 Transport 实现 |

---

## 十、必记要点

1. **三层结构**：`BatchDesc` → `TransferTask`（= 一个 `TransferRequest`）→ `Slice`（= 一个 RDMA WR）。完成判定靠计数相等。
2. **`BatchID` 就是 `BatchDesc*` 强转的整数**，O(1) 无查找，但调用方负责生命周期。
3. **必须先 `registerLocalMemory`**：RDMA 需要 pin 内存 + 拿 lkey/rkey。
4. **`lkey`/`rkey` 是每张网卡一个的数组** —— 这是多网卡聚合的实现基础。
5. **选网卡两级降级**：Topology 亲和 → 通配；`retry_count` 递增实现自动换卡 failover。
6. **`merge_final_slice`** 避免产生无意义的小尾片。
7. **`wr_id` 携带 `Slice*`**，完成事件 O(1) 找回上下文。
8. **worker 线程「有活忙轮询、闲 100ms 后休眠」**，兼顾延迟与 CPU。
9. **多协议 segment（如 `"rdma,hip"`）能自动实现机内走 GPU IPC、跨机走 RDMA**。
10. **网卡故障会被发布到元数据服务**（`refreshPublishedLocalTopology`），让全集群绕开坏卡。

---

> 上一篇：[00 · 总览与架构](./00-总览与架构.md)
> 下一篇：[02 · Mooncake Store 缓存池](./02-MooncakeStore缓存池.md) —— 有了搬运能力，接下来看「东西存哪、谁记得住」：对象模型、两阶段写入与 Master 元数据服务。
