# 03 · PagedAttention 与 KV Cache 管理 —— vLLM 的招牌技术

> 承接 [02](02-调度器-连续批处理.md)。[02](02-调度器-连续批处理.md) 里反复出现的 `allocate_slots`（申请显存）、「前缀缓存命中」到底怎么实现？本文讲透 vLLM 最核心的创新：像操作系统分页一样管理 KV 显存。
> 注释分两类：`【逻辑】`讲原理；`【Python】`讲语法。
> 对应源码：`vllm/v1/core/kv_cache_manager.py`、`block_pool.py`、`kv_cache_utils.py`。

---

## 0. 为什么需要 PagedAttention —— 先理解 KV Cache 是什么

### 0.1 KV Cache 从哪来

Transformer 的注意力机制里，每个 token 都要和它前面所有 token 做计算。为此每个历史 token 会算出一对张量：**Key（K）和 Value（V）**。

自回归生成时，如果每生成一个新 token 都把前面所有 token 的 K/V 重算一遍，计算量是平方级的、极其浪费。**KV Cache 就是把算过的 K/V 存起来复用**：生成第 N 个 token 时，前 N-1 个的 K/V 直接从缓存拿，只算当前这一个。

代价是：**KV Cache 很占显存**。一个请求的 KV 大小 = 序列长度 × 层数 × 隐藏维度 × 2(K和V) × 数据类型字节数。长上下文时能到几个 GB。

### 0.2 传统方案的问题：显存碎片

最朴素的做法：给每个请求预留一块「连续显存」，按它可能的最大长度分配。问题：
- **内部碎片**：请求实际只用了 100 token，却按 max_len=2048 预留，浪费 95%。
- **外部碎片**：请求长度各异、来去不定，连续的大块显存被切得七零八落，明明总量够却分不出连续块。

### 0.3 PagedAttention 的解法：借鉴操作系统「分页」

操作系统管理内存时，不要求进程占用连续物理内存，而是切成固定大小的「页（page）」，逻辑连续、物理分散，用「页表」映射。

vLLM 把这套搬到 KV Cache：
- 把 KV 显存切成固定大小的 **block（块）**，每块存固定数量 token 的 KV（比如 16 个 token/块）。
- 一个请求的 KV **逻辑上连续，物理上可以分散在任意 block**。
- 用一张 **block table（块表）** 记录「这个请求的第几段 token 在哪个物理块」。
- attention 计算时，kernel 按块表去各个物理块取 K/V。

好处：
- **几乎无碎片**：按需一块一块分配，用多少给多少。
- **前缀共享**：两个请求有相同的 prompt 前缀 → 直接**共用同一批物理块**，省显存又省算力（Prefix Caching）。

```
逻辑视图（请求看到的连续序列）
  token: [t0 t1 t2 t3 | t4 t5 t6 t7 | t8 t9 ...]
          └─逻辑块0─┘  └─逻辑块1─┘  └逻辑块2┘
                 │            │           │
  block table:   ▼            ▼           ▼
物理视图      物理块#37     物理块#5    物理块#42   （物理上完全分散）
```

理解了这个类比，下面的代码就好懂了。三个核心角色：

- **`KVCacheBlock`**：一个块的元数据（在哪、被谁用、内容哈希）。
- **`BlockPool`**：物理块的总仓库，管理谁空闲谁占用。
- **`KVCacheManager`**：对上层（调度器）的门面，`allocate_slots`（申请）、`free`（释放）都在这。

> 本篇沿用 [`00` 篇](00-全局架构与数据流总览.md)的统一示例（A/B 两请求，共享 40-token 的 system prompt）。

### 0.4 `allocate_slots` + 前缀缓存 内部流程图 ★

[02 篇](02-调度器-连续批处理.md)每次调 `allocate_slots` 时，内部发生的事：

```
调度器调用 allocate_slots(request, num_new_tokens)
        │
        ▼
┌──────────────────────────────────────────────────┐
│（调度器在此之前已调 get_computed_blocks 做前缀命中）│
│  用 request.block_hashes 去 BlockPool 哈希表查     │
│  find_longest_cache_hit → 命中的块 touch() 复用    │
│  → num_computed_tokens 直接 > 0（省掉这段计算）     │
└──────────────────┬───────────────────────────────┘
                   ▼ allocate_slots 三步走：
┌──────────────────────────────────────────────────┐
│ 步骤1: 算 已算token(含命中) + 要新算token → 需几块  │
├──────────────────────────────────────────────────┤
│ 步骤2: 空闲块够吗?                                  │
│    required_blocks > available_blocks ?            │
│      是 → return None ──► 触发抢占/停放新(见02篇)    │
│      否 → 继续                                     │
├──────────────────────────────────────────────────┤
│ 步骤3: get_new_blocks(需要的块数)                   │
│    ├ popleft_n: 从空闲链表【头】取块 (LRU 最久未用)  │
│    ├ _maybe_evict_cached_block: 清掉块里的旧缓存    │
│    └ ref_cnt += 1  (标记占用)                      │
│    然后 cache_blocks: 把装满的块登记进哈希表         │
│    (供以后别的请求前缀命中 → 形成正反馈)             │
└──────────────────┬───────────────────────────────┘
                   ▼
          返回新分到的 block 列表给调度器
```

块的生命周期（`ref_cnt` 状态机，全篇的灵魂）：

```
 空闲(ref_cnt=0,在链表里) ──get_new_blocks/touch──▶ 占用(ref_cnt≥1,离开链表)
        ▲                                                    │
        │                                                    │ free
        └──────────── ref_cnt 减到 0，append 回链表尾 ◀───────┘
   (仍保留哈希，可被前缀命中"抢救"；直到被 evict 才真正清空)
```

---

## 1. 一个块长什么样：`KVCacheBlock`

文件：`vllm/v1/core/kv_cache_utils.py`（第 118 行）

```python
@dataclass
class KVCacheBlock:
    """KV-cache block metadata."""

    # Block ID, ranging from 0 to num_gpu_blocks - 1.
    block_id: int
    # Reference count.
    ref_cnt: int = 0
    # The hash key (block hash + group id) of the block, only available
    # when the block is full and cached.
    _block_hash: BlockHashWithGroupId | None = None
    _block_hash_num_tokens: int | None = None

    # Used to construct a doubly linked list for free blocks.
    prev_free_block: "KVCacheBlock | None" = None
    next_free_block: "KVCacheBlock | None" = None

    # Whether the block is a null block that should never be cached.
    is_null: bool = False
```

【Python 重点 —— `@dataclass`】

- `@dataclass` 是一个**装饰器**（回顾 [02](02-调度器-连续批处理.md)：装饰器就是加在类/函数上以 `@` 开头的修饰符，给它附加行为）。
- 加了 `@dataclass`，Python 会**自动帮你生成 `__init__` 构造函数**等样板代码。也就是说，你只需像上面这样列出字段名和类型，就能 `KVCacheBlock(block_id=5)` 这样创建对象，不用手写 `def __init__(self, block_id, ...)`。
- `ref_cnt: int = 0` :字段带默认值，创建时不传就用 0。
- `_block_hash: ... = None` :`_` 开头表示「内部字段」，外部应通过下面的 `@property` 访问。

【逻辑 —— 每个字段的含义（这是理解全章的地基）】

- `block_id`：物理块编号，0 到 (总块数-1)。**这是块的身份证**。
- `ref_cnt`（引用计数）：**多少个请求正在用这个块**。
  - `ref_cnt == 0` → 没人用，是「空闲块」，可以被回收/复用。
  - `ref_cnt > 0` → 有请求在用，不能动。
  - 这是「前缀共享」的关键：两个请求共用一个块，`ref_cnt` 就是 2。
- `_block_hash`：块内容的哈希值。**只有「装满且要缓存」的块才有哈希**。前缀缓存就是靠哈希匹配来判断「这段内容之前算过没有」。
- `prev_free_block` / `next_free_block`：指向前一个/后一个空闲块的指针，用来把所有空闲块串成一个**双向链表**（下节详解，这是 LRU 逐出的关键）。
  - 【Python】类型注解写成字符串 `"KVCacheBlock | None"`（带引号）：因为类定义还没结束就要引用自己，用字符串形式「前向引用」避免报错。
- `is_null`：是否是「空块」（占位用，永不缓存）。

### 1.1 `@property` —— 把方法伪装成属性

```python
    @property
    def block_hash(self) -> BlockHashWithGroupId | None:
        return self._block_hash
```

【Python】`@property` 装饰器让你**用访问属性的语法去调用方法**。即：写 `block.block_hash`（没有括号）实际上会调用这个方法。好处是对外像个只读属性，对内可以加逻辑、隐藏 `_block_hash` 这个内部字段。这是 Python 封装的常用手法。

---

<a id="sec-2"></a>
## 2. 空闲块的组织：`FreeKVCacheBlockQueue`（散落的关键实现）

文件：`vllm/v1/core/kv_cache_utils.py`（第 184 行）

所有 `ref_cnt == 0` 的空闲块，需要一个结构管起来。vLLM **没有用现成的队列，而是自己手写了一个双向链表**。为什么？看它的文档：

```python
class FreeKVCacheBlockQueue:
    """This class organizes a list of KVCacheBlock objects to a doubly linked
    list of free blocks. We implement this class instead of using Python
    builtin deque to support removing a block in the middle of the queue
    in O(1) time. ...this class does not allocate any Python objects when
    manipulating the linked list. Instead, this class manipulates the
    prev_free_block and next_free_block attributes of the given blocks.
    """
```

【逻辑 —— 为什么自己写链表】

- 需求：不仅要「从头取空闲块」「往尾加空闲块」，还要**从队列中间 O(1) 删除任意块**。
  - 什么时候删中间的？—— 当一个空闲块因为「前缀缓存命中」被某请求重新占用时（`touch`），要把它从空闲队列里摘出来。
- Python 内置的 `deque` 不支持 O(1) 删中间元素，所以自己用双向链表实现。
- 而且它**不额外创建节点对象**：直接复用 `KVCacheBlock` 自带的 `prev_free_block`/`next_free_block` 两个指针字段。零额外内存分配，极致性能。

### 2.1 构造：把块串成链表 + 哨兵节点

```python
    def __init__(self, blocks: list[KVCacheBlock]) -> None:
        self.num_free_blocks = len(blocks)

        # Initialize doubly links of consecutive blocks
        for i in range(self.num_free_blocks):
            if i > 0:
                blocks[i].prev_free_block = blocks[i - 1]
            if i < self.num_free_blocks - 1:
                blocks[i].next_free_block = blocks[i + 1]

        # Create a fake head and a tail block ...
        self.fake_free_list_head = KVCacheBlock(block_id=-1)
        self.fake_free_list_tail = KVCacheBlock(block_id=-1)
        if self.num_free_blocks > 0:
            self.fake_free_list_head.next_free_block = blocks[0]
            blocks[0].prev_free_block = self.fake_free_list_head
            self.fake_free_list_tail.prev_free_block = blocks[-1]
            blocks[-1].next_free_block = self.fake_free_list_tail
        else:
            self.fake_free_list_head.next_free_block = self.fake_free_list_tail
            self.fake_free_list_tail.prev_free_block = self.fake_free_list_head
```

【逐行】

- `for i in range(self.num_free_blocks):`
  - 【Python】`range(n)` 产生 0,1,...,n-1 的序列。`for i in range(n)` 就是最典型的「循环 n 次、i 是下标」。
  - 【逻辑】把每个块的 `prev/next` 指针指向相邻块，串成链：块0 ↔ 块1 ↔ 块2 ...
- `blocks[i - 1]` / `blocks[i + 1]` :用下标访问列表元素，`blocks[-1]` 是最后一个（Python 支持负数下标从末尾数）。
- **哨兵节点（fake head/tail）**：`block_id=-1` 的两个假块，放在链表头尾。
  - 【逻辑】这是链表编程的经典技巧：有了「假头假尾」，中间任何真实块都保证有 `prev` 和 `next`，删除/插入时**不用特判「是不是第一个/最后一个」**，代码大大简化、少 bug。

### 2.2 取一个空闲块：`popleft`（LRU 的「最久未用」端）

```python
    def popleft(self) -> KVCacheBlock:
        """Pop the first free block and reduce num_free_blocks by 1."""
        if (self.fake_free_list_head.next_free_block is self.fake_free_list_tail
                or self.fake_free_list_head.next_free_block is None):
            ...
            raise ValueError("No free blocks available")

        first_block: KVCacheBlock = self.fake_free_list_head.next_free_block
        ...
        # Connect fake_head and the next block of first_block
        self.fake_free_list_head.next_free_block = first_block.next_free_block
        first_block.next_free_block.prev_free_block = self.fake_free_list_head
```

【逻辑】从链表头取块（`fake_head` 后面的第一个真实块），并把它从链表摘掉（让 fake_head 直接指向第二个块）。

【逻辑 —— 为什么是 LRU】新释放的块从**尾部**加入（后面 `append`），从**头部**取用（`popleft`）。所以头部永远是「最久没被碰过」的块。当需要腾地方时，优先复用/逐出最久未用的——这就是 **LRU（Least Recently Used）** 缓存淘汰策略。这对前缀缓存很重要：热门前缀的块经常被 touch、留在缓存里；冷门前缀的块沉到头部、被优先逐出。

【Python】`is` 判断「是不是同一个对象」（比 `==` 更严格，比较身份而非值）。这里判断「头的下一个是不是就是尾」→ 空链表。

---

## 3. 物理块仓库：`BlockPool.get_new_blocks`

文件：`vllm/v1/core/block_pool.py`（第 647 行）

`BlockPool` 是所有物理块的总管。申请新块的核心方法：

```python
    def get_new_blocks(self, num_blocks: int) -> list[KVCacheBlock]:
        """Get new blocks from the free block pool."""
        if num_blocks > self.get_num_free_blocks():
            raise ValueError(f"Cannot get {num_blocks} free blocks from the pool")

        ret: list[KVCacheBlock] = self.free_block_queue.popleft_n(num_blocks)

        if self.enable_caching:
            for block in ret:
                self._maybe_evict_cached_block(block)
                assert block.ref_cnt == 0
                block.ref_cnt += 1
                ...
        else:
            for block in ret:
                assert block.ref_cnt == 0
                block.ref_cnt += 1
                ...
        return ret
```

【逐行】

- `if num_blocks > self.get_num_free_blocks(): raise ...`
  - 【逻辑】要的比空闲的还多，直接报错。（注意：上层 `allocate_slots` 会先检查、不够就返回 None，所以正常不会走到这个异常。）
- `ret = self.free_block_queue.popleft_n(num_blocks)`
  - 【逻辑】从空闲链表头一次取 `num_blocks` 个块（就是上一节的 `popleft` 批量版）。取的是最久未用的块。
- `self._maybe_evict_cached_block(block)`
  - 【逻辑 —— 关键】取到的空闲块**可能之前还缓存着某段旧内容的 KV**（`ref_cnt=0` 但仍有哈希，留着以备前缀命中）。现在要拿它装新内容，就得先**逐出（evict）**：清掉它的哈希、从哈希表里删掉。这就是「用它来装新数据 = 放弃它缓存的旧数据」。
- `block.ref_cnt += 1`
  - 【逻辑】引用计数从 0 变 1，标记「现在有人用了，不再是空闲块」。

【Python】`for block in ret:` 遍历列表。两个分支（enable_caching 与否）逻辑几乎一样，只差 `_maybe_evict_cached_block` 一步——代码注释里明说「为了只遍历一次而故意重复了代码」，是性能取舍。

### 3.1 对偶操作：`touch`（前缀命中时「抢救」一个空闲块）

```python
    def touch(self, blocks: Sequence[KVCacheBlock]) -> None:
        for block in blocks:
            # ref_cnt=0 means this block is in the free list, so remove it.
            if block.ref_cnt == 0 and not block.is_null:
                self.free_block_queue.remove(block)
            block.ref_cnt += 1
```

【逻辑】前缀缓存命中时：某个空闲块（`ref_cnt=0`）里正好缓存着新请求需要的前缀 KV。此时不能让它被逐出，于是：
1. 若它还躺在空闲链表里（`ref_cnt==0`），用 `remove` **从链表中间 O(1) 摘出来**（这就是 [§2](#sec-2) 自造链表的意义！）。
2. `ref_cnt += 1`，标记被新请求引用。

`get_new_blocks`（拿新块装新数据）和 `touch`（复用旧块的缓存内容）是一对反向操作，共同实现「按需分配 + 前缀复用」。

---

## 4. 上层门面：`KVCacheManager.allocate_slots`

文件：`vllm/v1/core/kv_cache_manager.py`（第 345 行）

这是**调度器唯一直接调用的申请入口**（02 里见过）。它协调 BlockPool，为请求的新 token 备好显存。函数很长、分支多（sliding window、connector、encoder、投机解码），但**主干只有三步**，文档里写得很清楚：

```python
        The allocation has three stages:
        - Free unnecessary blocks in `comp` and check
           if we have sufficient free blocks (return None if not).
        - Handle prefix tokens ...
        - Allocate new blocks for tokens to be computed (`new + lookahead`)
```

抓主干看这几段：

### 4.1 入口校验与计数

```python
        if num_new_tokens == 0 and num_external_computed_tokens == 0:
            raise ValueError("num_new_tokens must be greater than 0 ...")

        num_local_computed_tokens = (
            request.num_computed_tokens + num_new_computed_tokens
        )
        total_computed_tokens = min(
            num_local_computed_tokens + num_external_computed_tokens,
            self.max_model_len,
        )
```

【逻辑】先算清楚这个请求「已经算好多少 token」（含本地前缀缓存命中的 + 外部 connector 提供的），并且不超过模型最大长度 `max_model_len`。这些数字决定后面要不要新分块、分几块。

### 4.2 算需要几块、够不够（不够就返回 None）

```python
        num_tokens_main_model = total_computed_tokens + num_new_tokens
        num_tokens_need_slot = min(
            num_tokens_main_model + num_lookahead_tokens, self.max_model_len
        )
        ...
        num_blocks_to_allocate = self.coordinator.get_num_blocks_to_allocate(...)

        available_blocks = self.block_pool.get_num_free_blocks() - reserved_blocks
        required_blocks = num_blocks_to_allocate + watermark_blocks
        if required_blocks > available_blocks:
            # Cannot allocate new blocks
            return None
```

【逻辑 —— 这是 [02](02-调度器-连续批处理.md) 抢占逻辑的源头】

- 算出「这个请求总共要占多少 token 的槽位」→ 换算成「要多少块」`num_blocks_to_allocate`。
- 对比「可用块数」。**不够就 `return None`**。
- 回顾 [02](02-调度器-连续批处理.md)：调度器拿到 `None`，对 running 请求就触发抢占、对 waiting 请求就停止放行。**所有显存压力的决策，源头都是这里的这个 `return None`。**
- `watermark_blocks`（水位线）：给等待/被抢占的请求留一点余量块，避免刚放进来又立刻卡死，是个稳定性设计。

### 4.3 真正分配 + 缓存

```python
        new_blocks = self.coordinator.allocate_new_blocks(
            request.request_id, num_tokens_need_slot, num_tokens_main_model, num_encoder_tokens,
        )
        ...
        num_tokens_to_cache = min(total_computed_tokens + num_new_tokens, request.num_tokens)
        self.coordinator.cache_blocks(request, num_tokens_to_cache)

        return self.create_kv_cache_blocks(new_blocks)
```

【逻辑】
- `allocate_new_blocks`：向 BlockPool 要新块（内部最终调 §3 的 `get_new_blocks`）。
- `cache_blocks`：把**装满的块**登记进哈希表，供以后别的请求做前缀命中。
  - 注意 `min(..., request.num_tokens)`：**只缓存「已确定」的 token**，投机解码里未验证的草稿 token 不缓存（可能被拒绝）。
- 返回分到的块列表给调度器。

---

## 5. 前缀缓存的钥匙：块哈希 `hash_block_tokens`

文件：`vllm/v1/core/kv_cache_utils.py`（第 576 行）

前缀缓存怎么判断「两个请求的前缀相同」？靠**给每个满块算一个哈希**。相同内容 → 相同哈希 → 命中复用。

```python
def hash_block_tokens(
    hash_function: Callable[[Any], bytes],
    parent_block_hash: BlockHash | None,
    curr_block_token_ids: Sequence[int],
    extra_keys: tuple[Any, ...] | None = None,
) -> BlockHash:
    if not parent_block_hash:
        parent_block_hash = NONE_HASH

    curr_block_token_ids_tuple = tuple(curr_block_token_ids)
    return BlockHash(
        hash_function((parent_block_hash, curr_block_token_ids_tuple, extra_keys))
    )
```

【逐行 + 逻辑 —— 这里有个精妙设计】

- `hash_function: Callable[[Any], bytes]`
  - 【Python】`Callable[[Any], bytes]` 是类型注解，表示「这个参数是一个函数：接收任意类型、返回 bytes」。**函数可以当参数传递**是 Python 的重要特性（一等函数）。这里把「用哪种哈希算法」做成可插拔的参数。
- `parent_block_hash`：**父块（前一个块）的哈希**。
  - 【逻辑 —— 关键设计】哈希不只算当前块的 token，还**把前一个块的哈希也拌进去**。这样哈希就形成一条链（类似区块链）：
    ```
    块0哈希 = hash(None, 块0的token)
    块1哈希 = hash(块0哈希, 块1的token)
    块2哈希 = hash(块1哈希, 块2的token)
    ```
  - 【为什么要这样】保证「命中」意味着**从头到这里的整个前缀都相同**，而不仅仅是这一块内容碰巧相同。因为 attention 里每个 token 依赖它前面所有 token，只有完整前缀一致，缓存的 KV 才真正能复用。
- `curr_block_token_ids_tuple = tuple(curr_block_token_ids)`
  - 【Python】转成元组 `tuple`。因为元组不可变、可哈希（列表可变、不能做哈希键）。
- `extra_keys`：额外区分因素（比如 LoRA id、多模态输入），保证不同上下文不会误命中。

【全链路回顾前缀缓存】
1. 请求进来，对它的 prompt 按块算出一串块哈希（`request.block_hashes`）。
2. `get_computed_blocks`（第 230 行）拿这串哈希去 BlockPool 的哈希表里查 `find_longest_cache_hit`——找出「最长的、已经算过的前缀」。
3. 命中的块直接 `touch`（§3.1）复用，`num_computed_tokens` 一上来就 > 0。
4. 于是这段前缀的 KV 完全不用重算 → 省显存（共享块）+ 省算力（跳过计算）。

这就是为什么在 vLLM 里，相同 system prompt 的大量请求会特别快。

### 5.1 统一示例：A/B 两请求的 block 分配与共享全过程 ★

现在把 [00](00-全局架构与数据流总览.md)/[02](02-调度器-连续批处理.md) 篇的例子在块层面走一遍，看物理块和 `ref_cnt` 怎么变化。
约定：`block_size=16`，SYS=40 token（占满 2 个块 + 半个），A prompt=48，B prompt=50。

**T0 —— A 到达，做 prefill：**
- A 无前缀命中，`allocate_slots(A, 48)` 需 ⌈48/16⌉=3 块 → 取空闲块 `10,11,12`。
- 算完后，**装满的块**（前 3 个都满）登记哈希：

```
物理块状态：
  block 10: tokens[0:16]  (SYS前16)   hash=H0=hash(None, tok[0:16])    ref_cnt=1(A)
  block 11: tokens[16:32] (SYS后半+)  hash=H1=hash(H0,   tok[16:32])   ref_cnt=1(A)
  block 12: tokens[32:48] (含问题)     hash=H2=hash(H1,   tok[32:48])   ref_cnt=1(A)
                                       └─ 父哈希链，见 §5 ─┘
```

**T1 —— B 到达，做前缀命中检查：**
- B 的 block_hashes：前两块的内容与 A **完全相同**（都是 SYS 的前 32 token）→ 算出的 `H0`、`H1` 一样 → 在哈希表里命中 `block 10`、`block 11`。
- 第三块开始，B 是 "...Capital of France?"，与 A 的 "...2+2?" 不同 → `H2` 不匹配，命中到此为止。
- 命中的 2 块被 `touch`：`ref_cnt` 从 1 → 2。

```
命中后物理块状态：
  block 10: ref_cnt=2  ← A、B 共享！(同一份 SYS KV，只存一份)
  block 11: ref_cnt=2  ← A、B 共享！
  block 12: ref_cnt=1  ← A 独有
```

- B 的 `num_computed_tokens` 直接 = 32（跳过前 32 token 计算），`allocate_slots(B, 18)`：
  剩余 50-32=18 token，需 ⌈(32+18)/16⌉ - 2(已复用) = 2 块 → 取空闲块 `13,14`。

```
B 分配后：
  block 13: tokens[32:48] (含"Capital of")  ref_cnt=1(B)
  block 14: tokens[48:50] (只用2格,未满)     ref_cnt=1(B)  ← 未满块，暂不登记哈希
```

**B 的逻辑视图 → 物理块表（block_table）：**
```
B 逻辑序列: [SYS前32 | 问题18...]
              ↓  ↓       ↓   ↓
block_table:  10 11      13  14   ← 前两块指向 A 已算好的物理块（共享）
```

**T2 —— A 生成完毕，`free(A)`（逆序释放）：**
- 从尾到头减 `ref_cnt`：block 12 → 0（回收，加回空闲链尾）；block 11 → 1；block 10 → 1。
- 注意 block 10、11 **不会被回收**，因为 B 还在用（`ref_cnt` 仍为 1）。共享块只有在**最后一个使用者**离开时才真正释放。

```
free(A) 后：
  block 10: ref_cnt=1(B)   仍在用
  block 11: ref_cnt=1(B)   仍在用
  block 12: ref_cnt=0      回收，但保留 H2 哈希 → 未来相同前缀可命中"抢救"
```

**这就是 PagedAttention + Prefix Caching 的完整数据流**：
- `ref_cnt` 是共享与回收的总开关；
- 相同前缀的物理块被多个请求共享，KV 只存一份（省显存）+ 计算只做一次（省算力）；
- 释放不等于清空——回收的块保留哈希，直到被 `evict` 或被新前缀命中「抢救」。

---

## 6. 释放：`free`

请求结束时（[02](02-调度器-连续批处理.md) 的 `update_from_output` 判定结束后），归还它的块：

```python
    def free(self, request: Request) -> None:
        """Free the blocks allocated for the request.
        We free the blocks in reverse order so that the tail blocks are evicted
        first when caching is enabled.
        """
        ...
        self.coordinator.free(request.request_id)
```

【逻辑】

- 归还本质是：把这些块的 `ref_cnt -= 1`。减到 0 就重新加回空闲链表尾部（`append`），可被复用。
- **注意「reverse order（逆序）」**：从序列尾巴往头释放。因为尾部的块（对话后段）最不可能被别的请求当作共享前缀，让它们先沉到 LRU 待逐出端；而头部的块（比如公共 system prompt）更可能被复用，晚点逐出。这是个提升前缀缓存命中率的小心机。

---

## 7. 全流程回顾

```
【请求侧】prompt → 按块算 block_hashes（hash_block_tokens，含父哈希链）
        │
【调度侧】get_computed_blocks: 用哈希查最长命中前缀 → touch 复用（省算力）
        │
        ▼
allocate_slots(请求, 新token数)          KVCacheManager
   1. 算 已算token / 需分配token
   2. get_num_free_blocks 够不够？ ── 不够 → return None → 触发抢占(见02)
   3. allocate_new_blocks ──────────► BlockPool.get_new_blocks
   4. cache_blocks 登记满块哈希              │  popleft_n 从空闲链表头取块(LRU)
                                            │  _maybe_evict 逐出旧内容
                                            │  ref_cnt += 1 标记占用
        │
【结束侧】free: ref_cnt -= 1，减到0则 append 回空闲链表尾（逆序释放）
```

**四个必记要点：**

1. **分页**：KV 切成固定大小 block，逻辑连续物理分散，用块表映射 —— 消灭碎片。
2. **引用计数 `ref_cnt`**：>0 有人用、==0 空闲可复用。是共享与回收的总开关。
3. **前缀缓存**：满块算哈希（带父哈希链），相同前缀直接复用块，省显存又省算力。
4. **LRU 空闲链表**：自造双向链表支持 O(1) 中间删除，头部取（最久未用先复用）、尾部还。

---

## 8. Python 语法速查（本文新增）

| 语法 | 含义 | 例子 |
|------|------|------|
| `@dataclass` | 自动生成 `__init__` 等的类 | `KVCacheBlock` |
| `@property` | 方法伪装成只读属性 | `block.block_hash` |
| `字段: 类型 = 默认值` | dataclass 字段声明 | `ref_cnt: int = 0` |
| `"ClassName"`（带引号的注解） | 前向引用（类内引用自己） | `"KVCacheBlock \| None"` |
| `is` / `is not` | 判断是否同一对象（身份） | `x is None` |
| `range(n)` | 0..n-1 序列 | `for i in range(n):` |
| `list[-1]` | 负下标，末尾元素 | `blocks[-1]` |
| `tuple(x)` | 转元组（不可变、可作哈希键） | `tuple(token_ids)` |
| `Callable[[参数], 返回]` | 函数类型注解（函数当参数传） | `hash_function` |
| `for x in seq:` | 遍历 | `for block in ret:` |
| 哨兵节点 | 假头假尾简化链表边界 | `fake_free_list_head` |

---

## 下一步

- **[04](04-Worker与模型执行.md) · Worker 与模型执行**：`SchedulerOutput`（含每个请求分到的 block 列表）交给 GPU 后，`gpu_model_runner` 如何把变长请求打平成张量、`execute_model` 里 attention kernel 如何用 block table 做分页注意力。这是整条链路的最后一环——真正的 GPU 计算。

我将继续生成 04。
