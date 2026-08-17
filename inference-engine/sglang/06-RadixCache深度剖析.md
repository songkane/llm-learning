# 06 · RadixCache 深度剖析 —— SGLang 的招牌技术

> 这是 SGLang 最核心的创新，也是它名字里 "SG"（Structured Generation）能高效落地的关键。本篇对标 vLLM 的 PagedAttention 篇，用**真实源码 + 逐行注释**讲透「基数树前缀缓存」。
>
> 注释分两类：`【逻辑】`讲 SGLang 在做什么、为什么；`【Python】`讲 Python 语法（面向不熟悉 Python 的读者）。
>
> 对应源码：`python/sglang/srt/mem_cache/radix_cache.py`、`memory_pool.py`。沿用 00 篇统一示例（A/B 两请求，共享前缀 `[101,102,103,104,105]`）。
>
> **代码版本**：`sgl-project/sglang` @ **tag `v0.5.16`**（2026-07-24 发布）。详见 [00 篇 · 分析的代码库版本](./00-导读与索引.md)。

---

## 0. 为什么需要 RadixCache —— 先理解 KV Cache 的浪费

### 0.1 KV Cache 从哪来

Transformer 注意力机制里，每个 token 都要和它前面所有 token 计算。为此每个历史 token 会算出一对张量 **Key（K）和 Value（V）**。自回归生成时，如果每生成一个新 token 都把前面所有 token 的 K/V 重算一遍，计算量是平方级的、极其浪费。**KV Cache 就是把算过的 K/V 存起来复用**。

### 0.2 一个被忽视的浪费：请求间的重复前缀

单个请求内部复用 KV 是基本操作。但还有一层更大的浪费：**不同请求之间往往有相同的前缀**。

- 聊天场景：成千上万个请求共享同一段 system prompt。
- 多轮对话：第 N 轮的输入包含了前 N-1 轮的全部历史。
- 多分支生成（如 beam search、self-consistency）：多个分支共享同一段起始 prompt。

如果每个请求都从头 prefill 这段公共前缀，就是在**重复计算相同的东西**。

### 0.3 RadixAttention 的解法：把前缀存进一棵基数树

SGLang 的做法：用一棵 **基数树（Radix Tree）** 把所有请求的 token 序列按公共前缀合并存储。相同前缀只占树上的一条路径，其 KV 只算一次、只存一份，后续请求命中即复用。

```
一棵基数树（我们的统一示例）：

           root
            │
            │ key=[101,102,103,104,105]  ("你是一个助手。请问")
            │ value=[KV槽0..4]  ← A、B 共享这段 KV！
         ┌──┴──────────────┐
         │                 │
 key=[201,202,203]   key=[301,302,303,304]
 ("1+1等于几？")       ("天空为什么是蓝色的？")
 value=[KV槽5..7]      value=[KV槽8..11]
 (A 独有)              (B 独有)
```

- 请求 A 走「root → 左分支」，请求 B 走「root → 右分支」，**公共父节点的 KV 被两者共享**。
- 这与 vLLM 的区别：vLLM 用「定长 block + 哈希表」找前缀（03 篇讲的 PagedAttention），SGLang 用「基数树最长公共前缀路径」。树结构天然表达前缀的层级共享。

三个核心角色：
- **`TreeNode`**：树节点，保存一段 token（`key`）及其 KV 索引（`value`），带引用计数 `lock_ref`。
- **`RadixCache`**：管理整棵树，提供 `match_prefix` / `insert` / `evict` / `inc_lock_ref` 等。
- **`ReqToTokenPool` + `TokenToKVPool`**：两级映射，`value` 里存的就是 KV 池里的物理槽位索引。

---

## 1. 一个树节点长什么样：`TreeNode`

文件：`radix_cache.py`（第 217 行）

```python
class TreeNode:

    counter = 0

    def __init__(self, id: Optional[int] = None, priority: int = 0):
        self.children = defaultdict(TreeNode)
        self.parent: TreeNode = None
        self.key: RadixKey = None
        self.value: Optional[torch.Tensor] = None
        self.lock_ref = 0
        self.last_access_time = time.monotonic()
        self.creation_time = time.monotonic()
        ...
        self.priority = priority
        self.id = TreeNode.counter if id is None else id
        TreeNode.counter += 1
```

【逐字段精读 —— 这是理解全篇的地基】

- `counter = 0`
  - 【Python】这是**类变量**（写在 `class` 下、`__init__` 外），所有实例共享同一个。用来给每个节点分配唯一自增 id。
- `def __init__(self, id=None, priority=0):`
  - 【Python】`__init__` 是**构造函数**，创建对象时自动调用，相当于其他语言的 constructor。第一个参数 `self` 永远是「对象自己」。
- `self.children = defaultdict(TreeNode)`
  - 【逻辑】孩子节点字典，键是「子节点 key 的首段」，值是子 `TreeNode`。
  - 【Python】`defaultdict(TreeNode)` 是「带默认值的字典」：访问一个不存在的键时，自动用 `TreeNode()` 创建一个默认值，不会报 KeyError。省去「先判断键在不在」的样板代码。
- `self.key: RadixKey = None`
  - 【逻辑】这个节点代表的 token 段（如 `[101,102,103,104,105]`）。
  - 【Python】`: RadixKey` 是类型注解，仅提示不强制。
- `self.value: Optional[torch.Tensor] = None`
  - 【逻辑】**这段 token 对应的 KV cache 物理槽位索引**（一个 int64 张量）。这是「能复用的东西」。
  - 【Python】`Optional[torch.Tensor]` = `torch.Tensor | None`，表示可以是张量也可以是空。
- `self.lock_ref = 0`
  - 【逻辑】**引用计数**——多少个「正在运行的请求」正在使用这个节点。**这是防止「正在用的缓存被逐出」的总开关**（对应 vLLM 的 `ref_cnt`）。
    - `lock_ref == 0` → 没有运行中的请求在用，可被 LRU 逐出。
    - `lock_ref > 0` → 有请求在用，**绝不能逐出**。
- `self.last_access_time = time.monotonic()`
  - 【逻辑】最后访问时间，供 LRU 逐出策略判断「谁最久没用」。
  - 【Python】`time.monotonic()` 是「单调时钟」，只增不减，不受系统时间调整影响，适合测时间间隔。
- `self.priority = priority`
  - 【逻辑】优先级，供「优先级感知逐出」策略使用。

### 1.1 `@property` —— 把方法伪装成属性

```python
    @property
    def evicted(self):
        return self.value is None
```

【Python】`@property` 装饰器让你**用访问属性的语法调用方法**：写 `node.evicted`（没有括号）就会执行这个方法。
【逻辑】`value is None` 表示这个节点的 KV 已被逐出（只剩树结构骨架，KV 槽已释放）。

### 1.2 `__lt__` —— 让节点可比较（LRU 排序的基础）

```python
    def __lt__(self, other: TreeNode):
        return self.last_access_time < other.last_access_time
```

【Python】`__lt__` 是「小于运算符 `<`」的魔术方法。定义了它，两个 `TreeNode` 就能用 `<` 比较，也能被 `heapq`（堆）排序。
【逻辑】比较规则是「最后访问时间早的算小」——所以放进小顶堆后，**堆顶永远是最久未用的节点**，正好是 LRU 要优先逐出的对象。

---

## 2. 匹配前缀：`match_prefix` 与 `_match_prefix_helper`

这是 RadixCache 最核心的操作：**给一个 token 序列，在树上找出最长的、已缓存的前缀**。

### 2.1 入口 `match_prefix`（第 355 行）

```python
    def match_prefix(self, params: MatchPrefixParams) -> MatchResult:
        key = params.key
        key, _ = key.maybe_to_bigram_view(self.is_eagle)   # EAGLE 投机解码用 bigram
        if self.disable or len(key) == 0:
            return self._empty_match_result
        key = key.page_aligned(self.page_size)             # 按 page 对齐
        if len(key) == 0:
            return self._empty_match_result
        value, last_node = self._match_prefix_helper(self.root_node, key)  # 树上匹配
        if value:
            value = torch.cat(value)                       # 拼接各段命中的 KV 索引
        else:
            value = self._empty_match_result.device_indices
        return MatchResult(
            device_indices=value,          # 命中前缀的 KV cache 索引（可直接复用！）
            last_device_node=last_node,
            last_host_node=last_node,
            best_match_node=last_node,
        )
```

【逐行】
- `key.page_aligned(self.page_size)`
  - 【逻辑】按 `page_size` 对齐——若 `page_size>1`，长度会向下截断到整数倍（半页不参与匹配）。默认 `page_size=1` 时无影响。
- `value, last_node = self._match_prefix_helper(self.root_node, key)`
  - 【逻辑】从根节点开始，真正在树上走。返回「命中的 KV 索引段列表」和「命中到的最后一个节点」。
  - 【Python】一次接两个返回值，是「元组解包」。
- `value = torch.cat(value)`
  - 【逻辑】把沿路命中的多段 KV 索引拼成一个连续张量。
  - 【Python】`torch.cat` 是 PyTorch 的张量拼接。

### 2.2 核心算法 `_match_prefix_helper`（第 650 行）★

这是全篇最该精读的算法。它沿着树逐段匹配：

```python
    def _match_prefix_helper(self, node: TreeNode, key: RadixKey):
        access_time = time.monotonic()
        node.last_access_time = access_time

        child_key = key.child_key(self.page_size)   # 取 key 的「首段」作为找孩子的索引

        value = []
        while len(key) > 0 and child_key in node.children.keys():
            child = node.children[child_key]
            child.last_access_time = access_time
            prefix_len = child.key.match(key, page_size=self.page_size)  # 这个孩子能匹配多长
            if prefix_len < len(child.key):
                # 只匹配了孩子 key 的一部分 → 必须从中间「劈开」这个节点
                new_node = self._split_node(child.key, child, prefix_len)
                value.append(new_node.value)
                node = new_node
                break
            else:
                # 完整匹配整个孩子 key → 继续往下一层走
                value.append(child.value)
                node = child
                key = key[prefix_len:]           # 削掉已匹配的部分
                if len(key):
                    child_key = key.child_key(self.page_size)

        return value, node
```

【逐行精读】

- `child_key = key.child_key(self.page_size)`
  - 【逻辑】取待匹配 key 的「首段」（第一个 page），用它作为在 `node.children` 字典里查找孩子的索引。
- `while len(key) > 0 and child_key in node.children.keys():`
  - 【逻辑】只要还有 key 没匹配完，且当前节点有对应首段的孩子，就继续往下走。
  - 【Python】`x in dict.keys()` 判断键是否存在；`and` 逻辑与，任一条件为假就停。
- `prefix_len = child.key.match(key, page_size=...)`
  - 【逻辑】计算「这个孩子的 key」和「待匹配 key」到底能匹配多少个 token。三种情况：
- **情况一（部分匹配）**：`if prefix_len < len(child.key):`
  - 【逻辑】待匹配 key 只覆盖了孩子 key 的前一段。比如孩子存的是 `[101,102,103,104,105]`，但新请求只共享 `[101,102,103]`——必须把这个节点从第 3 个位置**劈成两半**（`_split_node`），前半段 `[101,102,103]` 成为可共享的父节点。匹配到此为止，`break`。
- **情况二（完整匹配）**：`else:`
  - 【逻辑】整个孩子 key 都被匹配上了，把它的 KV 收进 `value`，`node` 下移到这个孩子，`key` 削掉已匹配部分（`key = key[prefix_len:]`），继续下一轮循环往更深处走。
  - 【Python】`key[prefix_len:]` 是「切片」，取从 `prefix_len` 到末尾的部分。

【一句话总结】`_match_prefix_helper` = **沿树逐段贪心匹配最长公共前缀；若某段只匹配一半，就劈开节点以精确暴露共享边界。**

### 2.3 节点分裂：`_split_node`（第 676 行）

为什么要分裂？因为基数树的节点存的是「变长 token 段」，当新请求只共享其中一部分时，必须把「共享部分」独立成一个节点，才能被两个请求各自的分支复用。

```python
    def _split_node(self, key: RadixKey, child: TreeNode, split_len: int):
        # new_node -> child （new_node 成为 child 的新父节点）
        new_node = TreeNode(priority=child.priority)
        new_node.hit_count = child.hit_count
        new_node.children = {key[split_len:].child_key(self.page_size): child}
        new_node.parent = child.parent
        new_node.lock_ref = child.lock_ref            # 继承引用计数
        new_node.key = child.key[:split_len]          # 前半段（共享部分）
        new_node.value = child.value[:split_len].clone()
        child.parent = new_node                       # 原节点变成新节点的孩子
        child.key = child.key[split_len:]             # 后半段（独有部分）
        child.value = child.value[split_len:].clone()
        new_node.parent.children[key.child_key(self.page_size)] = new_node
        ...
        return new_node
```

【逻辑图解】以「孩子存 `[101,102,103,104,105]`，新 key 只共享前 3 个」为例：

```
分裂前：              分裂后：
  parent               parent
    │                    │
    │ [101..105]         │ [101,102,103]  ← new_node（共享部分，可被多分支复用）
  child                new_node
                         │
                         │ [104,105]      ← child（原节点，只剩后半段）
                       child
```

- `new_node.key = child.key[:split_len]`：新父节点拿前半段（共享）。
- `child.key = child.key[split_len:]`：原节点缩短成后半段（独有）。
- `new_node.value = child.value[:split_len].clone()`：KV 索引也对应切分。
  - 【Python】`.clone()` 复制张量，避免两个节点共享同一份底层存储导致互相影响。

> 分裂只改变**树结构**，不复制 KV 数据本身（KV 仍在池里原地），是纯 O(1) 级别的指针操作，代价极小。

---

## 3. 写回缓存：`insert` 与 `cache_finished_req`

请求算完后，要把它的 KV 挂到树上，成为后续请求可复用的「公共资产」。

### 3.1 `cache_finished_req`（第 437 行）

```python
        # 完整 token 序列 = 原始输入 + 输出
        token_ids = (req.origin_input_ids + req.output_ids)[:kv_len_to_handle]
        kv_indices = self.req_to_token_pool.req_to_token[req.req_pool_idx, :len(token_ids)]
        radix_key = RadixKey(token_ids, req.extra_key, is_bigram=self.is_eagle).page_aligned(self.page_size)
        key_len = len(radix_key)
        values = kv_indices[:key_len].to(dtype=torch.int64, copy=True)
        # Radix Cache takes one ref in memory pool
        if is_insert:
            priority = getattr(req, "priority", 0) or 0
            result = self.insert(InsertParams(key=radix_key, value=values, priority=priority))
            freed_end = result.prefix_len
```

【逐行】
- `token_ids = (req.origin_input_ids + req.output_ids)[:kv_len_to_handle]`
  - 【逻辑】请求的完整序列 = 原始输入 + 已生成输出。
  - 【Python】两个 list 用 `+` 直接拼接；`[:n]` 切片取前 n 个。
- `kv_indices = self.req_to_token_pool.req_to_token[req.req_pool_idx, :len(token_ids)]`
  - 【逻辑】通过 `ReqToTokenPool` 查出这些 token 对应的 KV 物理槽位。这就是要挂到树上的 `value`。
- `getattr(req, "priority", 0) or 0`
  - 【Python】`getattr(obj, "attr", default)` 安全取属性，没有就返回默认值。`x or 0` 保证 None 时取 0。
- `result = self.insert(...)`：把 `(radix_key, values)` 插进基数树。

### 3.2 `_insert_helper`（第 706 行）

插入逻辑和匹配对称——沿树走，能复用的路径就复用，走到没有的地方新建节点：

```python
        while len(key) > 0 and child_key in node.children.keys():
            node = node.children[child_key]
            prefix_len = node.key.match(key, page_size=self.page_size)
            total_prefix_length += prefix_len
            key = key[prefix_len:]
            value = value[prefix_len:]
            if prefix_len < len(node.key):
                new_node = self._split_node(node.key, node, prefix_len)   # 部分匹配→分裂
                node = new_node
            ...
        if len(key):
            new_node = TreeNode(priority=priority)      # 剩余部分→挂新节点
            new_node.parent = node
            new_node.key = key
            new_node.value = value.clone()
            node.children[child_key] = new_node
            self.evictable_size_ += len(key)            # 新增的这段成为「可逐出」
            ...
```

【逻辑】
- 已存在的前缀路径直接复用（`total_prefix_length` 累加），不重复存。
- 走到树上没有的地方，`TreeNode(...)` 新建节点挂上去。
- `self.evictable_size_ += len(key)`：新挂的这段 KV 目前没有请求锁定，计入「可逐出大小」，将来显存紧张时可被 LRU 回收。

---

## 4. 引用计数：`inc_lock_ref` / `dec_lock_ref`（灵魂机制）★

这是保证「正在用的缓存不被误删」的核心，对应 vLLM 的 `ref_cnt`。

### 4.1 `inc_lock_ref`（第 594 行）—— 请求开始用某前缀时加锁

```python
    def inc_lock_ref(self, node: TreeNode):
        ...
        delta = 0
        while node != self.root_node:
            if node.lock_ref == 0:
                self.evictable_size_ -= len(node.key)     # 从「可逐出」转为「受保护」
                self.protected_size_ += len(node.key)
                delta -= len(node.key)
            node.lock_ref += 1                            # 引用计数 +1
            self._update_leaf_status(node)
            node = node.parent                            # 一路向上锁到根
        return IncLockRefResult(delta=delta)
```

【逐行】
- `while node != self.root_node:`
  - 【逻辑】**从命中的节点一路向上锁到根**。为什么向上？因为要用一个节点，它的所有祖先前缀也必须保住（它们是这条前缀路径的一部分）。
- `if node.lock_ref == 0:`
  - 【逻辑】如果这个节点原本是「可逐出」状态（没人锁），现在第一次被锁：把它的大小从 `evictable_size_`（可逐出）转到 `protected_size_`（受保护）。
- `node.lock_ref += 1`：引用计数自增。多个请求共享同一前缀时，`lock_ref` 会累加（如 A、B 都用公共前缀，则该节点 `lock_ref=2`）。
- `node = node.parent`：上移到父节点，继续锁。

### 4.2 `dec_lock_ref`（第 609 行）—— 请求用完后解锁

```python
    def dec_lock_ref(self, node: TreeNode, params=None):
        ...
        while node != self.root_node:
            if node.lock_ref == 1:                        # 最后一个使用者要走了
                self.evictable_size_ += len(node.key)     # 从「受保护」转回「可逐出」
                self.protected_size_ -= len(node.key)
                delta += len(node.key)
            node.lock_ref -= 1                            # 引用计数 -1
            self._update_leaf_status(node)
            node = node.parent
        return DecLockRefResult(delta=delta)
```

【逻辑】`dec` 是 `inc` 的镜像：`lock_ref` 从 1 减到 0 时，该节点重新变为「可逐出」。**共享前缀只有在最后一个使用者离开时才真正变得可逐出**——这正是共享安全的保证。

> 对照统一示例：A、B 都在跑时，公共前缀节点 `lock_ref=2`，受保护；A 结束 `dec` 到 1（B 还在用，仍受保护）；B 也结束 `dec` 到 0，此时才可被 LRU 逐出。

---

## 5. 逐出：`evict`（第 565 行）—— LRU 回收显存

显存不足时，逐出最久未用的、且没被锁的叶子节点：

```python
    def evict(self, params: EvictParams) -> EvictResult:
        ...
        num_tokens = params.num_tokens
        leaves = list(self.evictable_leaves)              # 所有「可逐出」的叶子
        eviction_heap = [
            (self.eviction_strategy.get_priority(node), node) for node in leaves
        ]
        heapq.heapify(eviction_heap)                      # 建小顶堆

        num_evicted = 0
        while num_evicted < num_tokens and len(eviction_heap):
            _priority, x = heapq.heappop(eviction_heap)   # 取最该逐出的节点
            self.token_to_kv_pool_allocator.free_segment(x.value, start_pos=0)  # 释放其 KV 槽
            num_evicted += len(x.value)
            self._delete_leaf(x)                          # 从树上删除
            # 若父节点因此变成叶子且未被锁，也加入候选（继续向上逐出）
            if len(x.parent.children) == 0 and x.parent.lock_ref == 0:
                new_priority = self.eviction_strategy.get_priority(x.parent)
                heapq.heappush(eviction_heap, (new_priority, x.parent))
            ...
        return EvictResult(num_tokens_evicted=num_evicted)
```

【逐行】
- `eviction_heap = [(priority, node) for node in leaves]`
  - 【Python】这是「列表推导式」：对 `leaves` 里每个 node 生成一个 `(优先级, node)` 元组，收集成列表。
- `heapq.heapify(...)`
  - 【Python】`heapq` 是 Python 内置的堆模块，`heapify` 把列表原地变成小顶堆。堆能高效取出「优先级最小」的元素。
- `heapq.heappop(eviction_heap)`
  - 【逻辑】弹出「最该逐出」的节点（LRU 策略下即最久未用的）。
- `self.token_to_kv_pool_allocator.free_segment(x.value, ...)`
  - 【逻辑】**真正释放这个节点占的 KV 显存槽位**——这才是逐出的目的：腾出显存。
- `if len(x.parent.children) == 0 and x.parent.lock_ref == 0:`
  - 【逻辑】删掉一个叶子后，它的父节点可能也变成了没孩子的叶子。若父节点也没被锁，就加入候选继续逐出——**逐出会沿树向上传播**。

> `eviction_strategy` 是可插拔的（`--eviction-policy` 控制），默认是 LRU（按 `last_access_time`），也支持优先级感知等策略。

---

## 6. 统一示例全过程（数据流演示）★

把 00 篇的 A/B 例子在基数树层面走一遍（`page_size=1`）。

**T0 —— A 到达，做 prefill：**
- `match_prefix(A=[101,102,103,104,105,201,202,203])`：树是空的，命中长度 0。
- A 完整 prefill 8 个 token，分到 KV 槽 `[0,1,2,3,4,5,6,7]`。
- 结束时 `cache_finished_req(A)` → `insert`：树上挂出一条路径。

```
root
 └ [101,102,103,104,105,201,202,203]  value=[0..7]  (整条路径, 目前一个节点)
```

**T1 —— B 到达，做前缀匹配：**
- `match_prefix(B=[101,102,103,104,105,301,302,303,304])`：
  - 沿树走，孩子 key `[101..105,201,202,203]` 与 B 匹配到第 5 个 token（`[101,102,103,104,105]`），第 6 个 A 是 201、B 是 301，不匹配。
  - `prefix_len=5 < len(child.key)=8` → **触发 `_split_node`**，把节点从第 5 位劈开：

```
root
 └ [101,102,103,104,105]  value=[0..4]   ← 分裂出的共享父节点
      └ [201,202,203]      value=[5..7]   ← A 的独有后段
```

  - 返回 `device_indices=[0,1,2,3,4]`——**B 直接复用 A 已算好的前 5 个 token 的 KV！**
- B 只需 prefill 独有的 `[301,302,303,304]` 4 个 token（省掉 5 个 token 的重复计算），分到新 KV 槽 `[8,9,10,11]`。
- B 结束时 `insert` → 树变成：

```
root
 └ [101,102,103,104,105]  value=[0..4]   lock_ref 期间=2 (A、B 共享)
      ├ [201,202,203]      value=[5..7]   (A 独有)
      └ [301,302,303,304]  value=[8..11]  (B 独有)
```

**T2 —— A、B 运行期间的锁：**
- A、B 都在跑时，对公共前缀节点各调过 `inc_lock_ref`，该节点 `lock_ref=2`，`protected`，绝不会被逐出。

**T3 —— A 结束：**
- `dec_lock_ref`：公共前缀节点 `lock_ref` 从 2 → 1（B 还在用，仍受保护）；A 的独有节点 `[201,202,203]` `lock_ref` → 0，变为可逐出（但保留在树上，若未来有请求前缀命中还能复用）。

**T4 —— 显存紧张，`evict`：**
- 从「可逐出叶子」里按 LRU 逐出，比如 A 的独有节点 `[201,202,203]` 被弹出，`free_segment([5,6,7])` 释放这 3 个 KV 槽。公共前缀因 B 还在用（`lock_ref=1`）不会被动。

**这就是 RadixAttention 的完整数据流：**
- 基数树表达前缀的层级共享，`_split_node` 精确暴露共享边界；
- `lock_ref` 是共享与回收的总开关（对应 vLLM 的 `ref_cnt`）；
- 相同前缀的 KV 只算一次、只存一份（省算力 + 省显存）；
- 逐出不影响正在使用的前缀，且被逐出前一直留作可复用。

---

## 7. 与 vLLM PagedAttention 的对照（加深理解）

| 维度 | vLLM PagedAttention | SGLang RadixAttention |
|------|---------------------|------------------------|
| 数据结构 | 定长 block（默认16 token）+ 哈希表 | 变长 key 的基数树 |
| 前缀匹配 | block 哈希匹配（带父哈希链） | 树上最长公共前缀路径 + 节点分裂 |
| 共享粒度 | block 粒度（16 的整数倍） | token 粒度（`page_size=1` 时） |
| 防误删 | `ref_cnt` | `lock_ref` + protected/evictable |
| 逐出 | LRU（自造双向链表） | LRU（可插拔策略 + 堆） |
| 天然优势 | 显存零碎片、实现规整 | 多轮/多分支的层级前缀共享更自然 |

> 两者殊途同归——都是「能复用的 KV 就别重算」。区别在于：vLLM 把 KV 切成整齐的页用哈希找，SGLang 用树的结构直接表达前缀关系。对于**深度嵌套的共享前缀**（如多轮对话树、agent 的多分支探索），基数树的表达更直接。

---

## 8. Python 语法速查（本篇）

| 语法 | 含义 | 例子 |
|------|------|------|
| `class C:` 里的类变量 | 所有实例共享 | `counter = 0` |
| `def __init__(self):` | 构造函数 | `TreeNode.__init__` |
| `defaultdict(T)` | 带默认值的字典 | `children = defaultdict(TreeNode)` |
| `@property` | 方法伪装成只读属性 | `node.evicted` |
| `__lt__` | 定义 `<` 运算符 | 供 heapq 排序 |
| `x: T = v` | 带类型注解的赋值 | `lock_ref = 0` |
| `Optional[T]` | `T \| None` | `Optional[torch.Tensor]` |
| `a, b = f()` | 元组解包 | `value, node = helper(...)` |
| `list[start:]` / `[:n]` | 切片 | `key[prefix_len:]` |
| `x in d.keys()` | 键是否存在 | `child_key in node.children.keys()` |
| `[expr for x in seq]` | 列表推导式 | `[(pri, n) for n in leaves]` |
| `heapq.heapify/heappop` | 堆操作（取最小） | LRU 逐出 |
| `getattr(o, "a", d)` | 安全取属性 | `getattr(req, "priority", 0)` |
| `.clone()` | 复制张量 | `child.value[:n].clone()` |
| `time.monotonic()` | 单调时钟 | `last_access_time` |

---

## 9. 必记要点

1. **基数树**：把所有请求的 token 序列按公共前缀合并存树，相同前缀只算一次、只存一份。
2. **`match_prefix` + `_split_node`**：沿树贪心匹配最长公共前缀；部分匹配时劈开节点精确暴露共享边界（纯指针操作，不搬 KV 数据）。
3. **`lock_ref`（引用计数）**：>0 受保护绝不逐出，==0 可逐出；一路锁到根；共享前缀 `lock_ref` 累加，最后一个使用者走才可逐出。
4. **`evict`（LRU）**：显存紧张时按最久未用逐出可逐出叶子，释放 KV 槽，逐出沿树向上传播。
5. **与 vLLM 的区别**：vLLM 定长 block + 哈希，SGLang 变长 key + 基数树；本质都是「能复用的 KV 就别重算」。
6. **可用 `--disable-radix-cache` 关闭**；变体有 `hiradix_cache.py`（分层/CPU offload）、`swa_radix_cache.py`（滑窗）。

---

> 相关篇目：[03-核心模块源码深度分析](./03-核心模块源码深度分析.md)（Scheduler 如何调用 match_prefix 组批）、[02-数据流贯穿全局](./02-数据流贯穿全局-Demo实例.md)（前缀复用在整条链路中的位置）。
