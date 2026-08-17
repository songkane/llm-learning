# 02 · 数据流贯穿全局（Demo 实例）

> 本篇用**一个具体请求**贯穿整条链路，展示每一步的**数据结构**及其**变换**。这是理解 SGLang 最有效的方式——看清「一段文本」如何一步步变成「回复文本」。
>
> 核心转换链：**文本 → input_ids → `Req` → `ScheduleBatch` → `ForwardBatch` → logits → next token → 文本**
>
> **代码版本**：`sgl-project/sglang` @ **tag `v0.5.16`**（2026-07-24 发布）。详见 [00 篇 · 分析的代码库版本](./00-导读与索引.md)。

---

## 0. Demo 场景设定

假设我们启动了一个单机服务：

```bash
python -m sglang.launch_server --model-path Qwen/Qwen2.5-7B-Instruct --port 30000
```

然后并发发来两个请求（为了演示前缀缓存复用，它们共享同一段 system prompt）：

- **请求 A**：`"你是一个助手。请问 1+1 等于几？"`
- **请求 B**：`"你是一个助手。请问天空为什么是蓝色的？"`

假设 tokenize 后：
- 公共前缀 `"你是一个助手。请问"` → `[101, 102, 103, 104, 105]`（5 个 token）
- A 的独有部分 `"1+1 等于几？"` → `[201, 202, 203]`
- B 的独有部分 `"天空为什么是蓝色的？"` → `[301, 302, 303, 304]`

下面跟着这两个请求走完全程。

> 本 Demo 与 [00 篇统一示例](./00-导读与索引.md) 一致，可对照阅读。

---

## 0.5 全链路时序图（含进程边界）★

先看这张**时序图**，它强调**三个进程之间怎么协作**——这是理解 SGLang 的关键。很多人卡在「为什么请求要跨进程传 token」，看懂这张图就通了。

```
 用户/HTTP     TokenizerManager    [ZMQ]      Scheduler子进程        GPU        Detokenizer子进程
（主进程）     （主进程）          IPC socket  event_loop         ModelRunner  event_loop
    │             │                  │            │                 │              │
    │ POST /generate                 │            │                 │              │
    ├────────────▶│                  │            │                 │              │
    │             │ tokenize 文本→ids │            │                 │              │
    │             ├──token化请求─────▶│            │                 │              │
    │             │ (scheduler_input) ├─recv_reqs─▶│ 构造 Req 入队    │              │
    │             │                  │            │                 │              │
    │             │                  │      ┌─────┤ 循环 #1 (prefill)│              │
    │             │                  │      │ get_next_batch_to_run │              │
    │             │                  │      │  match_prefix(RadixCache)            │
    │             │                  │      │ run_batch ─┼──forward──▶│             │
    │             │                  │      │            │◀──logits───┤             │
    │             │                  │      │ sample     │ A吐"2" B吐"天"           │
    │             │                  │      │ process_batch_result   │              │
    │             │                  │      │  cache_finished/unfinished_req       │
    │             │                  │      └──send_to_detokenizer──(detok_ipc)───▶│
    │             │                  │            │                 │   detokenize │
    │             │◀──send_to_tokenizer(tokenizer_ipc)──────────────────────────────┤
    │◀─SSE "2"────┤ 聚合             │            │ 循环 #2 (decode) │              │
    │             │                  │            │  每请求各+1 token │              │
    │◀─SSE ...────┤                  │            │  ...直到 EOS      │              │
```

要点：
- **左侧主进程**只做「门面 + 翻译」（tokenize / 聚合），不碰 GPU。
- **中间 Scheduler 子进程**独立地一轮轮跑 `event_loop`，通过 ZMQ 与主进程解耦——这样 GPU 计算不被 Python GIL 和 HTTP 层拖累。
- **右侧 Detokenizer 子进程**专职把 token id 增量还原成文本，也不占 GPU 进程资源。
- 三条 ZMQ 通道（`scheduler_input` / `detokenizer` / `tokenizer`）就是「过河的船」——因为要跨进程，只传 token id 这种小数据。

> 对照 vLLM：vLLM 把 detokenize 放在主进程，SGLang 拆成独立子进程，拆分更彻底。但「引擎核心在独立子进程死循环跑 step / event_loop」这个思想两者一致。

---

## 1. 第一站：HTTP 层接收（主进程）

请求命中 `http_server.py` 的 `/generate` 路由（FastAPI），封装成内部请求对象，交给 `TokenizerManager.generate_request()`（`tokenizer_manager.py:624`）。

**数据形态**：此时还是原始 JSON / 文本 + 采样参数。

```json
{ "text": "你是一个助手。请问1+1等于几？",
  "sampling_params": {"temperature": 0.7, "max_new_tokens": 64} }
```

---

## 2. 第二站：Tokenize（主进程）

`TokenizerManager` 调用分词器，把文本变成 token id：

```
请求 A: origin_input_ids = [101,102,103,104,105, 201,202,203]
请求 B: origin_input_ids = [101,102,103,104,105, 301,302,303,304]
```

然后通过 ZMQ（`scheduler_input_ipc`）把「token 化请求」发给 Scheduler 子进程。TokenizerManager 内部登记该请求（`rid → 状态`），挂起等待结果。

**数据变换**：`文本` → `List[int]（token ids）` + 采样参数。传输的只有这些小数据。

---

## 3. 第三站：Scheduler 收请求并构造 `Req`（子进程）

Scheduler 主循环 `event_loop_normal`（`scheduler.py:1520`）中 `recv_requests()` 收到请求，`process_input_requests()` 为每个请求构造一个 **`Req` 对象**（`schedule_batch.py:714`），放入 `waiting_queue`。

### 3.1 `Req` —— 请求对象（核心字段）

| 字段 | 含义 | 请求 A 初始值 |
|------|------|--------------|
| `rid` | 请求唯一 ID | `"req-A"` |
| `origin_input_ids` | 原始输入 prompt 的 token ids | `[101,102,103,104,105,201,202,203]` |
| `output_ids` | 已生成的输出 token（append-only 只追加） | `[]` |
| `fill_ids` | 本轮参与计算的完整 token 序列 = origin + output | `[101,...,203]` |
| `prefix_indices` | 命中共享前缀对应的 KV cache 索引（已缓存部分） | `[]`（首次为空） |
| `req_pool_idx` | 该请求在 `req_to_token_pool` 中的槽位索引 | 分配后得到，如 `0` |
| `sampling_params` | 采样参数 | `temp=0.7, max_new_tokens=64` |
| `finished_reason` | 完成原因（None=未完成） | `None` |
| `stream` | 是否流式 | `True` |
| `extend_range` | 本轮 extend 的 `[start, end)` 范围 | 组批时确定 |

> `Req` 是**一个请求在其整个生命周期内的状态载体**，跨多轮 decode 迭代持续存在，`output_ids` 每步追加一个新 token。

---

## 4. 第四站：组 batch（调度决策核心）

主循环调用 `get_next_batch_to_run()`（`scheduler.py:2687`），决定这一轮跑什么。决策逻辑（**prefill 优先**）：

```
1. 若上一轮是 prefill 批 → 先 filter_batch 过滤已完成请求，merge 进 running_batch  (3000-3003)
2. 尝试 get_new_batch_prefill(running_batch)：从 waiting_queue 里挑请求组 prefill 批  (3018)
3. 决策分支 (3035-3044):
     if 有新 prefill 批:  跑 prefill
     elif running_batch 非空:  update_running_batch → 跑 decode
     else:  None（空闲）
4. return NextBatchPlan(batch_to_run=..., running_batch=...)  (3061)
```

### 4.1 前缀缓存在这里介入

`get_new_batch_prefill` 内部通过 `PrefillAdder`（`schedule_policy.py:441`）挑请求。对每个请求，先查 **RadixCache**：

```
RadixCache.match_prefix(A的token序列)   # radix_cache.py:355
```

- **请求 A 先到**：树是空的，前缀命中长度 = 0，需要完整 prefill 8 个 token。prefill 完成后，A 的前缀 `[101..105]` 会被 `insert` 进基数树。
- **请求 B 后到**：`match_prefix` 命中公共前缀 `[101,102,103,104,105]`（长度 5）！于是 B 的 `prefix_indices` 填上这 5 个 token 已有的 KV 槽位索引，**B 只需 prefill 独有的 `[301,302,303,304]` 4 个 token**，省掉 5 个 token 的重复计算。

> 这就是 RadixAttention 的价值：请求 B 复用了请求 A 算好的前缀 KV。

### 4.2 `ScheduleBatch` —— 批对象

被选中的请求组成一个 **`ScheduleBatch`**（`schedule_batch.py`）。核心字段：

| 字段 | 含义 |
|------|------|
| `reqs` | 本批包含的 `Req` 列表 |
| `req_pool_indices` | 各请求的池槽位索引（GPU 张量） |
| `seq_lens` | 各请求当前序列长度（GPU 张量） |
| `out_cache_loc` | 本轮新 token 要写入的 KV cache 槽位 |
| `forward_mode` | 本批的前向模式（EXTEND / DECODE / MIXED / IDLE） |
| `input_ids` | 本批要计算的 token ids（拼接后） |

组 prefill 批时调 `prepare_for_extend()`，组 decode 批时调 `prepare_for_decode()`。这两个方法负责：
- 从 `ReqToTokenPool` / KV allocator 分配槽位；
- 把 CPU 上的 Python 列表整理成 GPU 张量。

**请求 A 的 prefill 批（EXTEND 模式）数据形态**：

```
forward_mode   = EXTEND
input_ids      = [101,102,103,104,105,201,202,203]   # 需要计算的 8 个 token
seq_lens       = [8]
out_cache_loc  = [槽0,槽1,...,槽7]                     # 分配 8 个 KV 槽
```

---

## 5. 第五站：`ScheduleBatch → ForwardBatch`（进入 GPU）

`run_batch()` → `TpModelWorker.forward_batch_generation()`（`tp_worker.py:529`）→ `ModelRunner.forward()`（`model_runner.py:1232`）。

在这里，`ScheduleBatch` 被转换成 **`ForwardBatch`**。源码文件头注释（`forward_batch_info.py:17-25`）中译：

> **一个 batch 的数据结构流转如下：`ScheduleBatch → ForwardBatch`**
> - `ScheduleBatch` 由 `scheduler.py::Scheduler` 管理，包含高层调度数据，**大部分在 CPU 上**。
> - `ForwardBatch` 由 `model_runner.py::ModelRunner` 管理，包含底层张量数据，**大部分是 GPU 张量**，由 `ForwardBatch.init_new` 直接从 `ScheduleBatch` 构造。

这句话点明了两个数据结构的分工：**`ScheduleBatch` 面向调度（CPU 决策），`ForwardBatch` 面向计算（GPU 张量）**。

### 5.1 `ForwardMode` 枚举（`forward_batch_info.py:98`，含中译）

```python
class ForwardMode(IntEnum):
    EXTEND = auto()   # 扩展一段序列，序列开头部分的 KV cache 已算好（如 system prompt）。通常即"prefill"。
    DECODE = auto()   # 解码一个 token。
    MIXED  = auto()   # 做 chunked prefill 时，同时包含 EXTEND 与 DECODE。
    IDLE   = auto()   # 没有序列要前向。DP attention 下，某些 worker 无请求时会 IDLE。
    TARGET_VERIFY  = auto()  # 投机解码：在 target 模型里验证一个 batch。
    DRAFT_EXTEND_V2 = auto() # 投机解码：在 draft 模型里扩展一个 batch。
    PREBUILT = auto()        # PD 分离的 decode worker：一批请求的 KV cache 已就绪，可直接开始 decode。
    SPLIT_PREFILL = auto()   # PD multiplexing 的拆分 prefill。
    DLLM_EXTEND = auto()     # 用于 dLLM。
```

> 教学要点：`EXTEND`/`DECODE` 是最基础的两种；`MIXED` 出现在 chunked prefill；`IDLE` 用于 DP attention 保持各 rank 同步；后面几个用于投机解码与 PD 分离。判断用 `is_extend()` / `is_decode()` 等方法。

### 5.2 `ForwardBatch` 核心字段

| 字段 | 含义 |
|------|------|
| `forward_mode` | 前向模式（同上枚举） |
| `input_ids` | 输入 token 张量（GPU） |
| `positions` | 位置编码索引 |
| `seq_lens` | 各请求序列长度 |
| `out_cache_loc` | 新 token 写入的 KV 槽位 |
| `req_to_token_pool` / `token_to_kv_pool` | KV 池引用 |
| `attn_backend` | 本次前向使用的 attention 后端 |

---

## 6. 第六站：GPU 前向 + 采样

`ModelRunner.forward()` 根据 `forward_mode` 调用模型，Attention 层通过 `AttentionBackend`：

- prefill → `forward_extend()`（`base_attn_backend.py:225`）
- decode → `forward_decode()`（`base_attn_backend.py:212`）

前向过程读写 KV Cache（`MHATokenToKVPool` 或 MLA 版），最后输出 **logits**（对词表每个 token 的打分）。

然后 `ModelRunner.sample()` 按采样参数（temperature/top_p 等）从 logits 采样出 **next_token_ids**。

**请求 A 的数据变换**：
```
ForwardBatch(input_ids=[101..203]) 
   → 模型前向 → logits [1, vocab_size]
   → sample() → next_token = 501  (假设是 "2" 对应的 token)
```

---

## 7. 第七站：处理结果 + 写回缓存（子进程）

`process_batch_result()`（`scheduler.py`）：
1. 把 `next_token=501` 追加到请求 A 的 `output_ids` → `output_ids=[501]`；
2. 判断是否结束（是否命中 EOS / 达到 max_new_tokens）；
3. 若请求完成，调 `RadixCache.cache_finished_req()`（`radix_cache.py:437`）把完整序列的 KV 写回基数树（供后续请求复用）；未完成则 `cache_unfinished_req()`；
4. 通过 ZMQ（`detokenizer_ipc`）把新 token 发给 DetokenizerManager。

`cache_finished_req` 关键逻辑（`radix_cache.py:455-471`，含中译注释）：

```python
# 完整 token 序列 = 原始输入 + 输出
token_ids = (req.origin_input_ids + req.output_ids)[:kv_len_to_handle]
# 取出这些 token 对应的 KV cache 索引
kv_indices = self.req_to_token_pool.req_to_token[req.req_pool_idx, :len(token_ids)]
...
# Radix Cache 在内存池里持有一个引用（把这段前缀 KV 插入基数树，供后续复用）
result = self.insert(InsertParams(key=radix_key, value=values, ...))
```

---

## 8. 第八站：Decode 循环（每步一个 token）

请求 A 的 prefill 完成后进入 `running_batch`。此后每一轮主循环：

```
get_next_batch_to_run → 无新 prefill → update_running_batch → prepare_for_decode
```

`prepare_for_decode` 为每个运行中请求分配 **1 个新 KV 槽**，构造 DECODE 模式的批：

```
forward_mode  = DECODE
input_ids     = [501]          # 只输入上一步生成的 token
seq_lens      = [9]            # 序列长度 +1
out_cache_loc = [槽8]          # 只需 1 个新槽
```

前向 → 采样 → 得到下一个 token → 追加到 `output_ids`。如此循环：

```
迭代1: output_ids=[501]        ("2")
迭代2: output_ids=[501,502]    ("2。")
迭代3: output_ids=[501,502,EOS] → finished!
```

> 对比 prefill：decode 每步只算 1 个新 token，但要读取**全部历史 KV**，所以是访存密集型。batch 里请求越多，一次前向摊薄的开销越低，吞吐越高——这正是连续批处理要尽量把多个请求凑在一起 decode 的原因。

---

## 9. 第九站：Detokenize + 返回（回程）

`DetokenizerManager.event_loop()`（`detokenizer_manager.py:166`）：
- 收到 token id，做**增量 detokenize**（处理 UTF-8 多字节字符可能跨 token 的边界问题）；
- `501 → "2"`，`502 → "。"`；
- 通过 ZMQ（`tokenizer_ipc`）把文本回传主进程。

主进程 `TokenizerManager._handle_batch_output()`（`tokenizer_manager.py:1899`）聚合结果，通过 SSE **流式**吐给客户端：

```
data: {"text": "2"}
data: {"text": "2。"}
data: {"text": "2。", "finished": true}
```

---

## 10. 全链路数据变换总表

| 阶段 | 位置 | 输入 | 输出 | 关键结构 |
|------|------|------|------|----------|
| 接收 | http_server | HTTP JSON | 内部请求 | dict |
| Tokenize | TokenizerManager | 文本 | token ids | `List[int]` |
| 构造请求 | Scheduler | token ids | 请求对象 | `Req` |
| 组批+缓存匹配 | Scheduler / RadixCache | `Req` 列表 | 批对象 | `ScheduleBatch`（含 `prefix_indices`） |
| 转 GPU 批 | ModelRunner | `ScheduleBatch`(CPU) | `ForwardBatch`(GPU) | `ForwardBatch` + `ForwardMode` |
| 前向+采样 | ModelRunner | `ForwardBatch` | next token | logits → token id |
| 处理结果 | Scheduler / RadixCache | token id | 追加 output + 写回缓存 | `Req.output_ids` 更新 |
| Detokenize | DetokenizerManager | token id | 文本片段 | `str` |
| 返回 | TokenizerManager | 文本片段 | SSE 流 | HTTP response |

---

## 11. 一图总览（带数据结构）

```
"你是一个助手。请问1+1等于几？"
        │ tokenize
        ▼
[101,102,103,104,105,201,202,203]           ← List[int]
        │ ZMQ → Scheduler
        ▼
Req(origin_input_ids=[...], output_ids=[], prefix_indices=[...])   ← Req
        │ get_next_batch_to_run + RadixCache.match_prefix
        ▼
ScheduleBatch(reqs=[A], forward_mode=EXTEND, out_cache_loc=[...])   ← CPU 调度对象
        │ ForwardBatch.init_new
        ▼
ForwardBatch(input_ids=<GPU tensor>, forward_mode=EXTEND, ...)      ← GPU 张量对象
        │ ModelRunner.forward → logits → sample
        ▼
next_token = 501
        │ 追加 output_ids；cache_finished/unfinished_req 写回基数树
        │ ZMQ → Detokenizer
        ▼
"2"  ← 增量 detokenize
        │ ZMQ → TokenizerManager → SSE
        ▼
返回给用户: "2。"
```

---

## 12. 小结与下一步

本篇用一个双请求 Demo 展示了：
- 数据如何在 `文本 → input_ids → Req → ScheduleBatch → ForwardBatch → logits → token → 文本` 之间变换；
- 请求 B 如何通过 RadixCache **复用**请求 A 的前缀 KV；
- prefill 与 decode 两阶段在数据形态上的差异。

> 下一篇 [03-核心模块源码深度分析](./03-核心模块源码深度分析.md) 将对 Scheduler 决策、RadixCache 算法、ModelRunner 前向、Attention 后端做**逐段源码级**剖析，并翻译核心英文注释。
