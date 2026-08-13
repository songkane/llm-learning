# 04 · Worker 与模型执行 —— GPU 上到底发生了什么

> 承接 02、03。调度器产出的 `SchedulerOutput`（含每个请求的 token 数、分到的 block）交给 GPU 后，`execute_model` 里发生了什么？本文讲最后一环：变长请求如何打平成张量、模型如何前向、attention kernel 如何用 block table 做分页注意力、采样如何产出 token。
> 注释分两类：`【逻辑】`讲原理；`【Python/PyTorch】`讲语法。
> 对应源码：`vllm/v1/worker/gpu_model_runner.py`、`vllm/v1/worker/gpu_worker.py`、`vllm/v1/attention/backends/flash_attn.py`。

---

## 0. 执行层的三个角色

```
Executor（执行器，vllm/v1/executor/）
   │  管理 1 个或多个 Worker（多卡时每卡一个）
   ▼
GPUWorker（vllm/v1/worker/gpu_worker.py）
   │  代表「一张 GPU 卡」这个角色：管显存、持有模型
   ▼
GPUModelRunner（vllm/v1/worker/gpu_model_runner.py）★本文重点
      真正干活的：把请求打平成张量 → 跑模型 → 采样出 token
```

- **Executor**：对上（EngineCore）提供 `execute_model` 接口，对下把任务分发给 Worker。单卡时就是薄薄一层转发；多卡（张量并行/流水线并行）时负责协调。
- **GPUWorker**：一张卡的「化身」。负责初始化 CUDA、加载模型权重、显存 profiling（算能开多少 KV block，见 03）。
- **GPUModelRunner**：**执行层的心脏**。把「一堆变长请求」变成「GPU 能吃的规整张量」，调用模型，再把输出变回「每个请求的 token」。

回顾 01 的 `EngineCore.step()`：`self.model_executor.execute_model(scheduler_output)` 这一行，最终就落到 `GPUModelRunner.execute_model`。

---

## 1. 核心难题：变长请求怎么喂给 GPU

GPU 擅长「大而规整」的张量运算。但一个 batch 里的请求千奇百怪：
- 请求 A 是新来的，要 prefill 10 个 prompt token。
- 请求 B 在 decode，只要算 1 个 token。
- 请求 C 也在 decode，要算 1 个（但它已经生成了 500 个 token，历史很长）。

vLLM 的做法：**不做 padding（补零对齐），而是把所有请求的 token「首尾相接」拼成一根一维长条**，配上「每个请求从哪到哪」的索引。这叫 **varlen（variable length，变长）** 打包，几乎零浪费。

```
请求:      A(10个prompt token)      B(1)   C(1)
拼接后:  [a0 a1 ... a9              b0     c0]     ← 一维，长度 = 10+1+1 = 12
              │                     │      │
cu_seqlens: [0,                    10,    11,   12]  ← 累积边界（谁从哪开始）
```

这就是下面 `_prepare_inputs` 在做的事。

> 本篇沿用 `00` 篇的统一示例（A/B 两请求，共享 40-token 的 system prompt）。

### 1.1 `execute_model` 内部流程图 ★

调度器的 `SchedulerOutput` 进来后，GPUModelRunner 内部的完整链路：

```
execute_model(scheduler_output)
        │
        ▼
┌─────────────────────────────────────────────────┐
│ ① _prepare_inputs：把变长请求打平成规整张量        │
│     num_scheduled_tokens → np.repeat/cumsum       │
│     产出: input_ids(1维) positions                 │
│           slot_mapping(写KV位置) block_table(读KV) │
│           → 打包成 attn_metadata                   │
└──────────────────┬──────────────────────────────┘
                   ▼
┌─────────────────────────────────────────────────┐
│ ② _model_forward：self.model(...) 触发 forward     │
│     embedding → N层 transformer                   │
│       每层 attention:                             │
│         reshape_and_cache(slot_mapping) 写新KV     │ ← §5.1
│         flash_attn_varlen_func(block_table) 读KV   │ ← §5.2
│     → hidden_states (每个token的高维表示)          │
└──────────────────┬──────────────────────────────┘
                   ▼
┌─────────────────────────────────────────────────┐
│ ③ 取预测位置的 hidden → compute_logits             │
│     hidden_states[logits_indices]                 │
│     (prefill 只取每个请求最后一个token的位置)       │
│     → logits (词表维度的分数)                      │
└──────────────────┬──────────────────────────────┘
                   ▼
┌─────────────────────────────────────────────────┐
│ ④ sample_tokens：按 SamplingParams 挑 token        │
│     → ModelRunnerOutput(sampled_token_ids)        │
│       回传给 02 的 update_from_output 记账          │
└─────────────────────────────────────────────────┘
```

四步对应下面 §2（①）、§4（②③）、§6（④），§3/§5 是①里 block_table/slot_mapping 的展开。

---

## 2. 打平请求：`_prepare_inputs`

文件：`vllm/v1/worker/gpu_model_runner.py`（第 2001 行）

这是执行层的第一步，把 `SchedulerOutput` 转成一批张量。核心片段：

```python
    def _prepare_inputs(
        self,
        scheduler_output: "SchedulerOutput",
    ) -> tuple[PerLayerAttnMetadata, torch.Tensor, ...]:
        total_num_scheduled_tokens = scheduler_output.total_num_scheduled_tokens
        assert total_num_scheduled_tokens > 0
        num_reqs = self.input_batch.num_reqs
        assert num_reqs > 0
        ...
        # Get the number of scheduled tokens for each request.
        req_ids = self.input_batch.req_ids
        tokens = [scheduler_output.num_scheduled_tokens[i] for i in req_ids]
        num_scheduled_tokens = np.array(tokens, dtype=np.int32)
        max_num_scheduled_tokens = max(tokens)
```

【逐行】

- `tokens = [scheduler_output.num_scheduled_tokens[i] for i in req_ids]`
  - 【Python 重点 —— 列表推导式】这是 Python 极高频的语法。等价于：
    ```python
    tokens = []
    for i in req_ids:
        tokens.append(scheduler_output.num_scheduled_tokens[i])
    ```
  - 一行写完「遍历 + 取值 + 收集成列表」。读法：「对 req_ids 里每个 i，取出 num_scheduled_tokens[i]，组成列表」。
  - 【逻辑】收集「本轮每个请求要算多少 token」（就是 02 调度器分配的额度）。
- `num_scheduled_tokens = np.array(tokens, dtype=np.int32)`
  - 【PyTorch/NumPy】`np.array(...)` 把 Python 列表转成 NumPy 数组（高效数值计算）。`dtype=np.int32` 指定为 32 位整数。
- `max_num_scheduled_tokens = max(tokens)` :本轮最长的请求要算多少 token（后面 kernel 要用）。

接着是把 token 位置铺开的关键：

```python
        # Get request indices.
        # E.g., [2, 5, 3] -> [0, 0, 1, 1, 1, 1, 1, 2, 2, 2]
        req_indices = np.repeat(self.arange_np[:num_reqs], num_scheduled_tokens)

        # cu_num_tokens: [2, 5, 3] -> [2, 7, 10]
        # arange: [0, 1, 0, 1, 2, 3, 4, 0, 1, 2]
        cu_num_tokens, arange = self._get_cumsum_and_arange(num_scheduled_tokens)
```

【逻辑 —— 这两行是打平的精髓，注释里给了绝佳例子】

假设 3 个请求，分别要算 `[2, 5, 3]` 个 token：
- `req_indices = [0,0, 1,1,1,1,1, 2,2,2]`
  - 每个位置属于哪个请求。请求 0 占 2 格、请求 1 占 5 格、请求 2 占 3 格。
  - 【NumPy】`np.repeat(值, 次数)`：把每个值重复对应次数。`np.repeat([0,1,2], [2,5,3])` → 上面结果。
- `cu_num_tokens = [2, 7, 10]`（累积和 cumsum）
  - 每个请求的**结束边界**：请求 0 到第 2 个，请求 1 到第 7 个，请求 2 到第 10 个。这就是 §1 说的 `cu_seqlens`。
- `arange = [0,1, 0,1,2,3,4, 0,1,2]`
  - 每个 token 在它自己请求内部的**局部位置**（第几个）。

有了这三个数组，就能把「一维长条」和「每个请求的边界/内部位置」对应起来。后续构造 input_ids、position_ids、slot_mapping 全靠它们做索引。

```python
        # Get token indices, then gather input_ids from the persistent buffer.
        token_indices = (
            positions_np + req_indices * self.input_batch.token_ids_cpu.shape[1]
        )
        ...
        self.input_ids.gather_from_cpu(token_indices_tensor)
```

【逻辑】用上面算出的索引，从「持久化的 token 缓冲区」里**gather（收集）**出这一轮真正要送进模型的 token id，拼成一维的 `input_ids`。这里为性能大量用了预分配缓冲 + 索引操作，而非临时新建张量。

---

## 3. 分页注意力的桥梁：slot_mapping 与 block_table

打平 token 之后，最关键的是告诉 GPU：**每个 token 的 KV 该写到 / 读自哪个物理块**。这就是 03 的 block table 在执行层的落地，靠两个张量：

- **`slot_mapping`（槽位映射）**：一维，长度 = 本轮 token 数。第 i 个元素 = 「第 i 个 token 的 KV 应该写进 KV Cache 的哪个物理槽位（= 块号 × 块大小 + 块内偏移）」。**写 KV 用它**。
- **`block_table`（块表）**：二维 `[请求数, 每请求最大块数]`。记录每个请求用了哪些物理块。**读历史 KV 做 attention 用它**。

这两个张量在 `_prepare_inputs` → `_build_attention_metadata` 里构造，打包进 `attn_metadata`，随张量一起传给模型。

```python
# flash_attn.py 第 262 行，attention metadata 的字段定义
    block_table: torch.Tensor
    slot_mapping: torch.Tensor
```

【逻辑】记住这个映射关系是理解 PagedAttention「逻辑连续、物理分散」如何在 GPU 上实现的关键：**slot_mapping 管写、block_table 管读**，二者把「请求的逻辑 token 序列」翻译成「物理块上的实际位置」。

### 3.1 统一示例：A/B 打平成张量的具体数值 ★

把 02 篇产出的 step #1 `SchedulerOutput`（A 算 48 token、B 算 18 token，B 前缀命中）
喂进 `_prepare_inputs`，看张量长什么样。沿用 03 篇分到的物理块：
A=`[10,11,12]`，B=`[10,11,13,14]`（前两块与 A 共享）。

**① 变长打平（零 padding）**，A 的 48 个 + B 的 18 个新 token 首尾相接：
```python
num_scheduled_tokens = [48, 18]            # 来自 SchedulerOutput
input_ids = [a0,a1,...,a47,  b32,b33,...,b49]   # 一维，总长 48+18 = 66
#            └── A 全部 48 ──┘└─ B 只有新的18个 ─┘   (B 前32个前缀已缓存,不重算)
```

**② `positions`（每个 token 在各自序列里的绝对位置）：**
```python
positions = [0,1,2,...,47,   32,33,...,49]
#            └─ A: 0~47 ─┘   └ B: 从32起(前32已算) ┘
```
注意 B 从 **32** 开始，不是 0——因为前 32 个位置的 KV 已经在共享块里算好了。

**③ 打平索引（§2 的 np.repeat/cumsum 在本例的值）：**
```python
req_indices = [0]*48 + [1]*18            # 每个位置属于哪个请求
cu_num_tokens = [48, 66]                 # 累积边界(cu_seqlens): A到48, B到66
```

**④ `slot_mapping`（写 KV：每个 token 写到哪个物理槽位）**
槽位 = 物理块号 × 16 + 块内偏移：
```python
# A 的 48 个 token → 块 10,11,12
#   token0→10*16+0=160, token1→161, ... token15→175(块10满)
#   token16→11*16+0=176, ... token31→191(块11满)
#   token32→12*16+0=192, ... token47→207(块12满)
# B 的 18 个新 token → 从块 13 开始(前2块是命中的,不重写)
#   b32→13*16+0=208, ... b47→223(块13满)
#   b48→14*16+0=224, b49→225(块14用了2格)
slot_mapping = [160,161,...,207,  208,209,...,225]   # 长度 66，与 input_ids 对齐
```

**⑤ `block_table`（读 KV：每个请求的历史 KV 在哪些块）**
二维，每行一个请求（不等长的用 0/padding 补齐到最大块数）：
```python
block_table = [[10, 11, 12,  0],     # 请求 A：3 块
               [10, 11, 13, 14]]     # 请求 B：4 块（前2块与A同！）
```
> 这张表直接体现了「B 复用 A 的物理块 10、11」——GPU 做 attention 时，
> 按这张表跳着读，B 读到的前 32 个 token 的 KV 就是 A 算好的那份。

**⑥ `logits_indices`（只有要预测下一个 token 的位置才算 logits）**
prefill 时只有每个请求的**最后一个** token 负责生成下一个：
```python
logits_indices = [47, 65]    # A 的最后一个(下标47)、B 的最后一个(下标65)
```

**这一层的数据变形一句话总结：**
```
SchedulerOutput{A:48, B:18}
   ──打平──▶ input_ids(66) + positions + slot_mapping(写) + block_table(读)
   ──喂给模型──▶ hidden_states(66×d) ──取[47,65]──▶ logits(2×词表) ──采样──▶ A吐1个, B吐1个
```

---

## 4. 跑模型：`execute_model` → `_model_forward`

文件：`gpu_model_runner.py`（`execute_model` 第 4259 行，`_model_forward` 第 3937 行）

`execute_model` 很长（几百行，含分布式、投机解码、CUDA graph 等分支），但主干就三步：**准备输入 → 前向 → 交给采样**。核心调用：

```python
            model_output = self._model_forward(
                input_ids=input_ids,
                positions=positions,
                intermediate_tensors=intermediate_tensors,
                inputs_embeds=inputs_embeds,
                **model_kwargs,
            )
```

而 `_model_forward` 本身极简，就是调用模型对象：

```python
    def _model_forward(self, input_ids, positions, ...):
        return self.model(
            input_ids=input_ids,
            positions=positions,
            intermediate_tensors=intermediate_tensors,
            inputs_embeds=inputs_embeds,
            **model_kwargs,
        )
```

【Python/PyTorch 重点 —— 两个关键语法】

- `self.model(input_ids=..., ...)`
  - 【PyTorch】`self.model` 是一个 `nn.Module`（PyTorch 模型对象）。**直接「像调用函数一样」调用一个模型对象 `self.model(...)`，PyTorch 会自动触发它的 `forward` 方法**。这是 PyTorch 的核心约定：你几乎从不手写 `.forward()`，而是 `model(x)`。
  - 【逻辑】这一行就是「模型前向计算」：input_ids（一维打平的 token）进去，经过 embedding → N 层 transformer（每层含 attention + FFN）→ 输出 `hidden_states`（每个 token 的高维表示）。attention 层内部会用到 §3 的 block_table/slot_mapping 读写 KV Cache。
- `**model_kwargs`
  - 【Python 重点】`**` 是「字典解包」。如果 `model_kwargs = {"a": 1, "b": 2}`，那么 `f(**model_kwargs)` 等价于 `f(a=1, b=2)`。用于把一批可变的关键字参数灵活传下去。
  - （对应地，`*args` 是「列表解包」，把列表拆成位置参数。）

前向出来后，从 hidden_states 里取出「要预测下一个 token 的那些位置」，算 logits（词表上每个词的分数）：

```python
                sample_hidden_states = hidden_states[logits_indices]
                logits = self.model.compute_logits(sample_hidden_states)
```

【逻辑】

- 不是每个 token 都要预测下一个词。prefill 时只有**每个请求的最后一个 token** 需要（它负责生成下一个）。`logits_indices` 就是这些位置的下标。
- `hidden_states[logits_indices]`
  - 【PyTorch】**张量的花式索引**：用一个下标数组从张量里一次性挑出多行。等价于「把 logits_indices 里指定的那些行取出来」。这是 PyTorch/NumPy 的核心操作。
- `compute_logits`：把选出的 hidden state 投影到词表维度，得到每个候选词的分数。这是采样的输入。

---

## 4.5 揭开黑盒：`self.model(...)` 一次前向的矩阵运算全过程 ★（可运行 demo）

上面那行 `self.model(input_ids=...)` 是整篇最「黑盒」的一步——一行代码，里面却是 embedding → 多层 transformer → hidden_states 的全部矩阵运算。这一节用一个**缩微到能手算、能直接运行**的玩具模型，把这行黑盒彻底拆开，让你看清每一步张量的**形状**和**数值**怎么流动。

> 重要缩放说明：真实 Llama-3.1-8B 是 `d_model=4096`、`32` 层、`32` 个注意力头、词表 `128256`。这里全部缩小到**能在纸上算清**的玩具尺寸：`d_model=4`、`1` 层、`1` 头、词表 `6`、序列长度 `3`。**运算的结构和真实的完全一样，只是数字小**。看懂这个小的，大的只是把维度乘上去、层数叠上去。

### 4.5.1 设定：一个 3 token 的迷你 prefill

沿用统一示例的思路，假设有一个请求，prefill 3 个 token（对应 §3 里 `input_ids` 的一小段缩影）：

```
token 序列:   [7, 2, 5]      ← 3 个 token 的 id（真实里是分词结果）
d_model = 4                  ← 每个 token 用 4 维向量表示（真实是 4096）
词表大小 = 6                  ← 只有 6 个候选词（真实是十几万）
1 层 transformer, 1 个 attention 头
```

这正是 `_prepare_inputs`（§2）打平后交给模型的 `input_ids`（一维，长度 3）。下面走一遍 `self.model(...)` 内部。

### 4.5.2 完整可运行 demo（纯 NumPy，复制即跑）

```python
import numpy as np

np.random.seed(0)                      # 固定随机，保证你跑出的数和下面一致

# ===== 0. 输入：_prepare_inputs 打平后的 input_ids（§2 的产物）=====
input_ids = np.array([7, 2, 5])        # 3 个 token，一维（varlen 打平）
seq_len = len(input_ids)               # 3
d_model = 4                            # 隐藏维度（真实 4096）
vocab = 6                              # 词表大小（真实 128256）

# ===== 1. Embedding：token id -> 向量 =====
# 词嵌入表：形状 [vocab, d_model]，每行是一个词的向量
embed_table = np.random.randn(vocab, d_model)
x = embed_table[input_ids]             # 花式索引：按 id 取行 -> [3, 4]
print("① embedding 后 x:", x.shape)    # (3, 4)  每个 token 一个 4 维向量

# ===== 2. Attention 层 =====
# 2a. 用三个权重矩阵，把 x 投影成 Q, K, V（都是 [d_model, d_model]）
Wq = np.random.randn(d_model, d_model)
Wk = np.random.randn(d_model, d_model)
Wv = np.random.randn(d_model, d_model)
Q = x @ Wq                             # [3,4]@[4,4] -> [3,4]
K = x @ Wk                             # [3,4]
V = x @ Wv                             # [3,4]
print("② Q/K/V:", Q.shape, K.shape, V.shape)

# 2b. 注意力分数：Q 和 K 做点积 -> [3,3]（每个 token 对每个 token 的关注度）
scores = Q @ K.T / np.sqrt(d_model)    # [3,4]@[4,3] -> [3,3]，除以 sqrt(d) 缩放

# 2c. causal mask：token 只能看自己和前面的（不能偷看未来）
mask = np.triu(np.ones((seq_len, seq_len)), k=1) * -1e9   # 上三角设成 -无穷
scores = scores + mask

# 2d. softmax：把分数变成概率（每行和为 1）
attn = np.exp(scores - scores.max(axis=1, keepdims=True))
attn = attn / attn.sum(axis=1, keepdims=True)             # [3,3]
print("③ 注意力权重(每行和=1):\n", attn.round(2))

# 2e. 用注意力权重对 V 加权求和 -> 每个 token 融合了上下文的新表示
attn_out = attn @ V                    # [3,3]@[3,4] -> [3,4]

# 2f. 输出投影 + 残差连接（transformer 标配）
Wo = np.random.randn(d_model, d_model)
x = x + attn_out @ Wo                  # 残差：新信息叠加回原向量 -> [3,4]
print("④ attention 后 x:", x.shape)

# ===== 3. FFN（前馈网络）：两层线性 + 激活，做非线性变换 =====
W1 = np.random.randn(d_model, d_model * 4)   # 升维 [4,16]
W2 = np.random.randn(d_model * 4, d_model)   # 降回 [16,4]
h = np.maximum(0, x @ W1)              # ReLU 激活（负数归零）-> [3,16]
x = x + h @ W2                         # 再降维 + 残差 -> [3,4]
print("⑤ FFN 后 hidden_states:", x.shape)   # 这就是 _model_forward 的输出

# （真实模型：上面 2~3 是「一层」，Llama 要重复 32 层。这里只跑 1 层。）

# ===== 4. 取要预测的位置 -> compute_logits =====
# prefill 只有最后一个 token 负责预测下一个（§4 的 logits_indices）
logits_indices = np.array([seq_len - 1])     # [2]，即最后一个 token
sample_hidden = x[logits_indices]            # 花式索引 -> [1, 4]

# 投影到词表维度：hidden -> 每个候选词的分数
lm_head = np.random.randn(d_model, vocab)    # [4, 6]
logits = sample_hidden @ lm_head             # [1,4]@[4,6] -> [1,6]
print("⑥ logits(词表分数):", logits.round(2))

# ===== 5. 采样（§6）：temperature=0 -> 贪心，取分数最高的词 =====
next_token = int(np.argmax(logits, axis=1)[0])
print("⑦ 采样出的 next_token id:", next_token)
```

运行后你会看到类似（数值取决于随机种子）：

```
① embedding 后 x: (3, 4)
② Q/K/V: (3, 4) (3, 4) (3, 4)
③ 注意力权重(每行和=1):
 [[1.   0.   0.  ]      ← token0 只能看自己
  [0.31 0.69 0.  ]      ← token1 看 token0+自己
  [0.28 0.15 0.57]]     ← token2 看前两个+自己
④ attention 后 x: (3, 4)
⑤ FFN 后 hidden_states: (3, 4)
⑥ logits(词表分数): [[ ... 6 个数 ... ]]
⑦ 采样出的 next_token id: 3
```

### 4.5.3 这个 demo 对应到 vLLM 源码的哪里

把上面 7 步和真实代码一一对上，你就打通了「数学」和「工程」：

| demo 步骤 | 干的事 | vLLM 里对应 |
|---|---|---|
| ① embedding | id → 向量 | `self.model(...)` 内部第一步（模型的 `embed_tokens`） |
| ② Q/K/V 投影 | 线性变换 | 模型每层 attention 的 `qkv_proj` |
| ③④ 注意力 | scores→softmax→加权 | §5.2 的 `flash_attn_varlen_func`（GPU 上高度优化的同一套数学） |
| ⑤ FFN | 非线性变换 | 模型每层的 MLP |
| （重复 N 层） | 叠加表达能力 | Llama 的 32 层 `decoder_layers` |
| ⑥ compute_logits | hidden→词表分数 | §4 的 `self.model.compute_logits`（`lm_head`） |
| ⑦ argmax | 挑 token | §6 的 `sample_tokens`（这里是 temperature=0 贪心） |

### 4.5.4 从 demo 到真实 vLLM：三个关键差异

demo 把结构讲清了，但真实 vLLM 为了「快」和「省显存」，在三处做了工程化改造——这正是前面几篇的主题：

1. **KV 不重算，而是缓存**（§3、§5.1）：demo 里每次都重新算全部 K/V。真实 decode 时，前面 token 的 K/V 已经算过并存在 KV Cache 里（`reshape_and_cache` 写入），当前步只算**新 token** 的 K/V，历史直接从物理块读——这就是 `slot_mapping`（写）+ `block_table`（读）的意义。
2. **多请求打平，不是一个个跑**（§2）：demo 只有 1 个序列。真实一个 batch 里 A/B/C 多个请求首尾相接成一维长条，靠 `cu_seqlens` 区分边界，一次 kernel 算完所有请求（连续批处理，02 篇）。
3. **kernel 融合 + CUDA Graph**（§5、§7.2）：demo 里 `②③④` 是分开的 numpy 运算。真实用 `flash_attn_varlen_func` 把它们**融合成一个 GPU 算子**（不落地中间的 `[3,3]` 分数矩阵，省显存也更快），decode 阶段还用 CUDA Graph 录制回放省启动开销。

> 一句话：**demo 展示的是「数学骨架」，vLLM 干的是「让这套骨架在 GPU 上又快又省地跑大 batch」**。你看懂了骨架，再回头看 §2~§7 的每个优化，就知道它们各自在优化骨架的哪一处。

---

## 5. attention kernel 如何用 block_table —— 分页注意力落地

文件：`vllm/v1/attention/backends/flash_attn.py`（`forward` 第 874 行）

模型每一层的 attention，最终调到 backend 的 `forward`。这里能看到 03 讲的分页 KV 如何被真正读写。

### 5.1 写 KV Cache：用 slot_mapping

```python
        reshape_and_cache_flash(
            key,
            value,
            key_cache,
            value_cache,
            slot_mapping,          # ← 每个新 token 的 KV 写到哪个槽位
            self.kv_cache_dtype,
            layer._k_scale,
            layer._v_scale,
        )
```

【逻辑】当前这一步新算出来的 `key`/`value`，按 `slot_mapping` 指定的位置，**分散写入（scatter）** 到物理 KV Cache 里。第 i 个 token 写到 `slot_mapping[i]` 那个槽。写完，这些 KV 就缓存住了，后续步骤能复用（这正是 03 「KV Cache 复用」的物理实现）。

### 5.2 读 KV 做注意力：用 block_table

```python
                flash_attn_varlen_func(
                    q=query[:num_actual_tokens],
                    k=key_cache,               # ← 整个物理 KV Cache
                    v=value_cache,
                    out=output[:num_actual_tokens],
                    cu_seqlens_q=cu_seqlens_q, # ← §2 算的请求边界
                    max_seqlen_q=max_seqlen_q,
                    seqused_k=seqused_k,
                    softmax_scale=self.scale,
                    causal=causal,
                    block_table=block_table,   # ← §3 的块表：告诉 kernel 每个请求的 KV 在哪些块
                    ...
                )
```

【逻辑 —— 这是 PagedAttention 的核心 kernel 调用，务必理解】

- `flash_attn_varlen_func` 是 FlashAttention 的**变长版本 kernel**（GPU 上的高性能算子）。
- `q`（query）是本轮要计算的 token，`k`/`v` 是**整个物理 KV Cache**（所有请求的块混在一起）。
- **`block_table` 是关键**：它告诉 kernel「请求 R 的历史 KV，分散在物理块 [37, 5, 42, ...] 里」。kernel 就能**按块跳着读**每个请求自己的 KV，而不需要它们在物理上连续。
- `cu_seqlens_q` 让 kernel 知道 q 里「哪一段属于哪个请求」，从而每个请求只和自己的历史 KV 做注意力，互不串扰。

**一句话**：`block_table` + varlen kernel = 「逻辑连续、物理分散」的 KV 能被正确、高效地做注意力。这就是 PagedAttention 名字的由来，也是 vLLM 高吞吐的底层支撑。

---

## 6. 采样出 token：`sample_tokens`

文件：`gpu_model_runner.py`（第 4638 行）

有了 logits（词表分数），最后一步是「挑出下一个 token」。回顾 01：`EngineCore.step()` 里 `execute_model` 拿到结果后（或单独）调 `sample_tokens`。

```python
    def sample_tokens(self, grammar_output):
        ...
        # Apply structured output bitmasks if present.
        if grammar_output is not None:
            apply_grammar_bitmask(
                scheduler_output, grammar_output, self.input_batch, logits
            )

        with record_function_or_nullcontext("gpu_model_runner: sample"):
            sampler_output = self._sample(logits, spec_decode_metadata)

        self._update_states_after_model_execute(
            sampler_output.sampled_token_ids, scheduler_output
        )
```

【逻辑】

- `apply_grammar_bitmask`：结构化输出（强制合法 JSON 等）时，把「非法的词」分数压到负无穷，让它们不可能被选中。普通场景跳过。
- `self._sample(logits, ...)`：**真正的采样**。根据每个请求的 `SamplingParams`（温度、top-p、top-k、贪心等）从 logits 里挑出 token id。
  - 贪心（temperature=0）：直接取分数最高的词。
  - 采样：按概率分布随机抽（温度调节随机性，top-p/top-k 限制候选范围）。
  - 采样逻辑在 `vllm/v1/sample/` 目录。
- `sampler_output.sampled_token_ids`：**本轮每个请求新生成的那个 token**。它会被打包进 `ModelRunnerOutput` 返回，回到 02 的 `scheduler.update_from_output` 去「记账」——追加到请求输出、判断是否结束。

至此，`EngineCore.step()` 的一整轮闭环完成，生成了一个 token。下一轮 step 再来一遍。

---

## 7. Worker 层的两个「幕后」职责（gpu_worker.py）

`GPUWorker` 在正式推理前，还干了两件对理解 vLLM 很重要的事：

### 7.1 显存 profiling：`determine_available_memory`

【逻辑】vLLM 启动时要决定「能开多少 KV Cache 块」（03 的物理块总数从哪来）。做法：
1. 加载模型权重后，先跑一次「假的」最大负载前向（`_dummy_run`），看峰值占了多少显存。
2. `可用于 KV 的显存 = 总显存 × gpu_memory_utilization - 权重 - 激活峰值`。
3. 除以「每块的大小」= 能开的块数。

这就是配置参数 `gpu_memory_utilization`（默认 0.92，即用 92% 显存）的作用点。理解了这里，就明白「为什么调大它能容纳更多并发请求，但调太大会 OOM」。

### 7.2 CUDA Graph 捕获：`compile_or_warm_up_model`

【逻辑】decode 阶段每步计算量小但调用极频繁，Python 逐次启动 GPU kernel 的开销（launch overhead）占比很高。vLLM 用 **CUDA Graph** 把「一次 decode 的整套 GPU 操作」录制成一张图，之后每步直接「回放」这张图，省掉重复的启动开销，显著加速。这在启动时预先「捕获」好（针对若干常见 batch size）。

这两件事解释了「为什么 vLLM 启动慢一点，但跑起来快」。

---

## 8. 全链路总回顾（贯通 01-04）

```
用户 LLM.generate                                        【01】
  → LLMEngine.add_request（分词，构造 Request）           【01】
  → EngineCore.add_request（进 waiting 队列）             【01】
  → 循环 EngineCore.step():                               【01】★心跳
       │
       ├─ 1. scheduler.schedule()                         【02】调度
       │      running 优先 → allocate_slots 申请显存      【02/03】
       │        不够 → 抢占 / 停止放行
       │      waiting 放新 → 前缀缓存命中检查（省算力）    【03】
       │      产出 SchedulerOutput（施工图）
       │
       ├─ 2. executor.execute_model(SchedulerOutput)      【04】执行
       │      GPUModelRunner._prepare_inputs
       │        打平变长请求成一维张量 + slot_mapping/block_table  【04】
       │      _model_forward → self.model(...)            【04】
       │        每层 attention: reshape_and_cache 写KV     【04/03】
       │                        flash_attn_varlen_func 用 block_table 读KV  【04/03】
       │      compute_logits → sample_tokens 采样出 token 【04】
       │
       └─ 3. scheduler.update_from_output()               【02】记账
              写回新 token、判断结束、free 释放显存         【02/03】
       ↑ 反复，每轮每个请求 +1 token，直到结束
  → LLMEngine.step 收结果 → detokenize → RequestOutput    【01】
```

**执行层（04）三个必记要点：**

1. **变长打平**：请求首尾相接成一维长条 + cu_seqlens 边界，零 padding 浪费。
2. **两张映射表**：`slot_mapping` 管写 KV、`block_table` 管读 KV —— PagedAttention 在 GPU 上的落地。
3. **模型即函数**：`self.model(...)` 触发 PyTorch forward；花式索引取 logits 位置；`sample_tokens` 挑出 token。

---

## 9. Python / PyTorch 语法速查（本文新增）

| 语法 | 含义 | 例子 |
|------|------|------|
| `[f(x) for x in seq]` | 列表推导式（遍历+收集） | `[num[i] for i in req_ids]` |
| `**kwargs` | 字典解包成关键字参数 | `f(**model_kwargs)` |
| `*args` | 列表解包成位置参数 | `f(*items)` |
| `model(x)` | 调用 nn.Module 触发 forward | `self.model(input_ids=...)` |
| `tensor[indices]` | 张量花式索引（按下标数组取行） | `hidden_states[logits_indices]` |
| `np.array(list)` | Python 列表转 NumPy 数组 | `np.array(tokens, dtype=np.int32)` |
| `np.repeat(v, n)` | 按次数重复元素 | `np.repeat([0,1,2],[2,5,3])` |
| cumsum（累积和） | 求前缀累积和 | `[2,5,3]→[2,7,10]` |
| `dtype=np.int32` | 指定数值类型 | 整数张量 |

---

## 学习收尾

到这里，你已经贯通了 vLLM 的核心主干：

- **01** 请求的一生（控制流骨架，step 心跳）
- **02** 调度器（连续批处理、抢占）
- **03** PagedAttention（分页 KV、前缀缓存、LRU）
- **04** 执行层（变长打平、分页 attention kernel、采样）

**建议动手**：用一个最小脚本 `LLM(model=...).generate(["Hello"], SamplingParams(max_tokens=5))`，在这几个函数打断点单步跟一遍，把四篇文档串起来看，理解会立刻立体化：
- `EngineCore.step`（01）
- `Scheduler.schedule` / `_preempt_request`（02）
- `KVCacheManager.allocate_slots`（03）
- `GPUModelRunner._prepare_inputs` / `sample_tokens`（04）

**进阶方向**（掌握主干后按需）：投机解码 `v1/spec_decode/`、分布式并行 `distributed/`、具体模型实现 `model_executor/models/`（先读 `llama.py`）、在线服务 `entrypoints/serve/`。

有任何一行代码看不懂，随时贴给我，我帮你逐行拆解。
