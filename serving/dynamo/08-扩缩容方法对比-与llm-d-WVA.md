# 08 · 扩缩容方法对比：Dynamo Planner vs llm-d WVA

> **对照基线**：Dynamo `main @ 946acce` ｜ [llm-d WVA](../llm-d/autoscaling/) `release-0.9 @ d5d5864`。
> 本篇只比**一件事**：给定一个 LLM 推理部署，「该开几个副本」这个问题两边是怎么解的。路由、KV 管理等其它维度不在本篇范围。
> 读法建议：先读 [03 篇](03-核心代码分析-SLA-Planner.md)（Dynamo 的算式）+ [07 篇](07-弹性扩缩容实现逻辑.md)（Dynamo 的流水线），再读 [WVA 01 篇](../llm-d/autoscaling/01-核心原理-轮询引擎与双阈值容量模型.md)（WVA 的公式），最后回到这里。

## 0. 为什么值得专门比这两个

它们是目前两条**都实现完整、但方法论相反**的 LLM 语义扩缩容路线：

| | llm-d WVA | Dynamo Planner |
|---|---|---|
| 全称 | Workload Variant Autoscaler | SLA Planner |
| 语言 / 形态 | Go / K8s controller（leader 选举 + 轮询） | Python / DGD 里的一个普通组件 |
| 输出 | `wva_desired_replicas` 指标，**不改副本** | 直接改 DGDSA 的 scale 子资源 |
| 决策依据 | **KV token 的供需比** | **单副本容量（rps）** |
| 方法论 | **倒推**：从「现在挤不挤」推该加几个 | **正推**：从「一个副本能扛多少」推需要几个 |

两者都不取代 HPA，而是把 LLM 语义补进去（通用 HPA 在 LLM 上的四个失效点见 [`autoscaling/README`](../../autoscaling/README.md#为什么-llm-推理需要专门的扩缩容)）。

## 1. 一句话分歧

> **WVA 问「当前的 KV token 供给够不够用」；Dynamo 问「一个副本在 SLA 内每秒能处理几个请求」。**

这一条分歧派生出后面所有差别。WVA 的核心量是**一个比值**（需求/供给），所以它必须选阈值；Dynamo 的核心量是**一个绝对容量**，所以它必须有性能模型。

## 2. 核心公式对照

**WVA**（`internal/engines/saturation/engine_v2.go`）：

\[
\begin{aligned}
RC &= \max(0,\ \tfrac{\text{TotalDemand}}{0.85} - \text{TotalAnticipatedSupply}) \quad (>0 \Rightarrow 扩容)\\
SC &= \max(0,\ \text{TotalSupply} - \tfrac{\text{TotalDemand}}{0.70}) \quad (>0 \Rightarrow 缩容)\\
n_{up} &= \max_i \lceil RC_i / PRC_i \rceil,\qquad n_{down} = \min_i \lfloor SC_i / PRC_i \rfloor
\end{aligned}
\]

单位是 **KV token**。`PRC` = 每副本容量 = `min(k1 显存受限, k2 计算受限)`。

**Dynamo**（`core/perf_model/engine_query.py` + `core/throughput_scaling.py`）：

\[
\begin{aligned}
\text{单副本容量}:\ &rps^\* = \max_{b \le b_{max}} \Big\{ \tfrac{b}{\text{TTFT}(b)} \ \Big|\ \text{TTFT}(b) \le \text{SLA}_{ttft} \Big\} \quad (\text{prefill})\\
&rps^\* = \max_{b} \Big\{ \tfrac{b}{osl \cdot \text{ITL}(b)} \ \Big|\ \text{ITL}(b) \le \text{SLA}_{itl} \Big\} \quad (\text{decode})\\
\text{副本数}:\ &n = \max\Big(\big\lceil \tfrac{\text{demand\_rps}}{rps^\*} \big\rceil,\ \text{min\_endpoint}\Big)
\end{aligned}
\]

单位是 **请求/秒**。其中 `ITL(b) = forward(b) / accept\_length`（投机解码进了公式）。

**结构差异一目了然**：WVA 是一个减法（差多少补多少），Dynamo 是一个除法（需求除以单位产能）。

## 3. 单副本容量从哪来

这是两边最实质的分歧点。

| | WVA | Dynamo |
|---|---|---|
| 怎么得到 | **从当前观测推**：`PRC = min(k1, k2)`，`k1 = KV容量 × 0.80`，`k2` 走四级优先级链（观测态优先） | **从性能模型查**：离线 profile 曲线 / AIC 仿真 / worker 自报，再用在线 FPM 回归修正 |
| 需要预先 profile | **不需要** | SLA 档**需要**；easy mode 档不需要 |
| 多副本聚合 | `PRC = median(各副本容量)`——中位数抗离群，刚启动没填满 KV 的副本不会拉低估计 | 单副本容量是模型给的，与实例数无关 |
| 换硬件 / 改并行度 | 自动跟随观测 | **旧曲线作废**，要重跑 profile |
| 冷启动（0 副本） | 从 capacity store 取**历史值**，所以 0 副本也能算出该开几个 | 零副本无 FPM，负载环直接 `insufficient_data` |

**这条差异的代价是对称的**：

- WVA 的观测式估计**不需要前置工作**，但它只能反映「当前工况下的容量」——负载模式一变，`k2` 的估计也跟着变，容量的含义不稳定。
- Dynamo 的模型式估计**含义明确且可外推**（这才使得「少一台会怎样」的预演成为可能，见 §5），但**必须先有曲线**，而且曲线与硬件/并行配置强绑定。

> 一个容易被忽略的连带效应：**Dynamo 的容量估计吃 Router 的缓存命中红利**。`_effective_prefill_isl`（`engine_query.py:753`）先扣掉 KV 命中再算 prefill 工作量，所以命中率上去了、需要的副本数就下来了。WVA 也有前缀缓存折减，但只作用于**调度器队列**那部分需求（`analyzer.go:763~771`），且不作用于本地引擎队列。

## 4. SLA 如何进入决策

| | WVA | Dynamo（`sla` 档） |
|---|---|---|
| SLA 进入方式 | **间接**——0.85 / 0.70 是经验阈值，隐含了「利用率到这个水平延迟就会变差」 | **直接**——TTFT / ITL 是容量搜索的**约束条件** |
| 能否指定「我要 200ms TTFT」 | 不能，只能调阈值 | 能 |
| 投机解码 | 不在模型里 | 进了公式：`itl = forward / accept_length` |
| SLA 达不成时 | 概念上不存在「达不成」，只有「利用率高」 | 只打 warning，**照样按该容量算副本数**（尽力而为，不罢工） |

**这是 Dynamo 这套方法论的主要卖点**：SLA 从「间接调参」变成「直接输入」。代价就是 §3 那条——得先有曲线。

> ⚠️ 但要记住 [03 篇 §0](03-核心代码分析-SLA-Planner.md#0-一句话概括) 那个默认值：`optimization_target` **默认是 `throughput`**，走的是静态阈值 easy mode。**默认档的 Dynamo 其实和 WVA 是同一类做法**（见 §12），SLA 主线要显式开。

## 5. 防抖动：静态死区 vs 模型预演

两边都知道「同一个阈值当扩容线又当缩容线必然抖」，解法不同。

**WVA：滞回带（hysteresis band）**

```
   0.85 ┤━━━━━━━━ scaleUpThreshold ── 之上扩容
        │   死区：什么都不做
   0.70 ┤━━━━━━━━ scaleDownBoundary ── 之下缩容
```

加上一处**刻意的不对称**：扩容用 `TotalAnticipatedSupply`（含 pending，**多**算 → 抑制级联重复扩容），缩容用 `TotalSupply`（不含 pending，**少**算 → 防止刚扩就缩）。两侧都朝保守偏，方向相反。

**Dynamo：缩容前先预演**

```
consolidation = N / (N-1)                        # 少一台后每台要多扛的倍数
T_own        = 预测(queue_scale=0)               # 新请求自己的前向耗时，与 N 无关
post_est     = 预测(queue_scale=consolidation)   # 缩容后的总 TTFT
queue_budget = (SLA_ttft − T_own) × sensitivity  # sensitivity 默认 0.8
可以缩容 ⟺ (post_est − T_own) < queue_budget
```

decode 侧还多一道**硬缓存可行性检查**：缩容后 `(sched_kv + queued_kv + queued_prefill) × N/(N-1) ≥ max_kv_tokens` 就直接拒绝——因为性能模型无法建模「超出缓存后的块淘汰」，属于模型外推区，只能硬挡。

| | WVA 死区 | Dynamo 预演 |
|---|---|---|
| 本质 | **静态**：两条固定阈值线 | **动态**：每次现算「缩完会不会违约」 |
| 依赖 | 无 | 需要可外推的性能模型 |
| 调参 | 调两个阈值 | 调 `load_scaling_down_sensitivity`（默认 80） |
| 失效场景 | 阈值配倒挂（WVA 有防御：整对退回默认） | 模型被单调性校验拒绝 → 干脆不决策 |
| 额外机制 | pending 的不对称计入 | DGD ready 门禁（上一轮 Pod 没起来不叠加） |

**取舍很清楚**：死区简单、无依赖、行为可预测，但阈值是拍出来的；预演贴合真实负载，但依赖模型质量，且模型不可信时会退化成「不动」。

> Dynamo **没有** cooldown 计时器，也没有死区。它的六道防振荡机制见 [07 篇 §9](07-弹性扩缩容实现逻辑.md#9-防振荡六道机制)。

## 6. P/D 联合扩缩

两边都得解「prefill 和 decode 两个池不能各自为政」。

| | WVA | Dynamo |
|---|---|---|
| 手段 | **Δ_util 匹配**：两侧利用率变化量要配得上 | **GPU 预算的比例 clamp**（`proportional_clamp_pair`） |
| 约束的是 | 「效果对齐」——扩完两侧的紧张程度应当相当 | 「资源不超发」——两侧加起来不能超预算 |
| 单侧算不出来时 | 该 role 不参与，其它 role 照常 | **整个 tick 放弃**，另一侧标 `partner_not_ready` |
| 角色感知 | 需求计费按角色不同：prefill 算 `I`，decode/both 算 `I+O` | prefill 用 TTFT 约束、decode 用 ITL 约束，两套独立容量搜索 |
| 提交 | 每个 role 各自产出 desired replicas | **一次性**产出 `ScalingDecision(num_prefill=…, num_decode=…)` |

**WVA 那个「decode 侧按 `I+O` 而非 `I+O/2` 计费」的选择值得单独记**：注释解释这是「峰值、无抢占」的规划口径——请求一旦被接纳，decode 的 KV 单调增长，引擎无法在不抢占+重算的前提下释放，所以必须留出最终大小。它**明知这会朝扩容方向偏**，并认为「decode 容量欠配导致抢占+重算」的代价高于多一个副本。

Dynamo 对应位置的取向是一样的（宁可多开），但表达方式不同：它体现在 `max(floor, min(ceiling, rec))` 里 floor 优先、以及功耗/GPU 预算只做 ceiling 不做 floor。

## 7. 多信号融合：投票 vs 类型化合并

两边都支持「多个信号源同时对副本数发表意见」，这是架构上最有意思的一处对照。

**WVA：analyzer 投票**

```
扩容：any-up  + ceil + max   —— 任一 analyzer 说缺就扩，谁要得多听谁的
缩容：all-down + floor + min —— 所有 live analyzer 都说能缩才缩，谁最保守听谁的
```

配套两个机制防「哑掉的信号源永久否决缩容」：
- **liveness 门**：per-(模型, analyzer)、3 个周期、纯内存；非 live 不参与否决。
- **opt-in**：代码里注册了但配置里没列的 analyzer **不参与**——防止未配置的 analyzer 返回 `SpareCapacity = 0` 静默否决所有缩容。

**Dynamo：类型化合并**（`plugins/merge/type_aware.py`）

```
1. REJECT 短路（优先级高于 final）
2. final=True 的 priority 最小者直接胜出
3. 分桶后： floor   = max(所有 AT_LEAST)
            ceiling = min(所有 AT_MOST)
            rec     = priority 最小的 SET，否则 baseline
            result  = max(floor, min(ceiling, rec))
```

配套机制：
- **熔断器**（per-plugin 三态机，默认 5 次失败 / 30s cooldown）
- **HOLD_LAST**：插件不到执行点时继承上次结果（慢环下界靠这个在中间的 tick 生效）
- **CONSTRAIN 阶段禁 SET**：约束类插件只能收紧不能改写目标

**对照看**：

| | WVA | Dynamo |
|---|---|---|
| 意见的类型 | 只有一种（RC / SC 数值） | **三种**（SET / AT_LEAST / AT_MOST） |
| 融合方式 | 按方向分别用 max / min，扩缩不对称 | 统一夹取公式，靠类型区分语义 |
| 否决权 | all-down 即一票否决缩容 | REJECT 短路整个 tick |
| 防哑机制 | liveness 门 + opt-in 缺省不参与 | 熔断器 + 默认弃权（`ACCEPT_WHEN_IDLE`） |
| 优先级 | analyzer 有 score 权重 | plugin 有 priority（数字小者优先） |

**同一个问题的两种建模**：WVA 把「意见」建模成数值，用聚合函数的方向性（max/min）表达扩缩不对称；Dynamo 把「意见」建模成带类型的约束，用类型系统表达权限（谁能定目标、谁只能收紧）。Dynamo 这套更适合开放给第三方插件，WVA 那套更简单直接。

## 8. 异构机型与成本

这是 **WVA 明显更强的一处**。

| | WVA | Dynamo |
|---|---|---|
| 多机型变体 | **一等公民**：同一模型可有多个 variant（如 `llama-8b-a100` cost 10.0 / `llama-8b-l40s` cost 4.0） | 无对应概念，一个 component 一种规格 |
| 选择依据 | `costEfficiency = Cost / PerReplicaCapacity`，**升序**挑最划算的 | — |
| 效果 | 需要扩容时优先给便宜的机型加副本 | 只能扩当前 component |
| 预算约束 | quota / inventory limiter | GPU 预算 + **功耗预算**（`total_gpu_power_limit`） |

WVA 的统一示例里那个决策很有代表性：V1(A100) 效率 `1.111e-4`、V2(L40S) 效率 `6.667e-5`，**最终给 L40S 从 0 扩到 1**，而不是给贵的 A100 加副本。这是「cost-aware」三个字的实际含义。

反过来 Dynamo 有一样 WVA 没有的：**功耗预算**（[07 篇 §6.4](07-弹性扩缩容实现逻辑.md#64-功耗预算的-ceiling-clamp)）。它只做天花板不做地板，且冲突时**赢过 GPU 下限**。在电力受限的机房里这是硬需求。

## 9. 决策怎么落地

| | WVA | Dynamo |
|---|---|---|
| 改不改副本 | **不改**，只产出 `wva_desired_replicas{variant_name, namespace}` | **自己改** |
| 谁执行 | KEDA / HPA 读指标后执行 | Planner → DGDSA scale 子资源 → operator → Deployment |
| 避免与 HPA 抢字段 | 天然不抢（自己不写） | 改的是 **DGDSA 的 scale 子资源**，HPA 也能改的同一入口；webhook 禁止直接改 DGD 的 replicas |
| 下发保护 | 无（不下发） | 至多一个在途 + **结果未知则永久挂起** + 配置代号校验 + DGD ready 门禁 |

**这是同一个思路的两种实现**：都在避免「两个控制器抢同一个 `replicas` 字段」。WVA 选择彻底不碰，Dynamo 选择走标准 scale 接口。

Dynamo 那条「结果未知则永久挂起」值得单独提醒：一旦提交结果不确定，**所有自动扩缩容停止且不自愈**，必须人工确认部署状态后重启 Planner。WVA 因为不下发，没有这类状态机风险。

## 10. 缩容到零

| | WVA | Dynamo |
|---|---|---|
| 支持程度 | **内建一等公民**，含唤醒路径 | 允许配 `min_endpoint = 0`（默认 1） |
| 0 副本时怎么算 | 从 capacity store 取历史 `PerReplicaCapacity`，需求来自 EPP 网关的 flow-control 队列（**请求还没到任何 pod**） | 零副本无 FPM，负载环直接返回 `insufficient_data` |
| 唤醒 | KEDA 或支持 scale-to-zero 的 HPA 读指标执行 0→1 | 靠外部触发；README 宣传的「100ms 冷启动」是 [ModelExpress](https://github.com/ai-dynamo/modelexpress) 的权重流式加载，独立仓库 |

**根因在于需求信号的位置**：WVA 能看到**网关队列**里还没落到 pod 的请求，所以 0 副本时仍有需求信号；Dynamo 的快环依赖引擎上报的 FPM，**没有 worker 就没有信号**，反馈环是断的。

所以缩容到零在 Dynamo 这边更像「允许你配 0」，不是打磨过的完整路径。

## 11. 容错与降级对照

| 场景 | WVA | Dynamo |
|---|---|---|
| 信号源崩溃 | `recover()` 兜 panic，结果丢弃，**当轮就不参与投票** | 熔断器计一次失败，按弃权处理 |
| 信号源哑掉（有结果但无信息量） | liveness 门 3 周期后失去否决权 | HOLD_LAST 缓存过期后退化为弃权 |
| 模型 / 数据不可信 | k2 四级优先级链逐级降级 | 单调性校验**拒绝整个模型** → `model_not_ready`，不决策 |
| 阈值 / 配置矛盾 | `up ≤ down` 倒挂时**整对退回默认** | 配置由 pydantic 校验；代号不匹配则丢弃决策 |
| 新增插件 / analyzer | 注册在 `StartOptimizeLoop` 后**冻结**，需重启；配置漂移会发 Warning Event | 插件可运行时注册（registry server） |

**两边的共同取向：故障的信号源不应该获得否决权。** 实现手段不同（liveness 门 vs 熔断器 + 默认弃权），但要解的问题完全一致——这是多信号扩缩容系统的必备设计。

一处值得学的差异：WVA 对「必须重启才能生效的配置」做了**冻结决定 + 检测漂移 + 发 K8s Event** 的三段式，不让运维静默踩坑。这是个可复用的通用做法。

## 12. 选型建议

先问自己三个问题：

| 问题 | 倾向 |
|---|---|
| 有没有精力维护一条「batch size → 延迟」的性能曲线？ | 有 → Dynamo `sla` 档；没有 → WVA 或 Dynamo easy mode |
| 同一个模型是否要跨机型择优（成本敏感）？ | 是 → **WVA**（Dynamo 无此能力） |
| 是否要求「直接指定 TTFT/ITL 目标」？ | 是 → **Dynamo `sla` 档**（WVA 只能调阈值） |

补两条实际考量：

- **已有栈的一致性通常比方法论优劣更重要**。用 llm-d 那套（Gateway API + EPP）就用 WVA；用 Dynamo 的单仓服务栈就用 Planner——跨栈拼装要自己维护指标对齐和 replicas 所有权。
- **起步阶段用阈值档**。Dynamo 的 easy mode（`throughput` / `latency` / `load`）**才是和 WVA 同类的做法**，不需要 profile；等负载特征稳定、profile 拿到手了再切 `sla`。

## 13. 必记要点

1. **方法论对立**：WVA 从供需比**倒推**，Dynamo 从单副本容量**正推**。这一条派生出其余全部差异。
2. **单副本容量的来源是最实质分歧**：WVA 从当前观测推（免前置、含义不稳），Dynamo 从性能模型查（需前置、可外推）。**可外推**才使 Dynamo 的缩容预演成为可能。
3. **SLA 进入决策的方式不同**：WVA 间接（阈值隐含延迟），Dynamo 直接（TTFT/ITL 是搜索约束）。但 Dynamo **默认档是阈值**，不是 SLA 档。
4. **防抖动：静态死区 vs 模型预演**。WVA 用 0.85/0.70 滞回带 + pending 的刻意不对称；Dynamo 用 `N/(N-1)` 预演 + ready 门禁，**没有死区也没有 cooldown**。
5. **多信号融合：数值投票 vs 类型化约束**。WVA 用 any-up/all-down + max/min 表达不对称；Dynamo 用 SET / AT_LEAST / AT_MOST 三类型 + 统一夹取公式表达权限。
6. **P/D 联合：Δ_util 匹配（效果对齐）vs GPU 预算比例 clamp（资源不超发）**。
7. **WVA 独有：异构机型 cost-aware 择优、成熟的 scale-to-zero**（因为它能看到网关队列，0 副本时仍有需求信号）。
8. **Dynamo 独有：直接指定 SLA、投机解码进公式、功耗预算约束**。
9. **落地都在避免抢 `replicas` 字段**：WVA 不写（只出指标给 HPA/KEDA），Dynamo 写标准 scale 子资源。
10. **两边共同取向：故障信号源不得否决**（liveness 门 vs 熔断器 + 默认弃权）；**扩容激进、缩容谨慎**贯穿两者全链。

---

> **上一篇** [07 · 弹性扩缩容实现逻辑](07-弹性扩缩容实现逻辑.md) ｜ **回到** [Dynamo 系列首页](README.md)
> **相关**：[llm-d WVA 系列](../llm-d/autoscaling/) ｜ [弹性扩缩容问题域索引](../../autoscaling/README.md)
