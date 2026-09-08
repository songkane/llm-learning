# 03 · 核心代码分析：Optimizer 与 Limiter

> **源码基线**：[`release-0.9 @ d5d5864`](https://github.com/llm-d/llm-d-autoscaling/tree/d5d586408420fbe0545f827a6ff5dc2f818b16de)（2026-08-07；2026-09-07 核对）。分支与 `v0.9.0` tag 相差 3 个提交，详见 [版本对照](README.md#版本对照与复现)。

> 承接 [02 篇](02-核心代码分析-指标采集与Analyzer.md)。Analyzer 交出的是「还差 60000 token」，本篇讲**怎么把它变成「llama-8b-l40s 加 1 个副本」**，以及 GPU 不够时怎么在多个模型之间分配。

## 1. 两个优化器与选路

```go
// internal/engines/pipeline/optimizer_interfaces.go
type ScalingOptimizer interface {
	Name() string
	Optimize(ctx context.Context, requests []ModelScalingRequest, constraints []*ResourceConstraints) []domain.VariantDecision
}
```

V2 路径的优化器选择看 ConfigMap 的一个布尔字段（QM 虽进入选择代码，但后续分派会拒绝执行）（`engine.go`）：

```go
	// Select optimizer based on enableLimiter flag (both are stateless, safe to swap)
	if analyzerName == domain.SaturationAnalyzerName || analyzerName == domain.QueueingModelAnalyzerName {
		savedOptimizer := e.optimizer
		if enableLimiter {
			e.optimizer = pipeline.NewGreedyByScoreOptimizer()
		} else {
			e.optimizer = pipeline.NewCostAwareOptimizer()
		}
		if savedOptimizer != e.optimizer {
			e.recordActiveOptimizer()
		}
```

| | `CostAwareOptimizer` | `GreedyByScoreOptimizer` |
|---|---|---|
| 触发 | `enableLimiter: false`（**默认**） | `enableLimiter: true` |
| 是否吃 GPU 约束 | ❌ 完全忽略（unlimited 模式） | ✅ 按 accelerator type / namespace 的预算 |
| 目标 | **最小化成本** | **在稀缺 GPU 下做公平分配** |
| 多模型 | 各模型独立处理 | 跨模型按 fair-share 优先级排队 |
| 扩容选谁 | `cost / PerReplicaCapacity` 最小 | fair-share 值最大的模型先拿 GPU |
| 缩容选谁 | `cost` 最大 | 同一套 `scaleDownRoleIterated` |
| rescale | ❌ | ✅（`enableRescale: true` 时） |

**【逻辑】** unlimited 是默认值，意味着开箱即用时 WVA **不会因为集群没卡就不扩** —— 它照样给出目标副本数，多出来的 Pod 会 Pending，触发云上的 cluster-autoscaler 加节点。这在弹性云环境是对的行为；在固定集群上你会得到一堆 Pending Pod，此时才需要打开 limiter。

### 1.1 兜底：约束拿不到就退回 unlimited

`selectV2Optimizer`（`engine.go`）有一段非常值得学的防御逻辑：

```go
// GreedyByScore treats absent constraints as zero available capacity
// (deny-all), not as unlimited. When no provider could supply constraints
// (limiter is not constraint-backed, or every provider failed — e.g. GPU
// capacity cannot be discovered), fall back to the cost-aware optimizer, the
// engine's unlimited path, so scale-up proceeds instead of being silently
// blocked.
	if len(constraints) == 0 {
		return pipeline.NewCostAwareOptimizer(), nil
	}
	return optimizer, constraints
```

上面的注释（`engine.go`）把取舍讲得很清楚：

> `GreedyByScore` 把**缺失的约束**当作**零可用容量（deny-all）**，而不是无限。因此只要拿不到真实约束 —— limiter 不提供约束（不是 `ConstraintProvider`），或者计算失败（比如这个集群上读不到 Node 对象）—— 就退回 cost-aware，让扩容无约束地进行，而不是被静默阻塞。
> **但一个"存在且报告 0 GPU"的约束会被保留**：那是真实的"没有容量"信号，应该继续阻塞扩容。

**【设计】** 这是"区分'不知道'与'知道是零'"的典型处理。很多系统在这里犯错：把 `nil`/空当成 0，结果一个 RBAC 权限问题导致所有扩容永久停摆，而且日志里什么都看不出来。

### 1.2 约束的采集

```go
// internal/engines/saturation/engine.go
	providers := gpuConstraintProviders(e.currentGPULimiter())
	if len(providers) == 0 {
		return pipeline.NewCostAwareOptimizer(), nil
	}

	currentUsage := computeCurrentGPUUsage(requests)
	currentUsageByNS := computeCurrentGPUUsageByNamespace(requests)
	var constraints []*pipeline.ResourceConstraints
	for _, cp := range providers {
		constraint, err := cp.ComputeConstraints(ctx, currentUsage, currentUsageByNS)
		if err != nil {
			logger.Error(err, "Failed to compute GPU constraints, skipping provider", "provider", cp.Name())
			continue
		}
		constraints = append(constraints, constraint)
	}
```

当前 GPU 用量的算法（`engine_v2.go`）：

```go
		for _, vc := range satCarrier.VariantCapacities {
			state := stateMap[vc.VariantName]
			gpusPerReplica := state.GPUsPerReplica
			if gpusPerReplica <= 0 {
				gpusPerReplica = 1
			}
			usage[vc.AcceleratorName] += state.CurrentReplicas * gpusPerReplica
		}
```

`computeCurrentGPUUsageByNamespace` 是同样的逻辑但按 namespace 分桶，注释里点了一个细节（`engine_v2.go`）：

> 每个请求的 namespace 都会被表示出来（**至少是一个空的 per-type map**），这样"有配额但当前用量为零"的 namespace 依然会作为活跃 namespace 暴露给约束提供者 —— 因此依然受约束。

### 1.3 `bindingAnchor`：完整变体身份与本周期容量来源

`internal/engines/pipeline/analyzer_helpers.go` 的 `bindingAnchor` 取代旧版 `saturationEntry`。两个优化器及 rescale 都按需构造这一结果，不修改输入 analyzer 的原始结果。

| 字段 | 来源 |
|------|------|
| 完整变体名单、Cost、AcceleratorName、Role、ReplicaCount、PendingReplicas | saturation 身份载体，即使它不投票也保留 |
| 模型供需、RC/SC、RoleCapacities；每变体 PRC、Reason、Demand、Utilization | 本周期绑定的 analyzer，按 VariantName 合并 |
| 每变体 TotalCapacity | 重新计算 `ReplicaCount × PerReplicaCapacity` |

选择规则：saturation 满足 `Enabled && Live && ResultIsInformative` 时优先绑定它；否则选择唯一满足这些条件的非 saturation analyzer。没有候选或有多个非 saturation 候选时返回 nil，该模型不产生新决策，最终 emit 阶段保持副本。默认 saturation-only 与 saturation+throughput 的容量来源仍是 saturation；throughput-only 可以用 throughput 容量选择变体。

若绑定的 analyzer 缺少某变体，只有 saturation 仍投票时，才允许回退到 saturation 的整对 `(demand, PRC)`。throughput-only 下不得把 token 需求与 token/s 容量混用，缺失变体的 PRC 保持 0，不会被主动选择。此前跑过的零副本变体可由 throughput 的 `T-sfz` 历史容量补齐；从未测得容量的变体交给独立 scale-from-zero 引擎。

**GPU 已用量仍来自完整身份名单**：`computeCurrentGPUUsage` / `computeCurrentGPUUsageByNamespace` 直接找 saturation 的 `satCarrier`，不会因为容量来源不可用就把已占用 GPU 从账本里漏掉。RC/SC 组合才使用 `votingResults` 过滤后的列表。

## 2. CostAwareOptimizer：成本贪心

```go
// internal/engines/pipeline/cost_aware_optimizer.go
// CostAwareOptimizer is a per-model optimizer that minimizes total cost while
// meeting capacity requirements. It processes each model independently:
//
//   - Scale-up: adds replicas to the most cost-efficient variant (lowest cost / perReplicaCapacity)
//   - Scale-down: removes replicas from the most expensive variant (highest absolute cost)
//   - Only the cheapest variant is protected at >=1 replica; others can scale to 0
//   - Variants with pending replicas are skipped for scale-up
```

主循环（`cost_aware_optimizer.go`）：

```go
	for _, req := range requests {
		anchor := bindingAnchor(req.AnalyzerResults)
		if anchor == nil { continue }

		stateMap := buildStateMap(req.VariantStates)
		vcMap := buildCapacityMap(anchor.VariantCapacities)
		targets := initTargets(req.VariantStates)     // 初始 = 当前副本数

		// Unified dispatch: one path for all models via (model, role) math.
		// Non-disaggregated uses synthetic "both" role; disaggregated uses actual roles.
		s := votingResults(req.AnalyzerResults)
		roles, ps := initRoleState(s)
		if anyRoleNeedsScaleUp(ps, roles) {
			allocateForModelPaired(ctx, s, anchor.VariantCapacities, stateMap, nil, targets,
				costGreedyRolePick, ps, roles)
		} else {
			scaleDownRoleIterated(ctx, s, anchor.VariantCapacities, targets, stateMap)
		}

		decisions := buildDecisionsWithOptimizer(req, stateMap, vcMap, targets, "cost-aware")
		...
	}
```

**【设计】** 注意 `Unified dispatch` 那句注释：**非 P/D 分离的模型用一个合成的 `"both"` 角色**走同一条代码路径。这样"单角色"就是"多角色"的 arity-1 特例，整个优化器只需要一套按 role 迭代的逻辑。

同时注意 `if / else`：**一个模型在一个周期内要么只扩、要么只缩**，不会同时对不同变体做相反操作。`anyRoleNeedsScaleUp` 有任何角色需要扩，就整个模型走扩容路径。

### 2.1 扩容：成本效率升序挑变体

```go
// internal/engines/pipeline/cost_aware_optimizer.go
func costGreedyRolePick(
	role string, _ []NamedAnalyzerResult,
	variants []domain.VariantCapacity,
	stateMap map[string]domain.VariantReplicaState,
	_ map[string]int, targets map[string]int,
) (string, int) {
	roleVCs := variantsForRole(variants, role)
	for _, vc := range sortByCostEfficiencyAsc(roleVCs) {
		if vc.PerReplicaCapacity <= 0 {
			continue
		}
		state := stateMap[vc.VariantName]
		if state.MaxReplicas != nil && *state.MaxReplicas > 0 {
			headroom := *state.MaxReplicas - targets[vc.VariantName]
			if headroom <= 0 {
				continue          // 这个变体顶到 maxReplicas 了，看下一个
			}
			return vc.VariantName, headroom
		}
		return vc.VariantName, math.MaxInt
	}
	return "", 0
}
```

**【逻辑】** 返回值是 `(变体名, 该变体还能加几个)`。`PerReplicaCapacity <= 0` 的变体被跳过 —— 那是 `no-data` 状态，不知道加了有没有用，不敢加。顶到 `maxReplicas` 的也跳过，自然溢出到次便宜的变体。

### 2.2 缩容：成本降序 + 三级 tie-break

```go
// internal/engines/pipeline/cost_aware_optimizer.go
// sortVariantsForScaleDown orders a role's variants for cost-greedy scale-down:
//  1. Cost descending — shed the most expensive first.
//  2. Tie: score-weighted per-replica capacity ascending — Σ_i Score_i·PRC_i[v].
//  3. Tie: variant name ascending — full determinism.
func sortVariantsForScaleDown(s []NamedAnalyzerResult, roleVCs []domain.VariantCapacity) []domain.VariantCapacity {
	weighted := func(name string) float64 {
		sum := 0.0
		for _, e := range s {
			if e.Result == nil { continue }
			sum += e.Score * prcForVariant(e.Result, name)
		}
		return sum
	}
	out := append([]domain.VariantCapacity(nil), roleVCs...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Cost != out[j].Cost { return out[i].Cost > out[j].Cost }
		wi, wj := weighted(out[i].VariantName), weighted(out[j].VariantName)
		if wi != wj { return wi < wj }
		return out[i].VariantName < out[j].VariantName
	})
	return out
}
```

**【逻辑】** 三级 tie-break 里第三级"按名字排"是**为了完全确定性** —— 不加这一级，Go map 遍历顺序的随机性会让同成本同容量的两个变体每周期被随机选中，日志和指标里就看不出规律，也无法复现问题。

第二级"按 score 加权容量升序"的含义是：同价的两个变体，**先砍容量小的**（砍它对总供给影响小，更细粒度）。

### 2.3 cheapest-at-1 保护

```go
// internal/engines/pipeline/cost_aware_optimizer.go（摘录）
	for i, vc := range sortedVariants {
		...
		current := targets[vc.VariantName]
		minReplicas := 0
		if states != nil {
			if st, ok := states[vc.VariantName]; ok && st.MinReplicas != nil {
				minReplicas = *st.MinReplicas
			}
		}
		removable := current - minReplicas
		if removable <= 0 { continue }
		n := maxRemovable(vc)
		if n > removable { n = removable }
		// cheapest-at-1: the last (cheapest) variant is protected at 1 only when no
		// more-expensive variant still holds replicas (#1237's positional rule).
		if i == len(sortedVariants)-1 && current-n < 1 && !anyHasReplicas(sortedVariants[:i], targets) {
			n = current - 1
		}
		if n <= 0 { continue }
		targets[vc.VariantName] = current - n
		onRemove(vc, n)
```

**【逻辑】"位置性规则"很绕，值得拆开：**

排序后最后一个（= 最便宜的）变体，只有在**所有更贵的变体都已经清零**时，才被保护在 ≥1 副本。

为什么要这个条件？考虑两个变体 A100(贵) 2 副本 + L40S(便宜) 1 副本，模型完全没流量：
- 没有位置条件：L40S 永远被钉在 1 副本 → 即使 A100 也还有 2 副本，总共至少 3 个副本下不去。
- 有位置条件：先砍 A100 到 0，此时 L40S 是"唯一还有副本的"，才被保护在 1 —— **保证模型至少有一个最便宜的副本在服务**。

这个"最后一个活口"的保护会被 `Enforcer` 在 scale-to-zero 开启时覆盖（见 §5）。

## 3. token 缺口 → 副本数：三个核心 helper

全部在 `internal/engines/pipeline/analyzer_helpers.go`。01 篇给了公式，这里补语义。

### 3.1 扩容：ceil + 跨 analyzer 取 max

```go
// internal/engines/pipeline/analyzer_helpers.go
// 返回 max_i ceil(state[i][role] / PRC_i[v])
func roleBottleneckReplicas(s []NamedAnalyzerResult, state RolePairedState, role, v string) int {
	max := 0
	for i, e := range s {
		prc := prcForVariant(e.Result, v)
		if prc <= 0 { continue }
		n := int(math.Ceil(state[i][role] / prc))
		if n > max { max = n }
	}
	return max
}
```

函数名 `roleBottleneckReplicas` 里的 "bottleneck" 就是 `max` 的含义：**多个 analyzer 各自要求的副本数中，最大的那个才是瓶颈**。saturation 说需要 1 个、throughput 说需要 3 个 → 取 3。

### 3.2 缩容：floor + 跨 analyzer 取 min

```go
// internal/engines/pipeline/analyzer_helpers.go
func safeRemovalReplicasForRole(s []NamedAnalyzerResult, v, role string) int {
	smallest := math.MaxInt
	found := false
	for _, e := range s {
		if !e.Live { continue }          // 非活跃 analyzer 不参与约束
		prc := prcForVariant(e.Result, v)
		if prc <= 0 { continue }
		n := int(math.Floor(e.RoleSpare[role] / prc))
		if n < smallest { smallest = n }
		found = true
	}
	if !found || smallest < 0 { return 0 }
	return smallest
}
```

函数名 `safeRemovalReplicas` 里的 "safe" 就是 `min` 的含义。注意 `!e.Live` 的跳过与 `needsScaleDownForRole` 的一票否决是**成对的**：非 live 的 analyzer 既不否决缩容，也不参与"能砍几个"的表决。

### 3.3 记账：Remaining / Spare 的原地递减

```go
// internal/engines/pipeline/analyzer_helpers.go（注释）
// applyAllocation subtracts the capacity provided by n replicas of variant v
// from each analyzer's Remaining counter. Clamps to 0. The slice is the working
// allocation state; Result.RequiredCapacity is never mutated.
//
// Contract: Remaining/Spare are engine-calibrated on entry (via the universal
// threshold post-step). Helpers do not read or mutate PendingReplicas.
```

```go
// internal/engines/pipeline/analyzer_helpers.go
func applyDeallocationForRole(s []NamedAnalyzerResult, v, role string, n int) {
	for i := range s {
		if s[i].Result == nil || s[i].RoleSpare == nil { continue }
		prc := prcForVariant(s[i].Result, v)
		if prc <= 0 { continue }
		s[i].RoleSpare[role] -= float64(n) * prc
		if s[i].RoleSpare[role] < 0 { s[i].RoleSpare[role] = 0 }
	}
}
```

**【设计】** `NamedAnalyzerResult` 上有一对**可变工作计数器** `Remaining` / `Spare`（+ per-role 的 `RoleSpare`），而 `Result.RequiredCapacity` / `SpareCapacity` **永不被修改**。

这个"只读结果 + 可变工作副本"的分离很重要：优化器是**迭代**的（一次挑一个变体加一个副本，然后扣掉这个副本提供的容量，再看还差多少），如果直接改 `Result`，那么后面 emit `wva_required_capacity` 指标时就会看到一个被扣光的 0，观测性全丢。

## 4. P/D 分离的联合提交：Δ_util 匹配

这是 WVA 相对普通 HPA 的主要能力差之一。

### 4.1 问题

P/D 分离下，prefill 和 decode 是两个独立的 Deployment，但它们**必须成比例扩容**：

```
只扩 prefill 不扩 decode → prefill 算完的 KV 传不出去，decode 侧成为瓶颈，白扩
只扩 decode 不扩 prefill → decode 侧闲着等 KV，同样白扩
```

如果用两个独立的 HPA，各自看自己的指标，几乎必然扩成不匹配的比例。

### 4.2 解法：按"可满足比例"取最小，再等比裁剪

```go
// internal/engines/pipeline/analyzer_helpers.go（核心算术）
for _, role := range roles {
	prc := prcByRole[role]
	n := min(roleBottleneckReplicas(s, pickerState, role, variantByRole[role]), capByRole[role])
	nByRole[role] = n
	demand := roleAggRemaining(s, pickerState, role)
	if demand <= 0 {
		utilByRole[role] = 1.0
	} else {
		utilByRole[role] = float64(n) * prc / demand      // 该 role 本轮可满足比例
	}
}

deltaUtil := math.MaxFloat64
for _, role := range roles {
	if utilByRole[role] < deltaUtil { deltaUtil = utilByRole[role] }   // 取最小 role = 瓶颈
}
if deltaUtil <= 0 { break }
```

之后按 `deltaUtil` 反算每个角色**实际提交**的副本数 `k_role`，保证两侧提交的比例一致。

**【逻辑】用统一示例的模型 D 走一遍：**

```
角色 prefill：RC = 30000 token，PRC = 20000/副本 → n = ceil(30000/20000) = 2
              util_prefill = 2 × 20000 / 30000 = 1.33  （能满足 133%，有余量）
角色 decode： RC = 90000 token，PRC = 25000/副本 → n = ceil(90000/25000) = 4
              但 maxReplicas 只剩 2 的 headroom  → n = min(4, 2) = 2
              util_decode = 2 × 25000 / 90000 = 0.56  （只能满足 56%）

deltaUtil = min(1.33, 0.56) = 0.56   ← decode 是瓶颈

按 0.56 反算实际提交：
  prefill: k = ceil(0.56 × 30000 / 20000) = ceil(0.83) = 1
  decode:  k = 2
→ 本轮提交 (prefill +1, decode +2)，而不是 (prefill +2, decode +2)
```

**关键收益**：prefill 没有超扩。如果各自独立算，prefill 会加 2 个 —— 但 decode 只能吃下 56% 的需求，多出来的那个 prefill 副本纯浪费。下一轮循环继续，直到 `deltaUtil <= 0` 或没有 headroom。

> **这就是"变体 = P/D 角色"这个抽象的价值**：把两个 Deployment 的扩容变成一个**联合决策**，而 HPA 做不到这一点。

## 5. GreedyByScoreOptimizer：稀缺 GPU 的公平分配

```go
// internal/engines/pipeline/greedy_score_optimizer.go
// GreedyByScoreOptimizer is a multi-model optimizer for GPU-constrained
// environments. It uses iterative mean-based fair-sharing to distribute scarce
// GPUs across competing models, ordered by fair-share priority value
// (priority × Σᵢ(Remainingᵢ × Scoreᵢ) across analyzers).
```

### 5.1 fair-share 值

```go
// internal/engines/pipeline/greedy_score_optimizer.go
//	fsv = priority × Σᵢ Score_i × Σ_role pickerState[i][role]
//
// Falls back to max remaining demand when the weighted result is zero.
func fairShareValue(priority float64, s []NamedAnalyzerResult, ps RolePairedState, roles []string) float64 {
	weighted := 0.0
	for i, e := range s {
		if e.Result == nil { continue }
		roleSum := 0.0
		for _, role := range roles {
			if i < len(ps) { roleSum += ps[i][role] }
		}
		weighted += roleSum * e.Score
	}
	if fsv := priority * weighted; fsv > 0 { return fsv }
	// Fallback: max remaining demand across roles when Score=0 or priority=0.
	...
}
```

三个乘数的语义：

| 因子 | 来源 | 含义 |
|------|------|------|
| `priority` | ConfigMap 的 `priority`（默认 1.0） | 运维声明的模型重要性 |
| `Score_i` | ConfigMap 的 `analyzers[].score`（默认 1.0） | 该 analyzer 的话语权权重 |
| `pickerState[i][role]` | 剩余 RC（**每轮递减**） | 还差多少 |

**【逻辑】** `fsv` 会随分配推进**下降**（第三个因子在减），所以这是一个"每轮重排序、优先满足当前最饥饿的模型"的迭代过程 —— 典型的 mean-based fair-share，只不过计量单位是 token 而不是 CPU/内存。

`Score = 0` 或 `priority = 0` 时的 fallback（退回"各角色最大剩余需求"）是为了避免一个配置失误导致该模型 `fsv = 0`、永远排在最后拿不到卡。

### 5.2 GPU 预算的合并：取最紧的

```go
// internal/engines/pipeline/cost_aware_optimizer.go
func mergeConstraints(constraints []*ResourceConstraints) map[string]int {
	merged := make(map[string]int)
	for _, c := range constraints {
		if c == nil { continue }
		for accType, pool := range c.Pools {
			if pool.Limit < 0 {
				// Unlimited sentinel: no finite cap. Represent it as an
				// unbounded budget (math.MaxInt) so the optimizer allocates the
				// type up to the model's fair-share demand. Leaving it absent
				// would let fairShareRolePick read a 0 budget and silently deny
				// the type — inverting the -1 = unlimited semantic.
				if _, ok := merged[accType]; !ok {
					merged[accType] = math.MaxInt
				}
				continue
			}
			if existing, ok := merged[accType]; !ok || pool.Available() < existing {
				merged[accType] = pool.Available()
			}
		}
	}
	return merged
}
```

**【逻辑】** 又是一个"缺失 ≠ 零"的例子：`Limit < 0` 是"无限"哨兵值，必须显式翻译成 `math.MaxInt` 写进 map。如果偷懒不写（让它缺失），下游 `fairShareRolePick` 读到 0 预算，就会把"无限"反转成"拒绝一切" —— 语义完全颠倒。

namespace 维度的合并更微妙（`cost_aware_optimizer.go`）：

```go
// A namespace present in any provider's NamespacePools is materialized in the
// result even when its inner map is empty — its presence marks a CLOSED
// allowlist (deny-all when empty) that the optimizer enforces by allocating
// only the listed types.
```

**"present 但为空 = 封闭白名单（拒绝一切）"** 与 **"absent = 不受此维度约束"** 是两个完全不同的状态，靠 map 里有没有这个 key 来区分。

### 5.3 两种 Limiter

```go
// internal/engines/pipeline/limiter_interfaces.go:1-30（设计说明）
// The limiter interfaces separate two orthogonal concerns:
//
//  1. Inventory (Granularity): How resources are tracked and what constraints apply.
//     Examples: cluster-wide pool, per-accelerator-type limits, node-level with scheduling.
//
//  2. AllocationAlgorithm (Strategy): How resources are distributed across decisions.
//     Examples: greedy by saturation, round-robin, priority-based, weighted fair share.
//
//	Limiter (public API)
//	   │
//	   ├── Inventory (resource granularity)
//	   │      └── creates ResourceAllocator
//	   │
//	   └── AllocationAlgorithm (distribution strategy)
//	          └── uses ResourceAllocator
```

| 模式 | Inventory | 数据来源 | 需要 Node 读权限 |
|------|-----------|---------|-----------------|
| `gpu-inventory`（默认） | `TypeInventory` | **物理发现**：List Node + GPU operator label | ✅ |
| `quota` | `QuotaInventory` | **运维声明的静态配额** | ❌ |

`QuotaInventory` 的设计（`quota_inventory.go:16-28`）：

```go
// QuotaInventory enforces operator-declared GPU quotas independent of physical
// cluster inventory. ... Each instance is tied to exactly one QuotaLimiterConfig
// scope — a deployment that needs both cluster and namespace caps composes two
// QuotaInventory instances via the limiter chain (sub-issue #1003).
//
// Quota values are static; QuotaInventory.Refresh is a no-op because there's
// no external source to discover.
//
// QuotaInventory is decoupled from physical inventory. The limiter chain
// composes a QuotaInventory with TypeInventory so allocations are bounded by
// min(physical, quota); used standalone, no physical bound is enforced.
```

配置形态（内联在 saturation ConfigMap 的 `default` 条目下）：

```yaml
default: |
  enableLimiter: true          # ← 必须同时打开，否则 limiter 被构造但从不被咨询
  limiters:
    - name: cluster-quota
      type: quota
      scope: cluster
      quotas:
        H100: 16
        A100: 8
        L40S: -1               # -1 = 无限
    - name: ns-quota
      type: quota
      scope: namespace
      namespaceQuotas:
        llm-d:      { A100: 4, L40S: 8 }
        llm-d-dev:  { L40S: 2 }
      exclude: [ kube-system ]
```

⚠️ **两个字段必须成对打开**，这是最容易踩的坑，`main.go:492-501` 专门打了一条启动日志警告：

```go
		if cfg.EffectiveLimiterMode() == config.LimiterTypeQuota {
			setupLog.Info("Quota limiter selected; quota caps are enforced ONLY when " +
				"enableLimiter: true is set in the saturation-scaling ConfigMap. " +
				"With the default enableLimiter: false the engine runs the unlimited " +
				"optimizer and quota caps are not applied.")
		}
```

**quota 模式还会连带关闭物理容量发现**（`engine.go`）：

```go
	// Collect accelerator inventory (only in limited mode AND only when the
	// inventory-based limiter is active). In quota mode (an inline limiters: quota
	// entry), the controller deliberately runs without consulting physical capacity —
	// listing Nodes here would defeat that contract (and trigger a
	// controller-runtime Node informer for the lifetime of the process).
	if e.Config.LimitedModeEnabled() && shouldCollectClusterInventory(e.Config) {
		inventory, err := collector.CollectInventoryK8S(ctx, e.client)
		...
	}
```

**【逻辑】** quota 模式的核心承诺是"**不需要 Node 读权限**" —— 这对多租户/受限 RBAC 环境很关键。如果这里顺手 List 一下 Node，不但违背承诺，还会让 controller-runtime 为 Node **建一个进程生命周期内都存在的 informer**（内存开销可观，大集群上 Node 对象不小）。这种"一个看似无害的 List 调用带来长期 informer"的坑，在 controller-runtime 里非常常见。

### 5.4 限流器热重建

```go
// internal/engines/saturation/engine.go
func (e *Engine) refreshLimiter(ctx context.Context) {
	e.limiterMu.Lock()
	defer e.limiterMu.Unlock()
	if e.limiterBuilder == nil { return }
	sig := limiterSignature(e.Config)
	if sig == e.limiterSig && e.GPULimiter != nil { return }
	limiter, err := e.limiterBuilder()
	if err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "failed to rebuild GPU limiter from config; keeping the previous limiter",
			"effectiveMode", e.Config.EffectiveLimiterMode())
		return
	}
	e.GPULimiter = limiter
	e.limiterSig = sig
	...
}
```

```go
// internal/engines/saturation/engine.go
// limiterSignature is a deterministic fingerprint of the config inputs that
// determine the GPU limiter, used to detect when a rebuild is needed. Quota
// entry maps are marshaled with sorted keys by encoding/json, so equal configs
// always produce equal signatures.
func limiterSignature(cfg *config.Config) string {
	entries, _ := json.Marshal(cfg.EffectiveQuotaEntries())
	return string(cfg.EffectiveLimiterMode()) + "|" + string(entries)
}
```

**【Go】** 靠 `encoding/json` 对 map 序列化时**按 key 排序**的特性来做确定性指纹 —— 这是 Go 标准库的一个明确保证（`json.Marshal` 对 `map[string]T` 排序输出），可以放心依赖。用 `fmt.Sprintf("%v", map)` 就不行，那个顺序是随机的。

构建失败时**保留旧 limiter** 而不是清空 —— 一个手滑写坏的 ConfigMap 不会让引擎处于"无限流器"状态。

## 6. Rescale：优先级加权的水填充（Alpha）

`internal/engines/pipeline/rescale.go`（682 行），`enableRescale: true` 开启，**只在 `GreedyByScoreOptimizer` 路径上生效**。

### 6.1 它解决什么

普通 fair-share 是**增量式**的：只分配"当前空闲"的 GPU。如果 GPU 已经全被低优先级模型占满，高优先级模型来了也只能干等。

Rescale 是**重分配式**的：在竞争条件下把**整个预算**按 `priority × demand` 重新划分，**从低优先级模型手里回收**。

```go
// internal/engines/pipeline/greedy_score_optimizer.go
	// Rescale pre-pass: for enabled, contended (type, budget-scope) groups, compute
	// priority-weighted targets and produce reclaim/fill decisions, consuming free
	// GPUs from `available`/`availableByNS` so the additive path below sees the
	// reduced budget. Models it handles are excluded from the additive path. When
	// rescale is off or no group is contended, `handled` is empty and behaviour is
	// unchanged.
	if o.Rescale.any() {
		rescaleDecisions, handled = o.applyRescale(ctx, requests, available, availableByNS)
	}
```

**【设计】** "pre-pass + handled 集合 + 消耗掉预算"这个结构值得注意：rescale 处理过的模型**从后续 additive 路径中排除**，且它消耗掉的空闲 GPU 从 `available` 里扣掉 —— 两条路径共享同一份预算账本，不会重复分配。rescale 关闭或无竞争时 `handled` 为空，行为**完全不变**（零风险的 feature flag 设计）。

### 6.2 水填充算法

```go
// internal/engines/pipeline/rescale.go（computeRescaleTargets 的文档注释）
// computeRescaleTargets distributes `budget` GPUs across models by priority-weighted
// water-filling, returning each model's target GPU allocation.
//
//	target_i = floor_i + (budget - Sum floor) * weight_i / Sum weight     (weight_i = priority_i * demand_i)
//
// A model wanting less than its share is capped at CapGPUs_i and the freed excess
// re-splits over the still-hungry models by the same weights, until none exceeds its
// cap. Floors are always reserved first; when Sum floor > budget the floors cannot be
// met — every model is clamped to its floor and overBudget is true (a Conflict the
// caller surfaces). Fractional shares are rounded to whole GPUs by the largest-remainder
// method, never exceeding a model's cap, so the returned targets sum to at most `budget`.
```

```go
type rescaleInput struct {
	ID        string
	Priority  float64 // model priority multiplier (>= 0)
	Demand    float64 // model demand for the weight (any unit; ratio only)
	FloorGPUs int     // reserved: sum over variants of minReplicas x gpusPerReplica
	CapGPUs   int     // upper bound: min(demand-in-GPUs, maxReplicas x gpusPerReplica); >= FloorGPUs
}
```

算法四步：

1. **先预留所有 floor**（`Σ minReplicas × gpusPerReplica`）；`Σ floor > budget` 时全部夹到 floor 并置 `overBudget = true`（调用方上报为 Conflict）。
2. 剩余 `pool = budget − Σ floor` 按 `weight_i = priority_i × demand_i` 比例分。
3. **迭代水填充**：某模型分到的超过它的 `CapGPUs`（它要不了那么多）→ 夹到 cap，把富余按同样权重**重新分给还饿的模型**，直到没有人超 cap。
4. 分数份额用**最大余额法**（largest-remainder）取整成整数 GPU，且**永不超过任何模型的 cap**，保证总和 ≤ budget。

**【设计】** `Demand` 字段的注释很有意思：

> `Demand` 用 analyzer 自己的单位，只出现在权重比值（priority × demand）里，**所以它的单位会被约掉**。

这让 rescale 可以在 saturation（token）和 throughput（token/s）之间通用 —— 因为只用相对比例，不用绝对值。

**最大余额法**（而不是简单 floor 或 round）保证了两件事：总和精确等于可分配量（不丢卡也不超发），且分配对小份额模型相对公平。这与选举里的席位分配是同一个算法（Hare quota）。

### 6.3 作用域耦合

```go
// internal/engines/pipeline/rescale.go
func (f RescaleFlags) enabledForScope(namespace string, namespaceScoped bool) bool {
	if namespaceScoped {
		return f.ByNamespace[namespace]
	}
	return f.Cluster
}
```

```go
// internal/engines/saturation/engine.go
// resolveRescaleFlags builds the scope-coupled rescale enablement for this cycle:
// the cluster flag from the global saturation `default` config, plus a per-namespace
// flag from each active namespace's OWN `default` config (never the global fallback,
// so the cluster flag cannot enable rescale on a namespace quota).
func (e *Engine) resolveRescaleFlags(requests []pipeline.ModelScalingRequest) pipeline.RescaleFlags {
	flags := pipeline.RescaleFlags{Cluster: e.Config.RescaleEnabledCluster()}
	seen := make(map[string]bool)
	for _, req := range requests {
		ns := req.Namespace
		if ns == "" || seen[ns] { continue }
		seen[ns] = true
		if enabled, hasLocal := e.Config.RescaleEnabledForNamespaceLocal(ns); hasLocal && enabled {
			if flags.ByNamespace == nil { flags.ByNamespace = make(map[string]bool) }
			flags.ByNamespace[ns] = true
		}
	}
	return flags
}
```

**【逻辑】** 关键约束：**namespace 级的 rescale 只能由该 namespace 自己的 `default` 配置开启，绝不从全局配置继承**。

为什么？因为 rescale 是**回收资源**的动作。如果集群管理员在全局开了 rescale，就会隐式地让每个 namespace 的配额内部也开始互相抢占 —— 而 namespace 的所有者可能并不知情、也没同意。把两个作用域的开关**解耦**，是一个明确的权限边界设计。

同理 `enableRescale` 和 `enableLimiter`、`limiters` 一样都是**budget-scope 标志**，只从 `default` 条目读，per-model override 里写了也无效（`saturation_scaling.go:36-46`）。

## 7. Enforcer：最后的安全网

```go
// internal/engines/pipeline/enforcer.go:23-51
// Enforcer applies scale-to-zero and minimum replica enforcement after saturation analysis.
type Enforcer struct {
	requestCountFunc RequestCountFuncType
	metricsEmitter   *metrics.MetricsEmitter
}

// EnforcePolicyOnDecisions applies scale-to-zero and minimum replica enforcement
// directly on VariantDecision slices. It operates on decisions in-place.
//
// Returns true if scale-to-zero was applied (all variants scaled to zero).
func (e *Enforcer) EnforcePolicyOnDecisions(
	ctx context.Context, modelID string, namespace string,
	decisions []domain.VariantDecision,
	scaleToZeroConfig config.ScaleToZeroConfigData,
	satConfig *config.SaturationScalingConfig,
	optimizerName string,
) bool {
```

它跑在优化器**之后**（`engine.go`）：

```go
	// Stage 3: Apply enforcer per-model (directly on decisions)
	for _, req := range requests {
		e.applyScaleToZeroEnforcement(
			ctx, req.ModelID, req.Namespace, optimizer.Name(),
			allDecisions,
			modelScaleTargets[utils.GetNamespacedKey(req.Namespace, req.ModelID)],
			req.VariantStates,
		)
	}
```

职责：
1. **scale-to-zero**：模型在保留期内请求数为 0（`requestCountFunc` 查 `vllm:request_success_total`）→ 把所有变体压到 0，**覆盖 cost-aware 的 cheapest-at-1 保护**。
2. **minReplicas 兜底**：确保没有决策低于 `minReplicas`。
3. 每次修正都记 `wva_enforcer_modifications_total`。

scale-to-zero 的三级开关（`config.ResolveScaleToZeroEnabled`）：

| 优先级 | 来源 |
|-------|------|
| 1 | saturation 条目内联的 `scaleToZero.enabled`（指针类型，nil = 继承） |
| 2 | 独立的 `wva-model-scale-to-zero-config` ConfigMap（按 model/namespace 匹配） |
| 3 | `WVA_SCALE_TO_ZERO` 环境变量 |

引擎还会检查 scale target 是否支持（`engine.go` 的 `scaleToZeroSupportedForEngines`）以及是否有变体的 `minReplicas > 0`（`hasMinReplicasAboveZero`）—— 后者会直接否掉整个模型的 scale-to-zero。

## 8. 决策的输出

`buildDecisionsWithOptimizer`（`cost_aware_optimizer.go`）把 `targets` map 变成 `[]VariantDecision`：

```go
		var action domain.SaturationAction
		switch {
		case target > state.CurrentReplicas:
			action = domain.ActionScaleUp
			...
		case target < state.CurrentReplicas:
			action = domain.ActionScaleDown
			...
		default:
			action = domain.ActionNoChange
			...
		}
```

顺带把观测字段填上（`cost_aware_optimizer.go`）：

```go
		// Observability fields consumed by RecordSaturationMetrics
		// (wva_saturation_utilization / wva_required_capacity / wva_spare_capacity).
		// Without these the three V2 gauges read zero. ...
		// For P/D-disaggregated models use the variant's per-role capacity; otherwise
		// fall back to the model-level totals.
		decision.Utilization = vc.Utilization
		if anchor != nil {
			reqCap, spareCap := anchor.RequiredCapacity, anchor.SpareCapacity
			role := state.Role
			if role == "" { role = domain.RoleBoth }
			if rc, ok := anchor.RoleCapacities[role]; ok {
				reqCap, spareCap = rc.RequiredCapacity, rc.SpareCapacity
			}
			decision.RequiredCapacity = reqCap
			decision.SpareCapacity = spareCap
		}
```

这里的 PRC、利用率、RC/SC 来自 `bindingAnchor`；saturation 不投票时可能使用 throughput 的 token/s，不能一律当作 KV token。

**【逻辑】** P/D 分离时 `wva_required_capacity` 报的是**该变体所属角色的** RC，不是模型级的 —— 否则 prefill 和 decode 两个变体会报同一个数字，无法分别看瓶颈在哪。

注释还提到 `SpareCapacity` 同时是 GPU limiter 的 `GreedyBySaturation` 排序输入 —— 在这里填上它，那个排序才能反映真实的富余 token 而不是恒为 0。

### 8.1 最终 emit

`applySaturationDecisions`（`engine.go`）遍历**所有活跃 VA**（不只是有决策的），确保每个变体每周期都刷新指标 —— 让 HPA 那条 External Metric 不断线：

```go
// internal/engines/saturation/engine.go
		// Emit metrics for external autoscalers (Important: Actuator emits these)
		// We should emit metrics even if no decision changed, to keep HPA alive
		act := actuator.NewActuator(e.client)
```

**【逻辑】** 这一点很关键：如果没决策就不 emit，Prometheus 里的序列会变陈旧，HPA 拿不到 External Metric 值 → 报 `FailedGetExternalMetric` → **停止一切扩缩容**（HPA 在指标不可用时保守不动）。所以"哪怕没变化也要发一遍"是维持反馈环的必要条件。

在 emit 之前，决策被先"暂存"到内存态 VA 的 status 上（`engine.go`）：

```go
		// Stage the just-computed decision on the in-memory VA so that
		// act.EmitMetrics below — which reads
		// Status.DesiredOptimizedAlloc.{NumReplicas,Accelerator}
		// (see actuator.EmitMetrics) — publishes the fresh target rather than
		// whatever was last persisted by the controller.
		numReplicas := int32(targetReplicas)
		statusAccelerator := acceleratorName
		if !constants.IsAcceleratorResolved(statusAccelerator) {
			statusAccelerator = ""
		}
		updateVa.Status.DesiredOptimizedAlloc = llmdVariantAutoscalingV1alpha1.OptimizedAlloc{
			NumReplicas: &numReplicas,
			Accelerator: statusAccelerator,
			LastRunTime: metav1.Now(),
		}
```

**加速器无法解析时的处理**（`engine.go`）：

```go
		// Emit a K8s event when accelerator cannot be resolved so operators
		// can see the problem without digging through controller logs.
		// The message is a constant string (not built per-cycle via Eventf with
		// formatted args), so each emission produces an identical
		// (involvedObject, source, type, reason, message) tuple — which the K8s
		// API server's event aggregator collapses into a single Event entry with
		// an updated count, rather than creating a new entry each optimization
		// cycle.
		if !constants.IsAcceleratorResolved(acceleratorName) {
			e.emitAcceleratorNotResolvedEvent(&updateVa)
```

**【K8s 技巧】** 这段注释讲了一个实用技巧：**K8s Event 的聚合是靠 `(involvedObject, source, type, reason, message)` 五元组完全相同来触发的**。用常量字符串而不是 `Eventf` 带格式化参数，就能让 15s 一次的周期性 Event 被折叠成一条带 count 的记录，而不是刷爆 etcd。这是写 Operator 时非常值得记住的一条。

## 9. 完整决策链路（限流场景数值演算）

在 01 篇的例子上加压：假设集群 `enableLimiter: true`，L40S 配额只剩 **0 张**，A100 剩 **2 张**。同时有第二个模型 D 也要扩。

```
输入：
  模型 M (priority 1.0)：RC = 60000 token
      V1 llama-8b-a100  cost 10, PRC 90000, 1 GPU/副本, 当前 2, max 5
      V2 llama-8b-l40s  cost  4, PRC 60000, 1 GPU/副本, 当前 0, max 5
  模型 D (priority 3.0)：RC = 40000 token
      V3 llama-70b-decode cost 20, PRC 25000, 2 GPU/副本, 当前 1, max 4

预算：A100 = 2, L40S = 0

Step 1 · fair-share 值
  fsv(M) = 1.0 × (1.0 × 60000) = 60000
  fsv(D) = 3.0 × (1.0 × 40000) = 120000    ← D 优先

Step 2 · D 先分
  V3 需要 ceil(40000/25000) = 2 副本 = 4 GPU
  A100 预算只有 2 → 只够 1 副本（2 GPU）
  提交 V3: 1 → 2，A100 预算 2 → 0
  D 的 Remaining: 40000 − 25000 = 15000（未满足，但没预算了）

Step 3 · M 再分
  按 cost 效率 V2(6.67e-5) < V1(1.111e-4)，先看 V2
  → L40S 预算 = 0，拿不到卡，跳过
  → 看 V1（A100）
  → A100 预算已被 D 用光 = 0，也拿不到
  提交 M: 无变化，RC 保留 60000

Step 4 · 观测输出
  wva_required_capacity{variant_name="llama-8b-a100"} = 60000    ← 仍在告警
  wva_decisions_limited_total{limiter_name="cluster-quota"} += 2
  wva_available_gpus{accelerator_type="A100"} = 0
```

**结论**：模型 M **被饿死了**，因为 D 优先级更高（3.0 vs 1.0）且先把 A100 吃光。这是 additive fair-share 的正常行为。

**如果打开 `enableRescale: true`**，水填充会重新划分整个 A100 预算（假设总共 4 张，D 当前占 2、M 当前占 2）：

```
weight(M) = 1.0 × 60000 = 60000
weight(D) = 3.0 × 40000 = 120000
Σ weight  = 180000

floor(M) = minReplicas(1) × 1 GPU = 1
floor(D) = minReplicas(1) × 2 GPU = 2
Σ floor  = 3, budget = 4, pool = 1

target(M) = 1 + 1 × 60000/180000 = 1.33
target(D) = 2 + 1 × 120000/180000 = 2.67
最大余额法取整 → M: 1, D: 3   （D 的余数 0.67 > M 的 0.33）

⇒ 从 M 回收 1 张 A100（2 → 1 副本），给 D（2 → 3 GPU）
```

M 被**主动降级**了 —— 这正是 rescale 的意图：高优先级模型的 SLO 优先于低优先级模型的容量。但注意 M 的 floor（minReplicas）永远被保留，不会被压到 0。

## 10. 本篇速查表

### 关键公式

| 公式 | 位置 |
|------|------|
| `costEfficiency = Cost / PerReplicaCapacity`（升序挑扩容目标） | `cost_aware_optimizer.go` |
| 缩容排序：`Cost desc → Σ Score_i·PRC_i asc → name asc` | `cost_aware_optimizer.go` |
| `n_up = max_i ceil(Remaining_i / PRC_i)` | `analyzer_helpers.go` |
| `n_down = min_i floor(RoleSpare_i / PRC_i)` | `analyzer_helpers.go` |
| `util_role = n_role × PRC_role / demand_role`；`Δutil = min_role util_role` | `analyzer_helpers.go` |
| `fsv = priority × Σ_i Score_i × Σ_role remaining[i][role]` | `greedy_score_optimizer.go` |
| `target_i = floor_i + (budget − Σfloor) × w_i/Σw`，`w_i = priority_i × demand_i` | `rescale.go` |
| 预算合并取 `min(available)`；`Limit < 0` → `math.MaxInt` | `cost_aware_optimizer.go` |

### 必记要点

1. **`enableLimiter` 决定用哪个优化器**，默认 false → cost-aware（unlimited，忽略一切 GPU 约束）。
2. **约束"拿不到"退回 unlimited，约束"报 0"继续阻塞** —— 区分"不知道"与"知道是零"。
3. **`limiters:` 与 `enableLimiter: true` 必须成对配置**，否则限流器被构造但永不被咨询。
4. **quota 模式刻意不读 Node** —— 不只是为了 RBAC，也为了不建一个进程级的 Node informer。
5. **非 P/D 模型走合成的 `"both"` 角色**，与多角色共用一条代码路径。
6. **一个模型一周期内只扩或只缩**，由 `anyRoleNeedsScaleUp` 分流。
7. **P/D 的 Δ_util 联合提交**是 WVA 相对 HPA 的核心能力差：按瓶颈角色的可满足比例等比裁剪，避免单侧超扩。
8. **优化器改的是工作副本 `Remaining`/`Spare`，绝不改 `Result.RequiredCapacity`** —— 保住观测性。
9. **cheapest-at-1 是位置性规则**：只有所有更贵变体清零后，最便宜的那个才被保护在 1。
10. **rescale 的 namespace 开关不从全局继承** —— 回收资源必须由资源所有者显式同意。
11. **rescale 用最大余额法取整**，保证总和 ≤ budget 且不超任何模型的 cap。
12. **没有决策变化也要 emit 指标**，否则 HPA 的 External Metric 断线会停掉一切扩缩容。
13. **周期性 K8s Event 用常量 message**，靠五元组相同触发 API server 的事件聚合。

---

**下一篇**：[04 · 面向大模型推理的能力地图](04-面向大模型推理的能力地图.md)
