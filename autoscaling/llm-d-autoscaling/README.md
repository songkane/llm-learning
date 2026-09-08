# llm-d WVA 源码学习（Workload Variant Autoscaler）

> **源码基线**：[`release-0.9 @ d5d5864`](https://github.com/llm-d/llm-d-autoscaling/tree/d5d586408420fbe0545f827a6ff5dc2f818b16de)（2026-08-07；2026-09-07 核对）。分支与 `v0.9.0` tag 相差 3 个提交，详见 [版本对照](README.md#版本对照与复现)。
> 本地对照：`sources/llm-d-autoscaling`（`./scripts/sync-sources.sh llm-d-autoscaling`）
> Go module 名为 `github.com/llm-d/llm-d-workload-variant-autoscaler`，与仓库名不同；代码里统一用 `wva` 缩写。

## 版本对照与复现

本系列按 **`release-0.9` 分支**分析，固定到核对时的提交 `d5d586408420fbe0545f827a6ff5dc2f818b16de`。本基线 `go.mod` 为 **Go 1.25.0**，共有 1840 个可达提交，`internal/` + `cmd/` 非测试 Go 源码 29333 行。

| Git 引用 | 提交 | 与本文关系 |
|----------|------|------------|
| `v0.9.0` tag | `aadaa59` | 旧基线；不能复现本文全部行为 |
| `release-0.9` | `d5d5864` | 本文基线，比 tag 多 3 个提交 |
| `main` | 比基线多 5 个提交 | **未触碰 `internal/engines`**，本文的机制分析在 main 上同样成立 |

这 3 个提交分别是 `8b3663e`（throughput 分支测试）、`57f3fe6`（按需容量来源、analyzer enablement、QM 拒绝、零副本容量复用）和 `d5d5864`（release 准备）。这是 Git 引用的实际差异，不把分支 HEAD 与发布 tag 混称为同一版本。[上游差异](https://github.com/llm-d/llm-d-autoscaling/compare/aadaa5964301f2417c85290c6cea81f991b36348...d5d586408420fbe0545f827a6ff5dc2f818b16de)

`release-0.9` 分支自 2026-08-07 起未再前移，是个稳定基线。`main` 上多出的 5 个提交是 Go 1.26 升级、base image 修复、benchmark 场景示例与 e2e 测试 —— `git diff d5d5864..origin/main -- internal/engines` 为空，扩缩容逻辑本身没有变化。

```bash
./scripts/sync-sources.sh llm-d-autoscaling
cd sources/llm-d-autoscaling
git switch release-0.9
git rev-parse HEAD       # 本文：d5d586408420fbe0545f827a6ff5dc2f818b16de
git diff v0.9.0..d5d5864 -- internal/engines
# 若分支后续前移，需要逐行复现本文时：
# git switch --detach d5d586408420fbe0545f827a6ff5dc2f818b16de
```

同步脚本会跳过已存在的仓库；已有检出不会自动切换。文中的 Go 片段包含省略与教学注释，以本提交中的对应函数为准。

### 本次基线差异的源码入口

| 变化 | 固定提交中的入口 |
|------|----------------|
| 启用与投票集合 | [runAnalyzersAndScore](https://github.com/llm-d/llm-d-autoscaling/blob/d5d586408420fbe0545f827a6ff5dc2f818b16de/internal/engines/saturation/engine_v2.go#L100) |
| 容量来源与身份合并 | [bindingAnchor / votingResults](https://github.com/llm-d/llm-d-autoscaling/blob/d5d586408420fbe0545f827a6ff5dc2f818b16de/internal/engines/pipeline/analyzer_helpers.go#L124) |
| 零副本历史容量 | [T-sfz 补充逻辑](https://github.com/llm-d/llm-d-autoscaling/blob/d5d586408420fbe0545f827a6ff5dc2f818b16de/internal/engines/analyzers/throughput/analyzer.go#L385) |
| QM 拒绝入口 | [refuseQueueingModel](https://github.com/llm-d/llm-d-autoscaling/blob/d5d586408420fbe0545f827a6ff5dc2f818b16de/internal/engines/saturation/engine_queueing_model.go#L38) |
| 保持副本与持续 emit | [applySaturationDecisions](https://github.com/llm-d/llm-d-autoscaling/blob/d5d586408420fbe0545f827a6ff5dc2f818b16de/internal/engines/saturation/engine.go#L1601) |
| 预算再分配 | [rescaleModelDecisions](https://github.com/llm-d/llm-d-autoscaling/blob/d5d586408420fbe0545f827a6ff5dc2f818b16de/internal/engines/pipeline/rescale.go#L335) |

## 一句话定位

> **WVA 是给 HPA/KEDA 喂目标副本数的「全局大脑」。**
> 它工作在 **scale 层**：把「整个集群的 容量供需 + GPU 预算 + 成本」算成一个 `wva_desired_replicas` 指标，HPA/KEDA 读它去改 `spec.replicas`。

| | 谁来做 | 写什么 |
|---|-------|-------|
| 副本数**应该**是多少、加在哪个变体上 | **WVA** | 只 emit `wva_desired_replicas`，从不直接改 replicas（唯一例外：scale-from-zero） |
| 副本数**实际**改成多少 | HPA / KEDA | `deployment.spec.replicas` |

和 HPA 的区别：**HPA 是"一个指标 → 一个 Deployment"的局部反馈环；WVA 是"所有变体一起看"的全局优化器。**

## 学习路线

沿用统一示例贯穿全篇：

```
集群：node-a（8×A100）、node-b（8×L40S），命名空间 llm-d
模型 M = llama-8b
  ├── 变体 V1 = llama-8b-a100 （cost 10.0，PRC 90000 token）
  └── 变体 V2 = llama-8b-l40s （cost  4.0，PRC 60000 token）
模型 D = llama-70b（P/D 分离）
  ├── 变体 P = llama-70b-prefill （role=prefill）
  └── 变体 Q = llama-70b-decode  （role=decode）
每个变体一个 HPA，打 llm-d.ai/managed: "true"
```

| 篇 | 主题 | 核心问题 |
|----|------|---------|
| [00](00-WVA总览与架构.md) | 总览与架构 | 什么是"变体"、**变体从注解合成为内存对象**、Reconciler 不做决策、三个 leader-only 轮询循环、Coordinator、V1/V2 与 QM 拒绝路径 |
| [01](01-核心原理-轮询引擎与双阈值容量模型.md) | **双阈值容量模型** | `applyUniversalThreshold` 那 8 行公式讲到能手算；扩容/缩容的刻意不对称；多 analyzer 的 any-up/all-down 与 liveness 门；**完整数值演算** |
| [02](02-核心代码分析-指标采集与Analyzer.md) | 采集与 Analyzer | 16 条查询分四批发出、完整清单与聚合函数辨析（vLLM/SGLang 双后端）；scale-from-zero 直抓 EPP 这个例外；`wva_*` 的 label 集差异；pod→变体映射的 label 快路径与 owner-walk 回退；**k2 的四级优先级链**；saturation V2 / throughput；queueing-model 保留实现但禁止调度 |
| [03](03-核心代码分析-Optimizer与Limiter.md) | Optimizer 与 Limiter | cost-aware vs greedy-by-score；**bindingAnchor 容量来源与 P/D 的 Δ_util 联合提交**；quota/inventory 两种限流器；**rescale 的优先级水填充**；Enforcer |
| [04](04-面向大模型推理的能力地图.md) | 能力地图 | 能做什么/做不到什么；三种落地形态；**坑清单 + 上线检查清单** |
| [05](05-实战Demo.md) | 实战 Demo | kind + 模拟 GPU 跑通全链路；9 个 Demo（含 QM 拒绝与恢复验证）逐个核对机制；排障决策树 |
| 📄 [`wva-autoscaling-logic.html`](wva-autoscaling-logic.html) | **计算逻辑可视化速查** | 公式字典、指标来源表、数值演算 Demo、风险清单。浏览器直接打开 |

**建议顺序**：00 → 01（必读，公式是全篇基础）→ 打开 html 速查一遍 → 按需读 02/03 → 04 决定用不用 → 05 动手。

## 四个影响代码走向的关键设计

### 1. 变体曾经是 CRD，现在是内存对象

WVA 的决策单元 `VariantAutoscaling` **曾经是真的 CRD**（`llmd.ai/v1alpha1`），当前版本已移除，改为每周期从**带 `llm-d.ai/managed` 注解的 HPA / KEDA ScaledObject 现场合成**，只存在于内存。

```go
// internal/variant/types.go:4-8
// It was previously the llmd.ai/v1alpha1 VariantAutoscaling CRD API. The CRD has
// been removed; discovery now happens by synthesizing these structs from annotated
// HPAs and KEDA ScaledObjects ... never registered in a scheme or written to the
// Kubernetes API server.
```

**这直接决定了哪些资料能信**：仓库自己的 `metrics-health-monitoring.md` 还在教 `kubectl get variantautoscaling`、`sglang-backend.md` 还在给 `kind: VariantAutoscaling` 的 YAML —— 都已失效。**看到让你 apply `VariantAutoscaling` 对象的资料，就是过时的。**

排障影响：VA 没有可查询的 CRD status；查看控制器日志、`wva_*` 指标和 Kubernetes Events。`OptimizationReady` 等 condition 只写到临时对象，不能当作持久状态查询。

### 2. Reconciler 不做扩缩容决策

四个 Reconciler（HPA / ScaledObject / InferencePool / ConfigMap）只做"发现 namespace"和"配置热更新"。真正的决策在 `mgr.Add(RunnableFunc)` 注册的三个 leader-only 轮询循环里：

| 引擎 | 间隔 | 作用 |
|------|------|------|
| `saturation.Engine` | 15s（`GLOBAL_OPT_INTERVAL`） | 主扩缩容引擎 |
| `scalefromzero.Engine` | **100ms** | 0→1 冷启动，**直接改 replicas** |
| `coordinator` | 15s（默认关闭） | 改 `hpa.spec.maxReplicas` 天花板 |

### 3. 启用、存活和容量来源是三个不同判断

`Enabled` 决定是否参与 RC/SC 投票，`Live` 决定是否有近期可用容量信号，`bindingAnchor` 决定优化器使用谁的容量。saturation 始终运行以提供完整变体身份；从显式 `analyzers` 列表移除它或设 `enabled: false` 后，它不再投票，也不能否决缩容。throughput-only 是受支持的配置，但没有可用容量时保持副本，真正的冷启动由 scale-from-zero 引擎处理。

**queueing-model 当前不可启用**：带 `default` 的 QM 配置仍有最高选路优先级，但随后进入 `refuseQueueingModel`，记录错误及 `OptimizationRefused` Warning，保持目标副本并继续发布指标。不要按旧资料部署 QM ConfigMap 来开启 SLO 扩缩容。

### 4. 决策收敛到两行公式

```
RC（要扩多少）= max(0, TotalDemand / scaleUpThreshold  − TotalAnticipatedSupply)   # 默认 0.85
SC（能缩多少）= max(0, TotalSupply  − TotalDemand / scaleDownBoundary)             # 默认 0.70
```

0.70 ~ 0.85 之间是**滞回带**（不靠时间窗口压抖动）；**扩容用含 pending 的 anticipated、缩容用不含 pending 的 supply** —— 刻意的不对称，两端都偏保守，分别防"级联重复扩容"和"刚扩就缩"。

## WVA 相对 HPA 的四个实质能力差

| 能力 | HPA 做不到的原因 |
|------|-----------------|
| **按 KV token 衡量负载** | CPU/内存对 LLM 无意义（vLLM 启动就把显存占满，CPU 恒定 10%） |
| **异构机型成本选择** | HPA 一次只看一个 Deployment，看不到同模型还有更便宜的变体 |
| **P/D 联合扩容** | 两个独立 HPA 各看自己指标，必然扩成不匹配的比例 |
| **0 副本唤醒** | 0 副本时没有 pod 暴露指标，HPA 的反馈环是断的 |

第三项的算法（03 篇 §4）：按各角色的"可满足比例" `util_role = n_role × PRC_role / demand_role` 取最小值作为 `Δutil`，再等比裁剪两侧的提交量 —— 保证 prefill 不超扩。

## 最容易踩的配置坑

| 坑 | 症状 | 修法 |
|----|------|------|
| QM 配置含 `default` | 主循环拒绝优化，指标仍存在 | 移除 QM 配置的 `default`，恢复 V2 配置 |
| throughput-only | saturation 日志仍出现，但已无缩容否决权 | 明确检查 `analyzers` 列表与 EPP 到达率 |
| `limiters:` 配了但 `enableLimiter: false` | 配额完全不生效 | 两个都设（启动日志有 Info 警告） |
| 新启用启动时未注册的 throughput | 不生效 | 重启（ConfigMap 上有 Warning Event `ThroughputAnalyzerRestartRequired`） |
| 阈值倒挂（`up ≤ down`） | **整对静默退回 0.85/0.70**，无 warning | 去 `analyzer-result` 日志确认实际值 |

## 排障：优先看这条日志

```bash
kubectl -n workload-variant-autoscaler-system logs deploy/wva-controller-manager \
  | grep 'analyzer-result' | tail -5
```

```
analyzer-result modelID=... analyzer=saturation supply=94371 demand=104031 util=1.102
                rc=28018 sc=0 scaleUpThreshold=0.85 scaleDownBoundary=0.7
                variants=[{"name":"...","prc":94371,"role":"both","reason":"P1-obs"}]
```

一行里有：供给、需求、利用率、RC、SC、两个阈值、每变体的每副本容量与**容量来源**（`reason`）。日志不包含 `Enabled`，所以 throughput-only 下看到 saturation 日志不代表它在投票。throughput 的 `T-sfz` 表示复用零副本变体此前的容量。saturation 的 `reason` 取值指出 k2 走了哪一级：`P1-obs`（实测，最可信）/ `P2-hist` / `P3-k2` / `P4-k1` / `P0-store`（零副本）/ `no-data`（采集断了）。

## 阅读源码的三个提示

1. **注释的信息量常常大于代码**。这个仓库大量注释在主动说明「当前实现哪里有偏差、为什么现在不改」（例如 fallback 路径的单位不一致、throughput 的 SC 安全门缺失）。这些是理解设计取舍的关键，也是排障时的第一手线索。
2. **`docs/` 与代码不一致时以代码为准**。典型例子：`docs/design/modeling-optimization.md` 写着 "WVA 目前只支持 unlimited 模式，limited 是 future work"，但 `enableLimiter` + `limiters:` 已经把 limited 实现了。
3. **module 路径与仓库名不同**。仓库是 `llm-d-autoscaling`，module 是 `github.com/llm-d/llm-d-workload-variant-autoscaler`，import 路径与 `wva` 缩写都沿用后者。

```bash
cd sources/llm-d-autoscaling
git log --oneline v0.8.0..d5d5864 -- internal/engines/analyzers/saturation_v2
git diff v0.8.0..d5d5864 -- deploy/configmap-saturation-scaling.yaml
git log --oneline -- internal/engines/pipeline/rescale.go
```

---

> **相关系列**：[llm-d Router](../../scheduling/llm-d/)（决定请求发给哪个副本）｜ [kube-scheduler](../../scheduling/kube-scheduler/) / [Volcano](../../scheduling/volcano/)（决定 Pod 落在哪个节点）
> **回到** [弹性扩缩容总览](../README.md)
