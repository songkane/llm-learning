# llm-d Router 源码学习（Endpoint Picker / 推理请求路由）

> **源码基线**：[`main @ 90a28bc`](https://github.com/llm-d/llm-d-router/tree/90a28bc66f1d96f84f8f18f11dcd6ed15f34e830)（2026-09-07；2026-09-08 核对）
> 本地对照：`sources/llm-d-router`（`./scripts/sync-sources.sh llm-d-router`）
> 仓库 `llm-d/llm-d-router`，Go module `github.com/llm-d/llm-d-router`，镜像 `llm-d-router-endpoint-picker`。

## 一句话定位

> **WVA 决定「该有几个副本」，Router 决定「这个请求发给哪个副本」。**

| | 谁来做 | 输出 |
|---|-------|------|
| 副本数应该是多少 | [WVA](../autoscaling/) | `wva_desired_replicas` 指标 |
| **这个请求发给哪个 pod** | **Router (EPP)** | **ext-proc 响应里的 endpoint 地址** |

两者都在 llm-d 项目下、都不做 Pod 落位（那是 [kube-scheduler](../../../scheduling/kube-scheduler/) / [Volcano](../../../scheduling/volcano/) 的事），可以完全独立阅读。正文按**项目**放在 [`serving/llm-d/`](../)；[调度](../../../scheduling/) 与 [扩缩容](../../../autoscaling/) 只保留问题域索引。

Router 与传统 L7 负载均衡的区别在于它**懂 LLM 的成本结构**：

| 传统 LB 的假设 | LLM 推理的现实 |
|---------------|---------------|
| 请求成本大致相同 | 差几个数量级（100 token vs 100k token） |
| 后端无状态 | **KV Cache 让后端强状态化**——发给缓存过前缀的 pod 能省掉整个 prefill |
| 轮询/最少连接够用 | 要看队列深度、KV 利用率、前缀命中、P/D 角色 |
| 一个请求一个后端 | P/D 分离下一个请求要走两个（甚至三个）后端 |

## 组件构成

Router 的全部代码在 `llm-d/llm-d-router` 一个仓库里，产出三个二进制：

| 二进制 | 入口 | 跑在哪 | 干什么 |
|--------|------|--------|--------|
| `epp` | `cmd/epp/main.go` | 独立 Deployment | 应答 ext-proc，选 pod |
| `pd-sidecar` | `cmd/pd-sidecar/main.go` | decode pod 内 | 编排 P/D 两阶段调用 |
| `coordinator` | `cmd/coordinator/` | 独立 Deployment（实验） | 集中编排 E/P/D，替代 sidecar 的方向 |

`kubernetes-sigs/gateway-api-inference-extension`（GIE）只提供 `InferencePool` API 与 Endpoint Picker Protocol；EPP 的实现、KV 索引、P/D sidecar 都在本仓库，无需部署其他仓库的组件。

## 版本与复现

本系列按 **`main` 分支**分析，固定到核对时的提交 `90a28bc66f1d96f84f8f18f11dcd6ed15f34e830`（2026-09-07）。基线 `go.mod` 为 **Go 1.26.6**，`pkg/` + `cmd/` 非测试 Go 源码 **78742 行**。

`main` 会前移，本系列的行号引用会逐渐漂移。逐行复现时请检出固定提交：

```bash
./scripts/sync-sources.sh llm-d-router
cd sources/llm-d-router
git switch --detach 90a28bc66f1d96f84f8f18f11dcd6ed15f34e830
git rev-parse HEAD       # 应为 90a28bc66f1d96f84f8f18f11dcd6ed15f34e830
```

同步脚本会跳过已存在的仓库；已有检出不会自动切换。文中 Go 片段包含省略与教学注释，以本提交中的对应函数为准。

### 代码地图

| 目录 | 行数 | 内容 | 对应篇 |
|------|------|------|--------|
| `pkg/epp` | 56707 | EPP 主体：handlers / requestcontrol / scheduling / framework / datalayer / flowcontrol / plugins | 01·02·03·05 |
| `pkg/sidecar` | 6870 | `pd-sidecar`：P/D 与 E/P/D 编排、6 种 KV connector | 06 |
| `pkg/coordinator` | 4982 | 实验性的独立编排服务（替代 sidecar 的方向） | 06 §7 |
| `pkg/kvcache` | 3621 | KV 块索引：`kvblock.Index`、TokenProcessor、Redis/内存后端 | 04 |
| `pkg/kvevents` | 2642 | ZMQ 事件摄取、分片 Pool、replay | 04 |
| `pkg/common` | 1863 | 错误类型（429/503 的语义就在这里）、日志、配置 | 01 §3 |
| `cmd` | 1952 | `epp` / `pd-sidecar` / `coordinator` 三个入口 | 01 §1 |

## 学习路线

沿用统一示例贯穿全篇：

```
集群：P/D 分离部署，命名空间 llm-d
  prefill pods:  P1, P2      label llm-d.ai/role=prefill
  decode  pods:  D1, D2, D3  label llm-d.ai/role=decode（含 pd-sidecar）
请求 A：系统提示 S（2000 token）+ 问题 QA（50 token）
请求 B：同一个系统提示 S + 问题 QB（50 token）   ← 与 A 共享 2000 token 前缀
追问：A 走完之后，B 应该去哪？为什么？
```

| 篇 | 主题 | 核心问题 |
|----|------|---------|
| [00](00-总览与架构.md) | 总览与架构 | Router 在 llm-d 里的位置、**项目边界与组件构成**、代码地图、四条设计哲学（**万物皆插件**、决策发生在 RequestBody EoS 之后、Filter/Scorer/Screener 的分工、Primary Profile 定去向而其余走 header）、部署模式对照 |
| [01](01-请求的一生-主控制流.md) | **请求的一生** | 从 Envoy ext-proc 到选出 pod 的 **14 步**：`StreamingServer.Process` → `Director.HandleRequest` → `Scheduler.Schedule`；**429 与 503 的分界线**；profile 的迭代执行；三层状态传递（取代 `CycleState`）；**反直觉行为清单**（无 body 的请求完全绕过调度） |
| [02](02-核心代码分析-调度框架与插件体系.md) | **调度框架与插件体系** | 8 类扩展点、YAML 如何变成运行时实例（**两阶段实例化 + 依赖 DAG**）、打分机制（`[0,1]` 区间、加权求和**不做归一化**、并列如何决胜）、**全部内置插件清单**（含哪些已废弃、哪些没实现完）、默认注入与 DataProducer 自动补全、feature gates、写一个自定义插件 |
| [03](03-核心代码分析-DataLayer与指标采集.md) | Data Layer 与指标采集 | EPP 的感官系统：**每 endpoint 一个 goroutine、每 50ms 抓一次 `/metrics`**；`Source → Extract → Attribute` 生命周期；**按 `llm-d.ai/engine-type` label 适配 vLLM/SGLang 的指标名差异**；`atomic.Pointer` 快照保证 scorer 读到一致视图；DataProducer 与 Source 的区别；**指标过期（stale）对不同消费者的不同后果** |
| [04](04-核心代码分析-KVCache索引与前缀缓存路由.md) | **KV 索引与前缀缓存路由** | 近似（EPP 路由历史）vs 精确（vLLM 真实 KV 事件）；ZMQ 事件摄取与**按 pod 分片保序**；`kvblock.Index` 结构；**EPP 的 block key 不需要与 vLLM 内部 hash 一致**（只要自己前后一致）；`MatchBlockKeys`/`ScoreTokens` 的分层加权；**坑：64 token 硬下限、`prefixMatchInfoProducerName` 漏配则静默退化为近似**；replay 恢复 |
| [05](05-核心代码分析-FlowControl流控与准入.md) | Flow Control 流控与准入 | **默认关闭**，要开 `flowControl` gate；单 `Processor` goroutine 的 actor 模型；`EnqueueAndWait` 阻塞直到派发或超时；**内部错误到 429/503 的精确映射**；`FlowKey`（FairnessID + Priority）与 priority band；公平性策略与排序策略；**`utilization-detector` 的 roofline 判据与 fail-closed 行为**（`stale_endpoints` 必须监控）；驱逐机制 |
| [06](06-核心代码分析-PD分离与Sidecar.md) | **P/D 分离与 Sidecar** | sidecar 是**跑在 decode pod 里**的反向代理（不是 prefill）；`disaggregatedPrefillHandler` 的四路分支；NIXLv2 默认协议下 prefill 请求如何被改写（`max_tokens=1`）与 `kv_transfer_params` 如何协调；**6 种 KV connector 对照**（NIXLv2 / Shared Storage / SGLang / Mooncake / P2P / NIXL+P2P pull）；E/P/D 多模态扇出；chunked decode 与 data parallel；**sidecar vs Coordinator 的架构差异** |
| [07](07-部署配方与排障.md) | **部署配方与排障** | 三个官方配方逐行讲（`optimized-baseline` / `pd-epp-config` / `epp-precise-prefix-cache-config`）；调优决策表；**完整 sizing 数据**（EPP 空闲 CPU 随 pod 数线性增长、内存随输出长度增长）；HA 三模式与 Active-Active 的状态分区问题；**20 条静默失效模式总表**；排障决策树；上线检查清单 |

**建议顺序**：00 → 01（这两篇建立框架，必读）→ 02（插件体系是理解其余各篇的钥匙）→ 按需读 03~06 → 07 落地。

**只想解决具体问题**：直接跳 [07 篇 §7.2 的静默失效模式总表](07-部署配方与排障.md#72-静默失效模式总表)与 [§7.4 排障决策树](07-部署配方与排障.md#74-排障决策树)，每条都指回对应章节。

## 四个影响代码走向的关键设计

### 1. 核心代码几乎不做决策，只编排插件

EPP 的 `Scheduler` 本身不知道「怎么选 pod」。它只是按顺序调用 Filter → Scorer → Picker，**所有策略都在插件里**。这意味着：

- **读源码要先读 YAML**。不知道配了哪些插件，读 `scheduler.go` 是读不出行为的。
- **有 8 类扩展点**（02 篇 §1），远超「过滤 + 打分」的直觉。
- **框架会自动注入缺失的组件**（Picker、Parser、Data Layer、DataProducer），配置里没写的东西可能仍然在跑（02 篇 §4）。

### 2. 路由决策发生在请求体收完之后

```go
// pkg/epp/handlers/request.go:39-47（节选）
	// an EoS in the request headers means this request has no body or trailers.
	if req.RequestHeaders.EndOfStream {
		// We will route this request to a random endpoint as this is assumed to just be a GET
		return s.fallbackToRandomEndpoint(ctx, reqCtx, 0)
	}
```

必须等到 body 收完才能决策——因为要分词、算前缀 hash、看模型名。**推论**：
- 没有 body 的请求（GET 之类）**完全绕过调度器，随机选 pod**。
- Router 必须缓冲请求体，这是 EPP 内存随并发与输出长度增长的根源（07 篇 §5.1）。

### 3. 多 profile 时只有 Primary 决定 Envoy 的去向

P/D 分离下调度器会跑两个 profile（prefill、decode），但 **ext-proc 只能回一个地址**。解法是：

```
Primary profile（decode）的结果 → Envoy 的目标 endpoint
其余 profile（prefill）的结果   → 写进 HTTP header（x-prefiller-host-port）
                                 → decode pod 里的 sidecar 读 header 去调 prefill
```

**这解释了为什么 P/D 需要 sidecar**：ext-proc 协议本身没有「一个请求发两个后端」的表达能力。

### 4. 前缀路由的失败几乎都是静默的

这是本系列反复强调的一条。前缀缓存配错时**不会报错、不会降级告警，只是命中率变成 0**：

| 配错什么 | 表现 |
|---------|------|
| 漏 `prefixMatchInfoProducerName` | 以为在用精确索引，实际是近似的 |
| 漏 `dataLayer` 里的 endpoint-notification wiring | 精确索引从未订阅任何 pod，永远空 |
| `token-producer` 的 `modelName` 配错 | hash 全不匹配 |
| 共享前缀短于 block size | 一个块都凑不满 |
| Active-Active 多副本 + 近似索引 | 各副本状态分区，命中率腰斩 |

**唯一可靠的验证手段是看 `request_cached_tokens` 指标**，不能只检查配置文件（07 篇 §8.4）。

## 最容易踩的配置坑

完整的 20 条在 [07 篇 §7.2](07-部署配方与排障.md#72-静默失效模式总表)，这里列最高频的 6 条：

| 坑 | 症状 | 修法 |
|----|------|------|
| **`llm-d.ai/engine-type` label 没打** | SGLang pod 按 vLLM 指标名解析 → 全部失败 → endpoint 变 stale → **Flow Control fail-closed 全池停摆** | 打对 label；监控 `datalayer_extract_errors_total` |
| **精确前缀漏 `prefixMatchInfoProducerName`** | 静默退化为近似 | 加上；用 `request_cached_tokens` 验证 |
| **配了 `flowControl:` 但没开 feature gate** | 除 saturationDetector 外**全部忽略** | 同时加 `featureGates: ["flowControl"]` |
| **P/D 的 InferencePool targetPort 指向 vLLM 端口** | 请求绕过 sidecar，P/D 完全不生效 | 指向 sidecar 端口（8000） |
| **vLLM 没启用 KV connector** | P/D 白配，decode 自己重算 prefill | sidecar 日志 grep `missing 'kv_transfer_params'` |
| **`maxPrefixTokensToMatch` 照抄 100000** | **EPP CPU 涨 3 倍**（低吞吐下实测） | 按真实系统提示长度设 |

## 排障：优先看这三组指标

```bash
# ① 所有排障的总入口：endpoint 健康吗
kubectl exec deploy/epp -- curl -s localhost:9090/metrics | grep -E \
  'ready_endpoints|datalayer_poll_errors_total|datalayer_extract_errors_total'

# ② 前缀路由生效了吗（唯一可靠的验证手段）
kubectl exec deploy/epp -- curl -s localhost:9090/metrics | grep request_cached_tokens

# ③ 开了 Flow Control 的话，stale_endpoints 必须是 0
kubectl exec deploy/epp -- curl -s localhost:9090/metrics | grep -E \
  'flow_control_(pool_saturation|stale_endpoints)'
```

**`ready_endpoints` 小于实际 pod 数**是绝大多数「慢」和「429」问题的根因。它下面分两支：`poll_errors` 涨说明网络/端口问题，`extract_errors` 涨说明指标名不匹配（几乎总是 engine-type label 的问题）。

**`stale_endpoints > 0` 是 Flow Control 停摆的唯一前兆**：饱和检测器是 fail-closed 的，指标过期时它判定「饱和」，于是整池拒绝新请求——一个采集故障会放大成全池不可用（05 篇 §6.4）。

## 阅读源码的三个提示

1. **先看 `deploy/config/` 再看 `pkg/epp`**。那里有 19 个现成的 EPP 配置示例，是理解「这套插件体系实际怎么搭」最快的入口。挑一个（比如 `pd-epp-config.yaml`，只有 37 行）对着 02 篇读。

2. **注释里有大量「当前实现的偏差」说明**。这个仓库的注释经常在主动交代设计取舍与已知问题，比如 `admission.go` 里那段解释 429/503 语义的注释、`request.go` 里承认「无 body 的请求会随机路由」并挂了对应 PR 链接。这些是排障的第一手线索。

3. **`docs/` 与代码不一致时以代码为准，但 `docs/operations.md` 例外**——它是压测得出的 sizing 数据，代码里没有等价信息，是唯一来源（07 篇 §5 全部引自它）。

```bash
cd sources/llm-d-router
ls deploy/config/                                           # 19 个 EndpointPickerConfig 示例
rg -n 'Type\(\) string' pkg/epp/framework/plugins --no-heading | wc -l   # 内置插件数量
rg -n 'featuregate' pkg/epp --no-heading -l                 # feature gate 的定义与使用点
```

---

> **相关系列**：[llm-d WVA](../autoscaling/)（决定该有几个副本）｜ [NVIDIA Dynamo](../../dynamo/)（同层的另一套服务栈）｜ [kube-scheduler](../../../scheduling/kube-scheduler/) / [Volcano](../../../scheduling/volcano/)（决定 Pod 落在哪个节点）
> **回到** [llm-d 总览](../README.md) ｜ [推理服务栈](../../README.md) ｜ [调度与编排索引](../../../scheduling/README.md)
