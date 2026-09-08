# 05 · 实战 Demo

> **源码基线**：[`release-0.9 @ d5d5864`](https://github.com/llm-d/llm-d-autoscaling/tree/d5d586408420fbe0545f827a6ff5dc2f818b16de)（2026-08-07；2026-09-07 核对）。分支与 `v0.9.0` tag 相差 3 个提交，详见 [版本对照](README.md#版本对照与复现)。

> 目标：在 **kind + 模拟 GPU** 上把 WVA 从零跑起来，逐个验证前四篇讲的机制。
> 全程不需要真 GPU —— 仓库自带 GPU 模拟与 `llm-d-inference-sim`（一个假装是 vLLM 的模拟器，会暴露 `vllm:*` 指标）。

## 0. 先看清整条链路要几个组件

WVA 的反馈环比一般 Operator 长，缺任何一环都不工作。先把清单摊开：

| 组件 | 作用 | 缺了会怎样 |
|------|------|-----------|
| kind 集群（模拟 GPU label） | 承载一切 | — |
| Prometheus + Prometheus Operator | scrape vLLM 指标 + 存 `wva_*` | WVA 启动即 `os.Exit(1)` |
| ServiceMonitor / PodMonitor | 让 Prometheus 找到 vLLM pod | 采集不到，`wva_desired_replicas` 无数据 |
| **WVA 控制器** | 决策 | — |
| KEDA 或 prometheus-adapter | 把 `wva_desired_replicas` 变成 External Metric | HPA 报 `FailedGetExternalMetric`，副本数永不变 |
| HPA / KEDA ScaledObject（带注解） | ① 被 WVA 发现为变体 ② 实际改副本 | WVA 发现不到任何变体 |
| llm-d EPP（可选） | 提供调度器队列/到达率指标 | 队列需求项为 0；scale-from-zero 不工作 |
| 模型服务（vLLM 或 sim） | 被扩缩容的对象 + 指标源 | — |

**注意 HPA 的双重身份**：它既是 WVA 的**发现入口**（靠 `llm-d.ai/managed` 注解被扫描到，合成为变体），又是 WVA 决策的**执行者**（读 External Metric 改副本）。整条链路是一个闭环，这是理解后面所有 Demo 的前提。

## 1. 一条命令拉起全套

```bash
cd sources/llm-d-autoscaling
git switch release-0.9
git rev-parse HEAD  # 本文核对：d5d586408420fbe0545f827a6ff5dc2f818b16de
# 本机编译使用 Go 1.25.0 或兼容工具链；Dockerfile 的 builder 为 Go 1.25。

export HF_TOKEN="hf_xxxxx"                    # 必填（sim 也要，用于拉 tokenizer）
export MODEL_ID="unsloth/Meta-Llama-3.1-8B"
export ACCELERATOR_TYPE="H100"                # 模拟的 GPU 类型
export GATEWAY_PROVIDER="kgateway"            # kind 上推荐
export ITL_AVERAGE_LATENCY_MS=20              # sim 的模拟 ITL
export TTFT_AVERAGE_LATENCY_MS=200            # sim 的模拟 TTFT
export HPA_STABILIZATION_SECONDS=0            # 建议 0：防抖动交给 WVA 的死区

CREATE_CLUSTER=true make deploy-e2e-infra IMG=wva:release-0.9-d5d5864 SKIP_BUILD=false
```

该命令从当前检出构建明确命名的镜像，避免误用 registry 的 `latest` 或与分支不一致的 `v0.9.0` 镜像。以下是源码核对后的操作步骤；本次文档更新没有实际创建集群或运行压测。

这一条命令会部署（`deploy/kind-emulator/README.md:31-40`）：

- kind 集群，3 节点，**模拟 GPU（混合厂商）**
- WVA 控制器
- llm-d EPP（`llm-d-router-standalone` chart）
- Prometheus monitoring + **KEDA**（提供 external metrics）

然后部署模拟的模型服务：

```bash
# P/D 分离（prefill + decode 两个变体）
kubectl apply -k config/samples/simulator/nodeSelector/disaggregated/

# 或只部署单个角色
kubectl apply -k config/samples/simulator/nodeSelector/decode/
kubectl apply -k config/samples/simulator/nodeSelector/prefill/
kubectl apply -k config/samples/simulator/nodeSelector/decode-lws/   # LeaderWorkerSet 版本
```

`nodeSelector/` 与 `label/` 两套目录的区别：前者用 nodeSelector 把 pod 钉到特定"GPU 类型"的节点上（WVA 从 nodeSelector 反推 accelerator 名），后者用 label 方式。**先用 `nodeSelector/`** —— accelerator 解析更可靠。

### 验证部署

```bash
# 1) WVA 控制器起来了
kubectl -n workload-variant-autoscaler-system get pods
kubectl -n workload-variant-autoscaler-system logs deploy/wva-controller-manager | head -50

# 应该看到：
#   "Prometheus client and API wrapper initialized and validated successfully"
#   "LeaderWorkerSet CRD detected - support enabled"   （或 not found）
#   "KEDA ScaledObject CRD detected - annotation-based ScaledObject discovery enabled"
#   "GPU limiter constructed"  type=inventory
#   "Coordinator disabled (experimental feature; ...)"

# 2) 模拟 GPU 已就绪
kubectl get nodes -o custom-columns='NAME:.metadata.name,GPU:.status.capacity.nvidia\.com/gpu'

# 3) 模型服务在跑，且暴露了 vLLM 指标
kubectl -n llm-d-sim get pods
kubectl -n llm-d-sim port-forward <sim-pod> 8000:8000 &
curl -s localhost:8000/metrics | grep -E 'kv_cache_usage|num_requests_waiting|cache_config_info'
```

## 2. Demo 1：让 WVA 发现一个变体

这是最基础的一步 —— 验证"注解发现"这条路通了。

```yaml
# config/samples/hpa/annotations/hpa.yaml（仓库自带，原文）
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: sample-deployment-hpa
  namespace: llm-d-sim
  annotations:
    llm-d.ai/managed: "true"                # ① 纳入 WVA 管理
    llm-d.ai/model-id: "default/default"    # ② 模型标识（多变体分组依据）
    llm-d.ai/variant-cost: "10.0"           # ③ 每副本成本
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: sample-deployment
  minReplicas: 1
  maxReplicas: 10
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 0         # 防抖动交给 WVA 双阈值
      policies:
      - type: Pods
        value: 10
        periodSeconds: 15
    scaleDown:
      stabilizationWindowSeconds: 0
      policies:
      - type: Pods
        value: 10
        periodSeconds: 15
  metrics:
  - type: External
    external:
      metric:
        name: wva_desired_replicas           # ④ 读 WVA 的输出
        selector:
          matchLabels:
            variant_name: sample-deployment
            exported_namespace: llm-d-sim
      target:
        type: AverageValue
        averageValue: "1"                    # ⑤ 关键：见下方解释
```

```bash
kubectl kustomize config/samples/hpa/annotations | kubectl apply -f -
```

> **为什么 selector 里是 `exported_namespace` 而不是 `namespace`？** WVA 发出的 label 名是 `namespace`，但当 Prometheus 的 scrape 配置也注入同名 label 时会冲突，指标自带的那个被重命名为 `exported_namespace`。这套示例的监控栈正好是这种情况。**换环境前先实测**：
>
> ```bash
> curl -sG http://localhost:9090/api/v1/query --data-urlencode 'query=wva_desired_replicas' | jq '.data.result[0].metric'
> ```
>
> 看返回的是 `namespace` 还是 `exported_namespace`，再照着写 selector。这一处写错的症状与"WVA 没产出指标"完全一样（HPA 报 `FailedGetExternalMetric`），很容易误判。

### ⑤ `averageValue: "1"` 是什么意思？

这是整个链路里最容易看不懂的一处。

HPA 的 `AverageValue` 语义是：`desired = ceil(metricValue / averageValue)`（对 External metric，metricValue 是**总量**，不除以副本数）。

所以 `averageValue: "1"` 让 HPA 变成一个**恒等函数**：

```
wva_desired_replicas = 5  →  HPA desired = ceil(5 / 1) = 5
```

**HPA 被降级成了一个"执行器"**，所有智能都在 WVA 那边。这是刻意的：WVA 已经算完了目标副本数，不希望 HPA 再套一层自己的比例计算。

### 验证发现成功

```bash
# 控制器日志里应该出现
kubectl -n workload-variant-autoscaler-system logs deploy/wva-controller-manager -f \
  | grep -E 'Grouped VAs by model|Processing model|analyzer-result|scaling-decision'
```

期望看到（对应 `engine.go`、`engine_v2.go`、`engine_v2.go`）：

```
Grouped VAs by model  modelCount=1 totalVAs=1
Processing model (V2)  modelID=default/default namespace=llm-d-sim variantCount=1
analyzer-result  modelID=default/default analyzer=saturation supply=104857 demand=12000
                 util=0.114 rc=0 sc=87714 scaleUpThreshold=0.85 scaleDownBoundary=0.7
                 variants=[{"name":"sample-deployment","prc":104857,"role":"both","reason":"P1-obs"}]
scaling-decision modelID=default/default decisions=[{"name":"sample-deployment","curr":1,"tgt":1,"action":"NoChange"}]
```

**`analyzer-result` 这一行就是排障的黄金日志**，把 01 篇讲的所有量都打出来了：`supply` / `demand` / `util` / `rc` / `sc` / 两个阈值 / 每变体的 `prc` 与 `reason`。

`reason` 字段直接告诉你 k2 是哪一级来的：

| `reason` | 含义 |
|---------|------|
| `P1-obs` | 队列饱和，k2 = 实测 `TokensInUse`（最可信） |
| `P2-hist` | 用了滚动均值 |
| `P3-k2` | 从 deployment 参数推导 |
| `P4-k1` | 退到 k1（纯显存受限） |
| `P0-store` | 零副本，用 capacity store 的值 |
| `no-data` | **完全没数据** → 该 analyzer 对该模型非 live |

### 验证指标出口

```bash
# WVA 的 /metrics
kubectl -n workload-variant-autoscaler-system port-forward svc/wva-controller-manager-metrics-service 8443:8443 &
curl -sk https://localhost:8443/metrics | grep -E '^wva_(desired_replicas|saturation_utilization|required_capacity|spare_capacity|saturation_metrics_up)'

# Prometheus 里
kubectl -n monitoring port-forward svc/prometheus-operated 9090:9090 &
open 'http://localhost:9090/graph?g0.expr=wva_desired_replicas'

# HPA 是否读到了
kubectl -n llm-d-sim describe hpa sample-deployment-hpa | grep -A5 Metrics
```

**如果 HPA 报 `FailedGetExternalMetric`**，问题在 KEDA/prometheus-adapter 这一环，不在 WVA。检查：

```bash
kubectl get --raw "/apis/external.metrics.k8s.io/v1beta1/namespaces/llm-d-sim/wva_desired_replicas" | jq
```

## 3. Demo 2：触发一次扩容，手算验证

### 3.1 打流量

```bash
# 通过 gateway 打压（EPP 会把请求路由到 sim pod）
kubectl -n llm-d-sim port-forward svc/<gateway-svc> 8080:80 &

# 简单循环压测；并发拉高让 KV 涨起来
for i in $(seq 1 200); do
  curl -s localhost:8080/v1/completions \
    -H 'Content-Type: application/json' \
    -d '{"model":"default/default","prompt":"'"$(head -c 4000 /dev/urandom | base64 | head -c 3000)"'","max_tokens":512}' \
    > /dev/null &
done
```

### 3.2 观察输入量

```promql
# KV 用量峰值（saturation 用的就是这条）
max by (pod) (max_over_time(vllm:kv_cache_usage_perc{namespace="llm-d-sim"}[1m]))

# KV 总容量（k1 的输入）
max by (pod, num_gpu_blocks, block_size) (vllm:cache_config_info{namespace="llm-d-sim"})

# 队列长度（k2 P1 的判据 + 本地队列需求）
max by (pod) (max_over_time(vllm:num_requests_waiting{namespace="llm-d-sim"}[1m]))

# 平均输入/输出 token
max by (pod) (rate(vllm:request_prompt_tokens_sum{namespace="llm-d-sim"}[5m]) / rate(vllm:request_prompt_tokens_count{namespace="llm-d-sim"}[5m]))
max by (pod) (rate(vllm:request_generation_tokens_sum{namespace="llm-d-sim"}[5m]) / rate(vllm:request_generation_tokens_count{namespace="llm-d-sim"}[5m]))
```

### 3.3 手算一遍，与日志对账

假设你观测到（单副本）：

```
num_gpu_blocks = 8192, block_size = 16  → TotalKvCapacityTokens = 131072
kv_cache_usage_perc = 0.72              → TokensInUse ≈ 94371
num_requests_waiting = 7                → QueueLength = 7
AvgInputTokens = 900, AvgOutputTokens = 480
```

按 01/02 篇的公式算：

```
k1 = 131072 × 0.80 = 104857
k2：QueueLength 7 ≥ 5 且 TokensInUse > 0 → P1 命中，k2 = 94371
effectiveCapacity = min(104857, 94371) = 94371

waitingQueueDemand = 7 × (900 + 480) = 9660        （role=both）
replicaDemand      = 94371 + 9660 = 104031

TotalSupply            = 1 × 94371 = 94371
TotalAnticipatedSupply = (1+0) × 94371 = 94371
TotalDemand            = 104031        （假设调度器队列为 0）
Utilization            = 104031 / 94371 = 1.102

RC = max(0, 104031/0.85 − 94371) = max(0, 122389 − 94371) = 28018
SC = max(0, 94371 − 104031/0.70) = max(0, 94371 − 148616) = 0

n = ceil(28018 / 94371) = 1
⇒ 目标副本 1 → 2
```

现在去日志里对：

```bash
kubectl -n workload-variant-autoscaler-system logs deploy/wva-controller-manager \
  | grep 'analyzer-result' | tail -3
```

`supply` / `demand` / `util` / `rc` / `sc` / `prc` 六个数应该和你手算的对得上（`TokensInUse` 由 `kv_cache_usage_perc × TotalKvCapacityTokens` 换算，会有小数误差）。

**能对上账，说明你真正读懂了这个模型。** 对不上，按下面的顺序查：

| 对不上的项 | 大概率原因 |
|-----------|-----------|
| `prc` 差很多 | k2 走了非 P1 的路径 —— 看 `reason` 字段 |
| `supply` 是 0 | `TotalKvCapacityTokens = 0` → `cache_config_info` 采不到，走了 fallback 路径 |
| `demand` 偏大 | 别忘了调度器队列需求（EPP 的 `flow_control_queue_size`） |
| `rc` 是 0 但 util > 0.85 | `TotalAnticipatedSupply` 里算进了 pending 副本 |
| 阈值不是 0.85/0.70 | ConfigMap 合并后倒挂被重置，或有 per-analyzer override |

### 3.4 观察扩容落地

```bash
watch -n2 'kubectl -n llm-d-sim get deploy sample-deployment; \
  kubectl -n llm-d-sim get hpa sample-deployment-hpa'
```

同时看指标：

```promql
wva_desired_replicas{exported_namespace="llm-d-sim"}
wva_current_replicas{exported_namespace="llm-d-sim"}
wva_required_capacity{exported_namespace="llm-d-sim"}
wva_saturation_utilization{exported_namespace="llm-d-sim"}
```

**端到端延迟的构成**（04 篇 §2.4）：WVA 周期 15s + Prometheus scrape 15~30s + adapter 缓存 ~30s + HPA sync 15s ≈ **1~2 分钟**。别以为卡住了。

### 3.5 验证死区

停止压测，观察 `wva_spare_capacity`：

```promql
wva_spare_capacity{exported_namespace="llm-d-sim"}
```

它从 0 变正的那一刻，才会开始缩容。中间会有一段 `RC = 0 且 SC = 0` 的**死区期间** —— 利用率在 0.70~0.85 之间，两个信号都是 0，副本数保持不动。

**这段死区是 WVA 相对 HPA 最直观的差别，值得亲眼看一次。**

## 4. Demo 3：多变体成本优化

这是 WVA 相对 HPA 的第一个实质能力。

```bash
# 复制 sim Deployment，改 nodeSelector 到另一种"GPU 类型"的节点
kubectl -n llm-d-sim get deploy sample-deployment -o yaml \
  | sed -e 's/sample-deployment/sample-deployment-cheap/g' \
        -e 's/nvidia.com\/gpu.product: H100/nvidia.com\/gpu.product: L40S/' \
  | kubectl apply -f -
```

第二个 HPA —— **`model-id` 必须与第一个完全一致**，`variant-cost` 更低：

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: sample-deployment-cheap-hpa
  namespace: llm-d-sim
  annotations:
    llm-d.ai/managed: "true"
    llm-d.ai/model-id: "default/default"     # ← 与贵变体一致！这是分组依据
    llm-d.ai/variant-cost: "4.0"             # ← 更便宜
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: sample-deployment-cheap
  minReplicas: 0
  maxReplicas: 10
  behavior:
    scaleUp:   { stabilizationWindowSeconds: 0, policies: [{type: Pods, value: 10, periodSeconds: 15}] }
    scaleDown: { stabilizationWindowSeconds: 0, policies: [{type: Pods, value: 10, periodSeconds: 15}] }
  metrics:
  - type: External
    external:
      metric:
        name: wva_desired_replicas
        selector:
          matchLabels:
            variant_name: sample-deployment-cheap
            exported_namespace: llm-d-sim
      target: { type: AverageValue, averageValue: "1" }
```

### 验证：`variantCount` 变成 2

```bash
kubectl -n workload-variant-autoscaler-system logs deploy/wva-controller-manager \
  | grep -E 'Processing model \(V2\)' | tail -1
# 期望：variantCount=2
```

### 验证：扩容加在便宜的那个上

再打一轮流量，然后：

```promql
wva_desired_replicas{exported_namespace="llm-d-sim"}
```

期望看到 `sample-deployment-cheap` 在涨，`sample-deployment` 不涨（成本效率 `4.0/PRC_cheap` < `10.0/PRC_expensive`）。

**如果反了**，说明便宜变体的 `PerReplicaCapacity` 太小，导致成本效率反而更差 —— 检查它的 `reason`：0 副本时走 `P0-store`，如果 capacity store 里没有它的记录、也没有兼容变体可借，会是 `no-data`（`PRC = 0`）→ 直接被 `costGreedyRolePick` 跳过（`cost_aware_optimizer.go`）。

> 这是一个真实的冷启动问题：**从未跑过的便宜变体可能因为"不知道它的容量"而永远不被选中**。让它先跑一个副本（`minReplicas: 1`）跑一段时间，capacity store 学到之后再改回 0。

### 验证：缩容砍贵的

流量降下来后，观察缩容顺序。按 `sortVariantsForScaleDown`（成本降序），应该**先砍 `sample-deployment`（cost 10）**，最后才动 cheap。

且注意 **cheapest-at-1 的位置性规则**：cheap 会被保护在 ≥1 副本，但**只有在 expensive 已经清零之后**。所以你会看到：

```
expensive 3 → 2 → 1 → 0    （先被砍光）
cheap     2 → 1            （然后砍到 1，停住）
```

## 5. Demo 4：P/D 分离的联合扩容

```bash
kubectl apply -k config/samples/simulator/nodeSelector/disaggregated/
```

两个 HPA，`model-id` 相同，scale target 上的 `llm-d.ai/role` 分别是 `prefill` / `decode`。

### 验证 role 被正确解析

```bash
kubectl -n workload-variant-autoscaler-system logs deploy/wva-controller-manager \
  | grep 'analyzer-result' | tail -1 | python3 -m json.tool 2>/dev/null || \
kubectl -n workload-variant-autoscaler-system logs deploy/wva-controller-manager \
  | grep 'analyzer-result' | tail -1
```

`variants` 数组里每个元素的 `role` 字段应该分别是 `prefill` 和 `decode`。**如果都是 `both`，role label 没生效** —— WVA 会退回非分离模式，Δ_util 联合提交不会启动。

日志字段里专门包含 role 的原因（`engine_v2.go`）：

> Role 被包含进来，是因为 V2 **按 P/D 角色对等待请求计费**，所以不知道解析出的是哪个 role，demand 就无法解读 —— 缺失的 `llm-d.ai/role` label 会被读成 `both`，从而改变计费。

### 验证按角色分别报 RC/SC

```promql
wva_required_capacity{exported_namespace="llm-d-sim"}
```

两个变体会报**各自角色的** RC，不是同一个模型级数字（`cost_aware_optimizer.go`）。

### 验证等比扩容

给 prefill 侧的 HPA 设一个较小的 `maxReplicas`（比如 2），让它成为瓶颈，然后打大流量。观察：

```promql
wva_desired_replicas{exported_namespace="llm-d-sim"}
```

期望：**decode 不会无节制地扩** —— 它会被 prefill 的 `util` 限制住（`Δutil = min_role util_role`）。这就是 03 篇 §4 的算法在生效。

对照实验：把这两个 Deployment 改成两个**独立 model-id** 的普通 HPA，你会看到 decode 一路扩到 maxReplicas，而 prefill 卡在 2 —— 比例完全歪掉，多出来的 decode 副本纯浪费。

## 6. Demo 5：ConfigMap 热调参

### 6.1 收窄死区（更激进）

```bash
kubectl -n workload-variant-autoscaler-system edit configmap wva-saturation-scaling-config
```

```yaml
data:
  default: |
    analyzers:
      - name: saturation
        score: 1.0
    scaleUpThreshold: 0.75        # 从 0.85 降下来 → 更早扩容
    scaleDownBoundary: 0.65       # 从 0.70 降下来 → 更晚缩容（死区变宽）
    kvCacheThreshold: 0.80
    queueLengthThreshold: 5
    enableLimiter: false
```

**下一个周期（≤15s）就生效，无需重启。** 验证：

```bash
kubectl -n workload-variant-autoscaler-system logs deploy/wva-controller-manager \
  | grep 'analyzer-result' | tail -1
# scaleUpThreshold=0.75 scaleDownBoundary=0.65
```

### 6.2 per-model 覆盖

```yaml
data:
  default: |
    analyzers:
      - name: saturation
        score: 1.0
    scaleUpThreshold: 0.85
    scaleDownBoundary: 0.70

  # key 格式必须是 "<modelID>#<namespace>"
  "default/default#llm-d-sim": |
    scaleUpThreshold: 0.70        # 这个模型更激进
    priority: 5.0                 # 更高优先级（limiter 路径下有效）
```

**⚠️ 三个只能写在 `default` 的字段**（budget-scope，写在 override 里静默无效）：`enableLimiter`、`enableRescale`、`limiters`。

### 6.3 per-analyzer 阈值覆盖

```yaml
    analyzers:
      - type: saturation
        score: 1.0
        parameters:
          scaleUpThreshold: 0.90   # 只对 saturation 生效
      - type: throughput
        score: 2.0                 # 更高话语权
        scaleUpThreshold: 0.80     # 只对 throughput 生效
```

两种写法都支持：`parameters:` 里的 well-known key 会被 `Normalize()` 折叠到类型化字段（`saturation_scaling.go` 的 `AnalyzerScoreConfig`），或者直接写顶层字段。

### 6.4 阈值倒挂的自我保护（故意踩一次）

```yaml
    scaleUpThreshold: 0.60
    scaleDownBoundary: 0.80       # ← 倒挂！
```

日志会显示阈值是 **0.85 / 0.70**（默认值），不是你写的 —— `resolveSaturationConfig` 检测到 `up <= down` 后**把整对退回默认**（`engine.go`）。

**这个"静默修正"很危险**：你以为改了，其实没改，而且日志里没有明确的 warning。所以每次改阈值都要去 `analyzer-result` 那行确认实际值。

## 7. Demo 6：GPU 配额限流

### 7.1 开启 quota limiter

```yaml
data:
  default: |
    analyzers:
      - name: saturation
        score: 1.0
    scaleUpThreshold: 0.85
    scaleDownBoundary: 0.70
    enableLimiter: true              # ① 必须！否则 limiter 构造但不被咨询
    limiters:                        # ② 选择 quota 模式
      - name: ns-quota
        type: quota
        scope: namespace
        namespaceQuotas:
          llm-d-sim: { H100: 2, L40S: 4 }
```

### 7.2 验证优化器切换

```bash
kubectl -n workload-variant-autoscaler-system logs deploy/wva-controller-manager \
  | grep -E 'GPU limiter \(re\)built|Optimizer selected'
# 期望：
#   GPU limiter (re)built from config  type=quota name=ns-quota
#   Optimizer selected  analyzer=saturation optimizer=greedy-by-score enableLimiter=true
```

```promql
wva_optimizer_active{optimizer_name="greedy-by-score"}   # == 1
wva_optimizer_active{optimizer_name="cost-aware"}        # == 0
```

**热重建验证**：把 `H100: 2` 改成 `H100: 8`，不重启，看是否有新的 `GPU limiter (re)built` 日志（靠 `limiterSignature` 的指纹比对触发，`engine.go`）。

### 7.3 验证限流生效

打大流量，让需求超过配额：

```promql
wva_decisions_limited_total          # 应该在涨
wva_required_capacity                # 持续 > 0（想扩但扩不了）
wva_desired_replicas                 # 卡在配额上限
```

**这是一个重要的观测组合**：`wva_required_capacity > 0` 持续存在 + `wva_desired_replicas` 不涨 = **被限流卡住了**，不是 WVA 坏了。04 篇的检查清单里建议对这个组合告警。

### 7.4 故意踩坑：只配 limiters 不配 enableLimiter

```yaml
    enableLimiter: false           # ← 忘了改
    limiters:
      - name: ns-quota
        type: quota
        scope: namespace
        namespaceQuotas:
          llm-d-sim: { H100: 2 }
```

启动日志里有一条 Info（`main.go:496-501`）：

```
Quota limiter selected; quota caps are enforced ONLY when enableLimiter: true is set
in the saturation-scaling ConfigMap. With the default enableLimiter: false the engine
runs the unlimited optimizer and quota caps are not applied.
```

配额完全不生效，`wva_optimizer_active{optimizer_name="cost-aware"} == 1`。**这是最常见的配置错误。**

### 7.5 开启 rescale（Alpha）

```yaml
    enableLimiter: true
    enableRescale: true             # 竞争时按 priority×demand 重分配整个预算
```

要看到效果需要**两个模型 + 不同 priority + GPU 竞争**：

```yaml
  "default/default#llm-d-sim": |
    priority: 1.0
  "other/model#llm-d-sim": |
    priority: 5.0
```

让两个模型都需要扩、配额只够一个，观察低优先级模型的 `wva_desired_replicas` **被主动降低**（但不低于 `minReplicas × gpusPerReplica` 的 floor）。

## 8. Demo 7：scale-to-zero 与冷启动

scale-to-zero 需要 `minReplicas: 0`，而 **HPA 的 `minReplicas` 不能为 0** —— 所以必须走 KEDA `ScaledObject`：

```bash
kubectl kustomize config/samples/keda/annotations | kubectl apply -f -
```

```yaml
# config/samples/keda/annotations/scaledobject.yaml（结构示意）
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: sample-deployment-so
  namespace: llm-d-sim
  annotations:
    llm-d.ai/managed: "true"
    llm-d.ai/model-id: "default/default"
    llm-d.ai/variant-cost: "10.0"
spec:
  scaleTargetRef:
    name: sample-deployment
  minReplicaCount: 0                # ← KEDA 支持真正的 0
  maxReplicaCount: 10
  triggers:
  - type: prometheus
    metadata:
      serverAddress: http://prometheus-operated.monitoring:9090
      query: wva_desired_replicas{variant_name="sample-deployment",exported_namespace="llm-d-sim"}
      threshold: "1"
```

**为什么 KEDA 能取到 0**：`variant_fromannotations.go:58` 直接读 `so.Spec.MinReplicaCount`，而 HPA 分支（`:107-110`）在 nil 时兜底成 1，且 HPA 本身校验 `minReplicas >= 1`。

### 启用 scale-to-zero

```yaml
# saturation ConfigMap，内联方式（优先级最高）
data:
  default: |
    analyzers:
      - name: saturation
        score: 1.0
    scaleToZero:
      enabled: true
```

或用独立的 `wva-model-scale-to-zero-config` ConfigMap，或 `WVA_SCALE_TO_ZERO` 环境变量（三级优先级见 03 篇 §7）。

### 验证缩到 0

停掉所有流量，等保留期（`requestCountFunc` 查 `vllm:request_success_total` 的窗口）过去：

```bash
watch -n5 'kubectl -n llm-d-sim get deploy sample-deployment'
```

```promql
wva_enforcer_modifications_total     # Enforcer 把决策改到 0 时会涨
```

注意 Enforcer 会**覆盖 cost-aware 的 cheapest-at-1 保护** —— 这是它比优化器"后跑"的意义。

### 验证从 0 唤醒

```bash
# 观察 scale-from-zero 引擎的日志（100ms 轮询，日志量大，用 grep 过滤）
kubectl -n workload-variant-autoscaler-system logs deploy/wva-controller-manager -f \
  | grep -E 'scaling up from zero|Successfully scaled up Target Workload|Scale-from-zero decision'

# 另一个终端发一个请求
curl -s localhost:8080/v1/completions -H 'Content-Type: application/json' \
  -d '{"model":"default/default","prompt":"hello","max_tokens":16}'
```

期望日志（`scalefromzero/engine.go`）：

```
Target workload has pending requests, scaling up from zero  metricName=inference_extension_flow_control_queue_size value=1
Successfully scaled up Target Workload  variant=... inferencepool=...
Scale-from-zero decision written to cache  targetReplicas=1 reason=ScaleFromZero: pending request - scale-up
```

**⚠️ 这条路径强依赖 EPP**：触发信号是 `inference_extension_flow_control_queue_size{target_model_name="<modelID>"}`。EPP 没部署 → 永远唤不醒 → 请求全部超时。

**⚠️ 已知行为**：同模型的**所有** 0 副本变体都会被拉到 1（`engine.go` 的 TODO），不会只拉最便宜的。多变体 + scale-to-zero 时会看到两个变体同时被唤醒。

## 9. Demo 8（可选）：开启 throughput analyzer

```yaml
data:
  default: |
    analyzerName: saturation
    scaleUpThreshold: 0.85
    scaleDownBoundary: 0.70
    analyzers:
      - name: saturation
        score: 1.0
      - name: throughput
        score: 1.0
```

```bash
# 首次启用启动时未注册的 throughput，必须重启；已有注册的 enabled 开关按周期读取
kubectl -n workload-variant-autoscaler-system rollout restart deploy/wva-controller-manager
```

**不重启会怎样**：ConfigMap 上出现一个 Warning Event：

```bash
kubectl -n workload-variant-autoscaler-system describe configmap wva-saturation-scaling-config
# Events:  Warning  ThroughputAnalyzerRestartRequired  ...
```

### 验证注册成功

```bash
kubectl -n workload-variant-autoscaler-system logs deploy/wva-controller-manager \
  | grep -E 'ThroughputAnalyzer (registered|NOT registered)'
# 期望：ThroughputAnalyzer registered (enabled in saturation config)
```

### 验证两个 analyzer 都在跑

```bash
kubectl -n workload-variant-autoscaler-system logs deploy/wva-controller-manager \
  | grep 'analyzer-result' | tail -4
```

应该每周期有**两行** `analyzer-result`，`analyzer` 字段分别是 `saturation` 和 `throughput`。

### 观察 ITL 模型预热

throughput 需要攒 ≥10 个 `(k*, ITL)` 样本且 k 跨度 ≥0.30 才能做 Tier-1 OLS。窗口未 ready 时先尝试 Tier-2，`reason` 可为 `T2-default` / `T2-pinned`，不能把预热期一概理解为供需都为 0。要制造 k 跨度，需要**变化的负载**（时高时低），恒定压测反而攒不出跨度。

留意 GPS mismatch 日志（INFO 级）：

```bash
kubectl -n workload-variant-autoscaler-system logs deploy/wva-controller-manager | grep -i 'gps'
```

⚠️ **如果没部署 EPP，不建议开 throughput**：TA 在 EPP 缺失时不会抑制 SpareCapacity（见 02 篇 §7.3），可能基于不可靠的供给估计触发缩容。

### 可选验证：throughput-only 与零副本历史容量

在上述双 analyzer 配置中把 saturation 设置为 `enabled: false`，保留已注册的 throughput。观察日志中的 `saturation analyzer is absent from the configured analyzer list: it will not vote and cannot veto scale-down for this model`。这条固定日志也会用于显式禁用的情况；同时仍能看到 saturation 的 `analyzer-result`，因为它继续提供变体身份。

先在有流量时让 TA 得到有效容量，再通过 Demo 7 的零副本配置使一个变体缩到零；历史状态仍有效时，throughput 日志应出现 `reason=T-sfz`。这只说明该变体可以参与主动容量选择，不保证本轮一定扩容。没有历史容量时不会出现此值；主引擎无法主动选择该变体，真正冷启动需依赖 EPP 与 scale-from-zero 引擎。实验后恢复 saturation 投票。

## 10. Demo 9（仅隔离实验）：验证 QM 拒绝与恢复

本基线不能通过 ConfigMap 开启 queueing-model SLO 扩缩容。本实验验证 `refuseQueueingModel` 的行为，只在前面创建的实验环境执行；QM 配置含 `default` 会覆盖全局选路，使主循环保持目标副本。

```bash
# 实验前确认使用默认 V2 配置，并记录当前目标副本。
# 此文件的 namespace 是 workload-variant-autoscaler-system。
kubectl apply -f deploy/configmap-queueing-model.yaml

kubectl -n workload-variant-autoscaler-system logs deploy/wva-controller-manager \
  | grep -E 'queueing-model optimization path is disabled|refusing to dispatch'
kubectl get events -A --field-selector reason=OptimizationRefused
```

预期：错误日志持续提示 QM 被禁用；有 EventRecorder 时发送 `OptimizationRefused` Warning。`wva_desired_replicas` 继续发布，目标保持先前可用值；无历史分配时尝试读取实际 scale target 的副本数，避免仅因没有决策就输出 0。`OptimizationReady=False` 只在临时内存对象中设置，不能用 `kubectl get variantautoscaling` 查询。

这是主优化循环的保持行为，独立的 scale-from-zero 引擎、HPA/KEDA 的稳定窗口或其它 trigger 仍可能影响实际副本数。不要期待 EKF 收敛，也不要通过修改 `sloMultiplier` 验证副本变化。

```bash
# 实验结束：移除刚加入的 default 键，避免继续覆盖 V2 选路。
kubectl -n workload-variant-autoscaler-system patch configmap wva-queueing-model-config \
  --type=json -p='[{"op":"remove","path":"/data/default"}]'

# 下一轮恢复 V2；检查实际日志时间，避免读到恢复前的历史错误。
kubectl -n workload-variant-autoscaler-system logs deploy/wva-controller-manager --since=1m \
  | grep -E 'Processing model \(V2\)|analyzer-result|refusing to dispatch'
```

无需重启。恢复依据是 `QMAnalyzerConfig()` 中不再有 `default`，以及随后重新执行 V2 分析；仅看到 `Optimizer selected` 或指标仍存在都不足以证明恢复。

## 11. 排障速查

### 决策树

先查 `OptimizationRefused`：QM 拒绝时 **指标仍存在且刷新**，不会进入下面的“没有序列”分支。移除 QM 配置的 `default` 后再排查 V2。

```
wva_desired_replicas 没有序列
├─ 控制器日志有 "No active VariantAutoscalings found"？
│    → HPA/ScaledObject 缺 llm-d.ai/managed: "true"，或不在跟踪的 namespace
├─ 有 "Processing model" 但 variantCount 少了一个？
│    → 该 HPA 缺 llm-d.ai/model-id，或 model-id 与其它变体不一致
├─ analyzer-result 里 reason=no-data？
│    → 采集断了：查 pod→变体映射（wva_pod_mapping_miss_total）
└─ analyzer-result 里 supply=0？
     → cache_config_info 采不到（走了 fallback），或 PRC=0

副本数不变（指标正常）
├─ kubectl describe hpa 报 FailedGetExternalMetric？
│    → KEDA / prometheus-adapter 那一环，不是 WVA
├─ wva_required_capacity > 0 但 desired 不涨？
│    ├─ 顶到 HPA maxReplicas？
│    ├─ wva_decisions_limited_total 在涨 → 被配额限流
│    └─ wva_gpu_discovery_up == 0 → GPU 发现失败（会退回 cost-aware，看日志）
├─ RC=0 且 SC=0？
│    → 在 0.70~0.85 死区里，正常行为
└─ SC > 0 但不缩？
     → 检查 Enabled 投票集：无 live 投票者、某个 live 投票者 RoleSpare ≤ 0、无有效 anchor，或未达到一个副本的富余量；非 live 本身不否决

阈值改了不生效
├─ analyzer-result 里的阈值仍是 0.85/0.70？
│    → 阈值倒挂被静默重置，或写在了 per-model override 但字段是 budget-scope
└─ 加了 analyzer 不生效？
     → 需要重启控制器（看 ConfigMap 的 Warning Event）

scale-from-zero 不工作
├─ EPP 部署了吗？inference_extension_flow_control_queue_size 有数据吗？
├─ InferencePool 在 datastore 里吗？（日志 "Inferencepool datastore is empty"）
└─ pool 的 label selector 匹配到 pod template 的 labels 了吗？
```

### 最有用的三条命令

```bash
# 1) 一眼看清每个 analyzer 的供需与阈值
kubectl -n workload-variant-autoscaler-system logs deploy/wva-controller-manager \
  | grep 'analyzer-result' | tail -5

# 2) 一眼看清最终决策
kubectl -n workload-variant-autoscaler-system logs deploy/wva-controller-manager \
  | grep 'scaling-decision' | tail -5

# 3) 一眼看清生效的配置（配置回显指标）
curl -sk https://localhost:8443/metrics | grep -E '^wva_config_'
```

### 关键 Grafana 面板

仓库自带 dashboard（`deploy/grafana/`），核心五张图（`docs/developer-guide/benchmark-panels/` 有截图）：

| 面板 | PromQL |
|------|--------|
| 副本数对比 | `wva_desired_replicas` 与 `wva_current_replicas` 叠加 |
| 饱和度 | `wva_saturation_utilization` + 两条阈值参考线（0.85 / 0.70） |
| 容量信号 | `wva_required_capacity` 与 `wva_spare_capacity` 双轴 |
| KV token | `wva_kv_cache_tokens_used / wva_kv_cache_tokens_capacity` |
| 队列深度 | `vllm:num_requests_waiting` + `inference_extension_flow_control_queue_size` |

**所有 saturation 类面板/告警都应该乘上新鲜度门**：

```promql
wva_saturation_utilization * on(variant_name, exported_namespace) group_left() (wva_saturation_metrics_up == 1)
```

## 12. Benchmark 环境（进阶）

仓库自带一套完整的 benchmark 工具链（`Makefile` 的 `benchmark-*` 目标），跑真实负载：

```bash
make benchmark-install                       # 拉 llm-d-benchmark（默认 v0.7.0）+ 装 llmdbenchmark CLI

export BENCHMARK_NAMESPACE=llm-d-bench
export MODEL_ID="unsloth/Meta-Llama-3.1-8B"
make benchmark-standup                       # 起环境
make benchmark-run BENCHMARK_HARNESS=guidellm   # 跑一轮（或 inference-perf）
make benchmark-run-bursty                    # 突发流量（inference-perf 多阶段速率）
make benchmark-report                        # 生成 markdown 结果表
make benchmark-teardown

# 一条龙
make benchmark-full                          # standup → run all scenarios → teardown
```

几个特别有用的辅助目标：

```bash
make benchmark-add-variant                   # 给运行中的 benchmark 加第二个变体
make benchmark-enable-v2-saturation          # 切到 V2 分析器（apply configmap + 重启）
make benchmark-restart-controller            # 清掉内存状态（如两轮之间清 k2 历史）
make benchmark-plot-two-variant              # 画双变体的副本/延迟/吞吐曲线
```

**`benchmark-restart-controller` 值得注意**：因为 k2 滚动均值、ITL 观测窗口与 `T-sfz` 历史容量**全在内存**，两轮 benchmark 之间不清就会互相污染 —— 上一轮学到的容量会影响下一轮的冷启动行为，结果不可复现。

对照实验的推荐做法（`docs/developer-guide/two-variant-wva-benchmark.md`）：

```bash
# A/B 对照：WVA 决策 vs 纯 KEDA 阈值
make benchmark-standup BENCHMARK_DIRECT_KEDA=true    # 控制器无关的 EPP+KEDA 自动扩缩容
```

## 13. 清理

```bash
# kind 环境
./deploy/kind-emulator/teardown.sh
# 或
kind delete cluster --name <cluster-name>
```

## 14. 本篇速查表

### 最小闭环的五个必备条件

1. Prometheus 可达（否则 WVA 直接 `os.Exit(1)`）
2. vLLM/SGLang 指标被 scrape 到（`vllm:kv_cache_usage_perc` 有数据）
3. HPA/ScaledObject 上有 `llm-d.ai/managed: "true"` + `llm-d.ai/model-id`
4. External Metric 通路打通（KEDA 或 prometheus-adapter）
5. HPA 的 metric target 用 `averageValue: "1"`（恒等映射）

### 最容易踩的配置坑

1. **`limiters:` 配了但 `enableLimiter: false`** → 配额完全不生效（启动日志有 Info 警告）
2. **新启用启动时未注册的 throughput 不重启** → 不生效（ConfigMap 上有 Warning Event）
3. **阈值倒挂** → 整对静默退回 0.85/0.70（无 warning，只能在 `analyzer-result` 日志里发现）

4. **QM 配置含 `default`** → 主循环拒绝优化，但指标继续刷新，不能用“有指标”判断健康
5. **throughput-only** → saturation 仍有日志，但不投票、不能否决缩容

### 验证清单

| 想验证 | 看什么 |
|-------|-------|
| 变体被发现 | 日志 `Grouped VAs by model` 的 `totalVAs` |
| 同模型的变体分组正确 | 日志 `Processing model (V2)` 的 `variantCount` |
| 容量算对了 | 日志 `analyzer-result` 的 `prc` + `reason` |
| 双阈值生效 | `analyzer-result` 的 `scaleUpThreshold` / `scaleDownBoundary` |
| 死区存在 | `wva_required_capacity == 0 and wva_spare_capacity == 0` 的时段 |
| 成本优化生效 | 只有便宜变体的 `wva_desired_replicas` 在涨 |
| P/D 联合扩容 | `analyzer-result` 的 `variants[].role` 是 prefill/decode，且 decode 被 prefill 限制 |
| 用了哪个优化器 | `wva_optimizer_active` |
| 被限流 | `wva_decisions_limited_total` 上涨 + `wva_required_capacity` 持续 >0 |
| Enforcer 介入 | `wva_enforcer_modifications_total` |
| 映射断了 | `wva_pod_mapping_miss_total` |
| 配置真的生效了 | `wva_config_*` 系列及实际 analyzer 配置 |
| QM 拒绝与恢复 | `refusing to dispatch` 日志 / `OptimizationRefused` Event；移除 `default` 后恢复 V2 |
| TA 零副本容量复用 | throughput 的 `reason=T-sfz` |

---

## 全系列回顾

| 篇 | 一句话 |
|----|-------|
| [00](00-WVA总览与架构.md) | WVA 是给 HPA 喂目标副本数的全局大脑；变体曾是 CRD、现已改为注解合成的内存对象（决定了哪些资料已过时），Reconciler 不做决策，真正的引擎是轮询循环 |
| [01](01-核心原理-轮询引擎与双阈值容量模型.md) | `RC = max(0, demand/0.85 − anticipated)`、`SC = max(0, supply − demand/0.70)`；扩容用 anticipated、缩容用 supply 的不对称是刻意的 |
| [02](02-核心代码分析-指标采集与Analyzer.md) | 16 条查询分四批发出的完整清单；k2 的四级链是一次在线标定；saturation / throughput 的量纲差异；QM 当前禁用 |
| [03](03-核心代码分析-Optimizer与Limiter.md) | 成本贪心 vs 公平分配；P/D 的 Δ_util 联合提交；rescale 的优先级水填充 |
| [04](04-面向大模型推理的能力地图.md) | 能做什么、做不到什么、三种落地形态、坑清单与上线检查清单 |
| **05** | **在 kind 上把每个机制亲手验证一遍** |

> 计算逻辑的可视化速查（含公式字典、指标来源表、数值 Demo）见同目录的 **[`wva-autoscaling-logic.html`](wva-autoscaling-logic.html)**，浏览器直接打开。
