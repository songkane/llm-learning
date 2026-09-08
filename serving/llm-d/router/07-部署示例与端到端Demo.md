# 07 · 部署示例与端到端 Demo

> **源码基线**：[`main @ 90a28bc`](https://github.com/llm-d/llm-d-router/tree/90a28bc66f1d96f84f8f18f11dcd6ed15f34e830)
> 本篇把 00~06 篇的机制合成**一套可部署的配置**，再用 [00 篇 §2 的统一示例](00-总览与架构.md#2-统一示例贯穿-0007-篇)端到端跑一遍，最后附一张全系列反复指回来的静默失效总表。

## 0. 这个示例覆盖什么

**P/D 分离 + 精确前缀缓存全开**——这是功能最完整、也最容易配错的组合，因为它同时用到了本系列几乎所有机制：

| 机制 | 哪篇讲 | 在这个示例里体现为 |
|------|--------|------------------|
| 两个 scheduling profile | 02 §6 | `disagg-profile-handler` + `prefill`/`decode` 两条链 |
| 精确 KV 索引与 ZMQ 事件 | 04 §1 | `precise-prefix-cache-producer` + vLLM 的 `--kv-events-config` |
| Data Layer 手写 wiring | 03 §1 | 显式的 `dataLayer` 段 |
| sidecar 两阶段调用 | 06 §2 | decode pod 里的 `pd-sidecar` |
| PD decider | 02 §5.4 | `prefix-based-pd-decider`，决定"这个请求值不值得分离" |

> **关于这份配置的来源**：上游把 P/D（`deploy/config/pd-epp-config.yaml`）和精确前缀缓存（`deploy/config/epp-precise-prefix-cache-config.yaml`）做成了**两个独立的示例文件**，本篇是把它们合成一份并补齐 K8s 侧清单，属于教学用的组合，不是上游某个文件的原文。真实部署请对照 `deploy/environments/dev/p-d/` 的 kustomize overlay。

## 1. 部署示例

### 1.1 拓扑与端口

端口是 P/D 部署最容易配错的地方，先把它钉死。**关键一点：两种 pod 对外都用 8000**，所以整个池子只需要一个 targetPort：

```
                    ┌─ EPP :9002 ext-proc  :9003 健康  :9090 指标
                    │        ▲ 抓 /metrics        ▲ ZMQ 订阅 KV 事件
客户端 → Envoy ──────┘        │                    │
          │                  │                    │
          └─ x-gateway-destination-endpoint = D3:8000
             x-prefiller-host-port          = P1:8000
                    │
                    ▼
        ┌───────────────────────────┐        ┌──────────────────────┐
        │ decode pod (D1..D4)       │        │ prefill pod (P1..P2) │
        │  pd-sidecar   :8000  ◄────┼─Envoy  │  vLLM      :8000     │
        │       │ localhost         │        │  ZMQ pub   :5557     │
        │       ▼                   │        │  ZMQ replay:5558     │
        │  vLLM         :8200       │───────►│  （无 sidecar）       │
        │  ZMQ pub      :5557       │ ①先调 prefill                 │
        │  ZMQ replay   :5558       │◄───────│ ②KV 经 NIXL 传回      │
        └───────────────────────────┘        └──────────────────────┘
```

| 端口 | 谁在听 | 谁来访问 |
|------|--------|---------|
| **8000** | decode pod 的 **sidecar**；prefill pod 的 **vLLM** | Envoy（只打 decode）、EPP（抓 `/metrics`）、sidecar（直连 prefill，不过 Envoy） |
| 8200 | decode pod 的 vLLM | 只有本 pod 的 sidecar，走 `localhost` |
| 8001-8007 / 8201-8207 | DP rank r 的 sidecar / vLLM | `sidecar 8000+r → vLLM 8200+r` |
| 5557 / 5558 | 两种 pod 的 vLLM | EPP 的 ZMQ 订阅 / replay |
| 9002 / 9003 / 9090 | EPP | Envoy 的 ext-proc / gRPC 健康检查 / Prometheus |

**8000 这个统一端口是整套设计的枢纽**，它同时解决了三个问题：

1. **`InferencePool` 只需要一个 `targetPorts`**。prefill pod 把 vLLM 直接放在 8000（`vllm-prefill/deployment.yaml:29`，且 `initContainers: []` —— prefill 不需要 sidecar），decode pod 把 sidecar 放在 8000、vLLM 藏在 8200 走 loopback。**两种角色在池子看来都是"一个 8000 端口的 endpoint"**，池子不需要为角色区分端口。
2. **EPP 抓 8000 就能拿到 vLLM 的指标**。sidecar 只接管五条推理路径做 P/D 编排，其余路径由兜底的 `mux.Handle("/", decoderProxy)`（`proxy.go:635`）**反向代理**给本地 vLLM，`/metrics`、`/v1/models` 都在其中。所以同一个端口既满足转发也满足采集。

   **但 `GET /health` 是个例外**：sidecar 自己应答，无条件返回 200（`proxy.go:624-626`），**不反映 vLLM 的真实状态**。所以 K8s 探针不要打 sidecar 的 8000，要直接打 vLLM 的 8200（下面 decode Deployment 里就是这么配的）。
3. **prefill 地址也是 8000**。EPP 从同一个池子里选出 prefill endpoint，写进 `x-prefiller-host-port` 的自然就是 `P:8000`，sidecar 直连过去打到的正是 prefill 的 vLLM。

**DP 是唯一需要列多个端口的场景**：`TARGET_PORTS` 由 `scripts/kind-dev-env.sh:232-237` 生成，基础是 `- number: 8000`，`VLLM_DATA_PARALLEL_SIZE > 1` 时才追加 `8001..800N`。

### 1.2 EPP 配置

```yaml
apiVersion: llm-d.ai/v1alpha1
kind: EndpointPickerConfig
plugins:
  # ── 分词与精确前缀索引 ────────────────────────────────
  - type: token-producer
    parameters:
      modelName: Qwen/Qwen3-8B          # ← 必须与 vLLM 部署的模型完全一致
      vllm:
        url: http://vllm-render:8082    # ← 专门跑分词的实例，不要指向推理 pod
  - type: precise-prefix-cache-producer
    parameters:
      tokenProcessorConfig:
        blockSizeTokens: 16             # 与 04 §9.1 的推演一致
      indexerConfig:
        kvBlockIndexConfig:
          enableMetrics: true           # 没有它就没法验证索引里有没有东西
      kvEventsConfig:
        podDiscoveryConfig:
          socketPort: 5557
          replaySocketPort: 5558        # 漏了它 EPP 重启后索引全空（04 §7.3）

  # ── Data Layer（手写 dataLayer 段就必须把这两个也列出来）──
  - type: metrics-data-source
  - type: core-metrics-extractor
  - type: endpoint-notification-source

  # ── 打分与选择 ────────────────────────────────────────
  - type: prefix-cache-scorer
    parameters:
      prefixMatchInfoProducerName: precise-prefix-cache-producer
  - type: queue-scorer
  - type: kv-cache-utilization-scorer
  - type: max-score-picker

  # ── P/D 编排 ─────────────────────────────────────────
  - type: prefill-filter
  - type: decode-filter
  - type: disagg-profile-handler
    parameters:
      deciders:
        prefill: prefix-based-pd-decider
  - type: prefix-based-pd-decider
    parameters:
      nonCachedTokens: 512              # 上游示例是 16，见下面「四个必须改的值」

dataLayer:
  sources:
    - pluginRef: metrics-data-source
      extractors:
        - pluginRef: core-metrics-extractor
    - pluginRef: endpoint-notification-source
      extractors:
        - pluginRef: precise-prefix-cache-producer

schedulingProfiles:
  - name: prefill
    plugins:
      - pluginRef: prefill-filter
      - pluginRef: prefix-cache-scorer
        weight: 2.0
      - pluginRef: queue-scorer
        weight: 1.0
      - pluginRef: max-score-picker
  - name: decode
    plugins:
      - pluginRef: decode-filter
      - pluginRef: prefix-cache-scorer
        weight: 2.0
      - pluginRef: queue-scorer
        weight: 1.0
      - pluginRef: kv-cache-utilization-scorer
        weight: 1.0
      - pluginRef: max-score-picker
```

**四个必须改的值**，改错了都是静默失效：

| 字段 | 改成什么 | 配错的后果 |
|------|---------|-----------|
| `token-producer.modelName` | vLLM 实际加载的模型名 | 参与 `getInitHash`，**hash 全不匹配，命中率恒 0**（04 §4.3） |
| `token-producer.vllm.url` | 一个**专跑分词**的 vLLM 实例地址 | 上游示例里写的 `http://localhost:8000` 只在 EPP 与 vLLM 同 pod 时才对；独立部署的 EPP 必须改。上游 dev 环境是单独起一个 `vllm launch render` 的 `vllm-render:8082` Service（`kind-dev-env.sh:66-69`），**不要指向承载推理流量的 pod** |
| `blockSizeTokens` | 与你的共享前缀长度匹配 | 共享前缀凑不满一块就完全无效（04 §9.2） |
| `nonCachedTokens` | 按网络开销定 | 见下 |

**`nonCachedTokens` 为什么我写 512 而不是上游的 16**：它是"未命中多少 token 才值得走 P/D 分离"的门槛。16 意味着几乎所有请求都分离；跨节点无 RDMA 时，分离的网络开销可能大于省下的 prefill 计算。512 在本篇的 demo 里还有个额外的好处——它让请求 A 和 B 走**不同的路径**，正好把 decider 的两个分支都演示出来（§2.4）。

**三处最容易漏的连带影响**：

1. **`prefixMatchInfoProducerName` 漏了** → 框架自动补一个**近似** producer，你以为在用精确索引，没有任何报错（04 §5.3）。
2. **`dataLayer` 段漏了 `endpoint-notification-source` → `precise-prefix-cache-producer` 这条 wiring** → producer 在、scorer 绑定也对，但**从来没订阅任何 pod**，索引永远是空的（04 §1.5）。
3. **`metrics-data-source` + `core-metrics-extractor` 这里是显式列出的，但机制是"叠加注入"而不是"要么全给要么全没"**。`ensureDataLayer`（`defaults.go:307-329`）只在两种情况下不注入：配了 `dataLayer.injectDefaults: false`，或者你的 `dataLayer.sources` 里**已经有一个 `metrics-data-source`**（`hasSourceOfType`，`defaults.go:339-346`）。其余情况它会把默认的 source + extractor **追加**进你手写的 `dataLayer` 里。所以只写 notification 那一条不会丢掉指标采集——我这里写全是为了让配置自解释、以及把 extractor 的绑定关系摆在明面上。

### 1.3 K8s 清单

**prefill Deployment**（只有 vLLM，不需要 sidecar，对照 `deploy/components/vllm-prefill/deployment.yaml`）：

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: llama-8b-prefill
  namespace: llm-d
spec:
  replicas: 2                                    # P1, P2
  selector:
    matchLabels: { app: llama-8b, llm-d.ai/component: prefill }
  template:
    metadata:
      labels:
        app: llama-8b                            # ← InferencePool 的 selector 认这个
        llm-d.ai/component: prefill              # ← 组件标识，非功能性
        llm-d.ai/role: prefill                   # ← prefill-filter 认这个
        llm-d.ai/engine-type: vllm               # ← 决定用哪套指标名映射
    spec:
      initContainers: []                         # ← prefill 没有 sidecar
      containers:
        - name: vllm
          image: vllm/vllm-openai:latest
          args:
            - --model=Qwen/Qwen3-8B
            - --port=8000                        # ← 直接占用池子的 targetPort
            - --kv-transfer-config={"kv_connector":"NixlConnector","kv_role":"kv_both"}
            - --kv-events-config={"enable_kv_cache_events":true,"publisher":"zmq","endpoint":"tcp://*:5557","replay_endpoint":"tcp://*:5558"}
          ports:
            - { containerPort: 8000, name: prefill-http }
            - { containerPort: 5557, name: kv-events }
            - { containerPort: 5558, name: kv-replay }
```

**decode Deployment**（sidecar + vLLM，对照 `deploy/components/vllm-decode/deployment.yaml`）：

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: llama-8b-decode
  namespace: llm-d
spec:
  replicas: 4                                    # D1..D4
  selector:
    matchLabels: { app: llama-8b, llm-d.ai/component: decode }
  template:
    metadata:
      labels:
        app: llama-8b
        llm-d.ai/component: decode
        llm-d.ai/role: decode                    # ← decode-filter 认这个
        llm-d.ai/engine-type: vllm
    spec:
      initContainers:
        # 原生 sidecar 模式：initContainer + restartPolicy: Always
        # 让它先于 vLLM 启动、随 pod 生命周期常驻
        - name: routing-sidecar                  # ← Envoy 和 EPP 都打这个容器
          image: ghcr.io/llm-d/llm-d-routing-sidecar:latest
          restartPolicy: Always                  # ← 缺了它就退化成普通 init 容器，跑完就退出
          args:
            - --port=8000
            - --kv-connector=nixlv2
            - --secure-proxy=false
          ports:
            - { containerPort: 8000, name: sidecar-http }
          env:
            - name: POD_IP
              valueFrom: { fieldRef: { fieldPath: status.podIP } }
      containers:
        - name: vllm
          image: vllm/vllm-openai:latest
          args:
            - --model=Qwen/Qwen3-8B
            - --port=8200                        # 只对 localhost 暴露，默认值就是它
            - --kv-transfer-config={"kv_connector":"NixlConnector","kv_role":"kv_both"}
            - --kv-events-config={"enable_kv_cache_events":true,"publisher":"zmq","endpoint":"tcp://*:5557","replay_endpoint":"tcp://*:5558"}
          ports:
            - { containerPort: 8200, name: http }
            - { containerPort: 5557, name: kv-events }
            - { containerPort: 5558, name: kv-replay }
          startupProbe:                          # 模型加载慢，先扛住 10 分钟
            httpGet: { path: /health, port: 8200 }
            failureThreshold: 60
            periodSeconds: 10
          readinessProbe:
            httpGet: { path: /health, port: 8200 }
            periodSeconds: 10
```

**sidecar 是 `initContainer` 而不是普通 `containers` 条目**，靠 `restartPolicy: Always` 变成 K8s 的原生 sidecar。这样它保证在 vLLM 之前就位——否则 Envoy 可能在代理还没监听时就把流量打过来。

**InferencePool**（服务发现，对照 `deploy/components/inference-gateway/inference-pools.yaml`）：

```yaml
apiVersion: inference.networking.k8s.io/v1
kind: InferencePool
metadata:
  name: llama-8b-pool
  namespace: llm-d
spec:
  selector:
    matchLabels:
      app: llama-8b                              # 同时覆盖 prefill 与 decode 两组
  targetPorts:
    - number: 8000                               # 只需要一个：两种角色都在 8000
  endpointPickerRef:
    name: llama-8b-epp
    kind: Service
    port:
      number: 9002
```

**只有一个 `targetPorts` 条目**，这是 §1.1 那个统一端口设计的直接结果：prefill 的 vLLM 和 decode 的 sidecar 都监听 8000，池子不必为两种角色区分端口。**只有开 data parallel 时才需要列多个**（`8001..800N`，见 §1.1 末尾）。

`endpointPickerRef` 指向的是 **EPP 的 Service**（不是 Deployment），所以下面必须一并建 Service：

```yaml
apiVersion: v1
kind: Service
metadata: { name: llama-8b-epp, namespace: llm-d }
spec:
  selector: { app: llama-8b-epp }
  type: ClusterIP
  ports:
    - { name: default, port: 9002, targetPort: 9002, appProtocol: http2 }   # ext-proc
    - { name: zmq,     port: 5557, targetPort: 5557, appProtocol: tcp }
    - { name: metrics, port: 9090, targetPort: 9090 }
```

**EPP Deployment + RBAC**（EPP 只读，不需要任何写权限）：

```yaml
apiVersion: apps/v1
kind: Deployment
metadata: { name: llama-8b-epp, namespace: llm-d }
spec:
  replicas: 1                                    # 多副本见下面的 HA 提示
  selector: { matchLabels: { app: llama-8b-epp } }
  template:
    metadata: { labels: { app: llama-8b-epp } }
    spec:
      serviceAccountName: llama-8b-epp
      terminationGracePeriodSeconds: 130           # 要大于 --drain-timeout
      containers:
        - name: epp
          image: ghcr.io/llm-d/llm-d-router-endpoint-picker:latest
          args:
            - --pool-name=llama-8b-pool          # 与 --endpoint-selector 二选一
            - --pool-namespace=llm-d
            - --config-file=/etc/epp/epp-config.yaml
            - --grpc-port=9002
            - --grpc-health-port=9003            # 健康检查独立端口，探针打它
            - --metrics-port=9090
            - --refresh-metrics-interval=50ms
            - --metrics-staleness-threshold=2s
            - --emit-endpoint-scores             # 排障利器，把打分写进 Envoy metadata
            - --v=4                              # 想看 'Calculated score' 就得 4 起
          ports:
            - { containerPort: 9002, name: grpc }
            - { containerPort: 9003, name: grpc-health }
            - { containerPort: 9090, name: metrics }
            - { containerPort: 5557, name: zmq }
          readinessProbe:                        # 注意是 gRPC 探针，不是 HTTP
            grpc:
              port: 9003
              service: envoy.service.ext_proc.v3.ExternalProcessor
            initialDelaySeconds: 5
            periodSeconds: 10
          volumeMounts:
            - { name: epp-config, mountPath: /etc/epp }
      volumes:
        - name: epp-config
          configMap: { name: llama-8b-epp-config }   # 内容就是 §1.2 那份 YAML
---
# 注意是命名空间级的 Role，不是 ClusterRole——EPP 只看自己那个 namespace
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: { name: llama-8b-epp, namespace: llm-d }
rules:
  - apiGroups: ["inference.networking.k8s.io"]      # 新组，EPP 优先读它
    resources: ["inferencepools"]
    verbs: ["get", "watch", "list"]
  - apiGroups: ["inference.networking.x-k8s.io"]    # legacy，上游标了待移除
    resources: ["inferencepools", "inferenceobjectives", "inferencemodelrewrites"]
    verbs: ["get", "watch", "list"]
  - apiGroups: ["llm-d.ai"]
    resources: ["inferenceobjectives", "inferencemodelrewrites"]
    verbs: ["get", "watch", "list"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "watch", "list"]                 # 只读，EPP 从不写 pod
  - apiGroups: ["authentication.k8s.io"]            # 只有开 --metrics-endpoint-auth 才需要
    resources: ["tokenreviews"]
    verbs: ["create"]
  - apiGroups: ["authorization.k8s.io"]
    resources: ["subjectaccessreviews"]
    verbs: ["create"]
---
# 只在开 --ha-enable-leader-election 时才需要，上游拆成了独立的 Role
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: { name: llama-8b-epp-leader-election, namespace: llm-d }
rules:
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["create", "patch"]
```

**这份配置不需要 `--allow-experimental-plugins`**：用到的 `precise-prefix-cache-producer`、`prefix-cache-scorer`、`disagg-profile-handler`、`prefix-based-pd-decider`、`endpoint-notification-source`、`token-producer` **全部是 Beta**（`runner.go:619-708` 逐个都是 `StabilityBeta`）。上游 dev 环境把那个 flag 设成 `true` 是因为它还要跑别的场景（`burst-prefix`、`multicluster-*`、`disaggregated-set-rollout` 这些才是 Alpha）。

**HTTPRoute**（Gateway 模式，把流量指向池子）：

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata: { name: llama-8b, namespace: llm-d }
spec:
  parentRefs: [{ name: inference-gateway }]
  rules:
    - matches:
        - path: { type: PathPrefix, value: / }
      backendRefs:
        - group: inference.networking.k8s.io
          kind: InferencePool
          name: llama-8b-pool
          port: 8000                             # 与 targetPorts 一致
      timeouts:
        request: 30s
```

**升级期的一个坑**：`inference.networking.x-k8s.io` 与 `inference.networking.k8s.io` 两个 CRD 组可以共存，上面的 ClusterRole 也把两个都授权了。但 EPP 的行为是**只读新组、完全忽略 legacy 组**：

```go
// pkg/epp/server/controller_config.go:78, 85（节选）
// EPP will prefer the new group and IGNORE legacy resources.
```

所以升级期间如果只改了旧组的 `InferenceObjective` 对象，**priority 配置会静默不生效**。三个核心 label 也是同理，都有 `inference.networking.k8s.io/*` 的 legacy 别名作为 fallback（`datastore.go:61-63`）。

**多副本的提示**：`replicas: 1` 是刻意的。EPP 跑多副本时，**近似前缀索引不共享**、Flow Control 的容量上限要乘副本数，而本篇用的是精确索引，所以要 HA 就得把 `indexerConfig` 换成 Redis 后端，并保证所有副本的 `hashSeed` / `hashAlgorithm` 一致（04 §4.3）。上游 `docs/operations.md` 的原话：

> **Active-Active mode should be avoided when using approximate prefix routing.** Because EPP replicas do not share prefix state, each replica only has visibility into the prefix state of the requests it has individually handled.

## 2. 端到端 Demo

跑 [00 篇 §2 的统一示例](00-总览与架构.md#2-统一示例贯穿-0007-篇)：2048 token 的共享系统提示 S，请求 A 是 `S + 40 token 的问题`，请求 B 是 `S + 24 token 的问题`。目标是**亲眼看到 B 命中了 A 留下的 KV**。

### 2.1 部署并等就绪

```bash
export NS=llm-d
kubectl apply -f prefill-deploy.yaml -f decode-deploy.yaml \
               -f epp-config.yaml -f epp-deploy.yaml \
               -f inferencepool.yaml -f httproute.yaml
kubectl -n $NS rollout status deploy/llama-8b-prefill deploy/llama-8b-decode deploy/llama-8b-epp
```

### 2.2 冒烟：EPP 看得见所有 endpoint 吗

**这一步不过就不要往下走**，后面所有现象都会是它的次级效应：

```bash
kubectl -n $NS exec deploy/llama-8b-epp -- \
  curl -s localhost:9090/metrics | grep -E 'ready_endpoints|datalayer_.*errors'
```

| 看到什么 | 说明 |
|---------|------|
| `ready_endpoints 6` | 正确：4 个 decode + 2 个 prefill |
| `ready_endpoints` 是 6 的整数倍 | `targetPorts` 列了多个端口但没开 DP，每个 pod 产出了多个 endpoint（§1.1） |
| `datalayer_poll_errors_total` 在涨 | 端口不通：targetPorts 或容器端口配错 |
| `datalayer_extract_errors_total` 在涨 | 指标名不匹配，几乎总是 `llm-d.ai/engine-type` 的问题 |

再确认 ZMQ 那条路通了——**精确索引的前置条件**：

```bash
# vLLM 真的在监听 ZMQ 吗
kubectl -n $NS exec deploy/llama-8b-decode -c vllm -- ss -ltnp | grep -E '5557|5558'
# EPP 的索引里有东西吗（需要 enableMetrics: true）
kubectl -n $NS exec deploy/llama-8b-epp -- curl -s localhost:9090/metrics | grep -i kv_block
```

### 2.3 请求 A：冷启动，走完整的 P/D

```bash
GW=$(kubectl -n $NS get gateway inference-gateway -o jsonpath='{.status.addresses[0].value}')

# 构造 2048 token 的系统提示（粗略按 4 字符 ≈ 1 token）
SYS=$(python3 -c "print('You are a senior Go engineer. Follow these rules strictly. ' * 150)")

curl -s "http://$GW/v1/chat/completions" -H 'Content-Type: application/json' -d "{
  \"model\": \"Qwen/Qwen3-8B\",
  \"messages\": [
    {\"role\": \"system\", \"content\": \"$SYS\"},
    {\"role\": \"user\",   \"content\": \"请解释 sync.Map 的适用场景。\"}
  ]
}" | jq -r '.choices[0].message.content' | head -3
```

这一发请求应该发生的事，按 01 篇的五个阶段：

```
EPP:  阶段 C  Locate → 6 个 endpoint，decode-filter/prefill-filter 各自筛出 4 / 2 个
      阶段 D  token-producer      → 2088 token
              precise producer    → 130 个完整 block，Index.Lookup 全空（首次）
      阶段 E  decode profile  → prefix 分全 0，纯看队列与 KV → 选中某个 D
              prefill profile → decider：未命中 2088 > 512 → 要分离 → 选中某个 P
              primary = decode
sidecar: 读 x-prefiller-host-port → 调 P:8000（max_tokens=1）
         → 拿回 kv_transfer_params → 交给本地 vLLM:8200 → NIXL 拉 KV → 出 token
```

逐条验证：

```bash
# ① prefill pod 真的被用上了（期望 > 0）
kubectl -n $NS exec deploy/llama-8b-prefill -- curl -s localhost:8000/metrics | grep num_requests_running

# ② sidecar 确认进了 P/D 分支，且没有抱怨缺 kv_transfer_params
kubectl -n $NS logs deploy/llama-8b-decode -c pd-sidecar | grep -E 'using P/D protocol|missing'
#    期望：看到 "using P/D protocol"，看不到 "missing 'kv_transfer_params'"
#    看到 missing → vLLM 没启用 KV connector，P/D 白配，decode 自己重算了 prefill

# ③ A 自己不该有缓存命中（它是第一个请求）
kubectl -n $NS exec deploy/llama-8b-epp -- curl -s localhost:9090/metrics | grep request_cached_tokens
```

### 2.4 请求 B：同一个前缀，验证命中

```bash
curl -s "http://$GW/v1/chat/completions" -H 'Content-Type: application/json' -d "{
  \"model\": \"Qwen/Qwen3-8B\",
  \"messages\": [
    {\"role\": \"system\", \"content\": \"$SYS\"},
    {\"role\": \"user\",   \"content\": \"请解释 channel 的关闭语义。\"}
  ]
}" | jq -r '.choices[0].message.content' | head -3
```

这一发应该发生的事**和 A 完全不同**：

```
阶段 D  precise producer → 129 个 block，前 128 块与 A 的 hash 完全相同
                           Index.Lookup → 命中 128 块在 A 落中的那个 D 上
                           matchBlocks=128, totalBlocks=129
阶段 E  decode profile  → prefix-cache-scorer 给那个 D 打 128/129 × 2 = 1.98 分
                           足以盖过队列与 KV 的劣势 → B 落到同一个 D
        prefill profile → decider：未命中只有 24 个 token < 512
                           → 判定不值得分离，不跑 prefill！
```

**两个观察点，一个证明前缀路由生效、一个证明 decider 生效**：

```bash
# ① 前缀命中——这是唯一可靠的验证手段，不能只看配置
kubectl -n $NS exec deploy/llama-8b-epp -- \
  curl -s localhost:9090/metrics | grep request_cached_tokens
#    期望：分布里出现 ~2048 的那一档。仍然全 0 → 查 §3 表里的 #1~#6

# ② A 和 B 落到了同一个 pod（需要 --emit-endpoint-scores，看 Envoy access log 的
#    dynamic metadata，或直接比对两个 decode pod 的请求计数）
kubectl -n $NS logs deploy/llama-8b-epp | grep 'Calculated score'   # 需 -v=4

# ③ B 没有走 prefill：prefill pod 的计数应该还是 A 那一次，没有增加
kubectl -n $NS exec deploy/llama-8b-prefill -- curl -s localhost:8000/metrics | grep num_requests_total
kubectl -n $NS logs deploy/llama-8b-decode -c pd-sidecar | grep 'skip disaggregated prefill'
```

第 ③ 条是这个 demo 最有意思的地方：**前缀缓存命中反过来让 P/D 分离变得不必要了**。B 的 2048 token 已经在本地 decode pod 的 KV 里，只有 24 个 token 需要 prefill，本地算比跨节点搬运更快，于是 decider 走了 06 篇 §2 的分支 ④，sidecar 退化成透明代理。**这两个机制不是各自独立的，它们会互相影响** —— 把 `nonCachedTokens` 改回上游的 16，B 就又会去走 prefill 了，可以改一下 ConfigMap 重启 EPP 对比看。

### 2.5 最后确认没有静默降级

请求都成功 ≠ 一切正常。这三条期望值都是 0：

```bash
# DataProducer 静默失败（前缀命中率归零但请求全成功）
kubectl -n $NS logs deploy/llama-8b-epp | grep -c 'failed to prepare per request data'
# 自定义插件的数据作用域违规
kubectl -n $NS exec deploy/llama-8b-epp -- curl -s localhost:9090/metrics | grep data_scope_violations
# 哪个 filter 把候选砍空了（429 的根因）
kubectl -n $NS logs deploy/llama-8b-epp | grep -c 'Filter eliminated all endpoints'
```

## 3. 静默失效模式总表

**全系列最实用的一张表**，全部是「配了、没报错、但没生效」的情况。前缀路由那一段（#1~#6）尤其值得记，因为它们的唯一症状就是 `request_cached_tokens` 偏低。

| # | 现象 | 症状 | 检查 | 出处 |
|---|------|------|------|------|
| 1 | **漏 `prefixMatchInfoProducerName`** | 以为精确，实际近似 | `request_cached_tokens` 偏低 | 04 §5.3 |
| 2 | **漏 `dataLayer` 里的 endpoint-notification wiring** | 精确索引恒空 | `kv_block` 指标为 0 | 07 §1.2 |
| 3 | **`token-producer` 的 `modelName` 配错** | hash 全不匹配，命中率 0 | 与 vLLM 部署的模型名对比 | 04 §4.3 |
| 4 | **共享前缀短于 block size** | 命中率 0 | 数一下系统提示 token 数 | 04 §9.2 |
| 5 | **近似路线 + 前缀 < 64 token** | 同上（64 是硬下限） | 换精确路线 | 04 §4.2 |
| 6 | **Sliding window / Mamba 模型** | KV 事件被静默跳过 | 模型架构 | 04 §8.3 |
| 7 | **`llm-d.ai/engine-type` 没打 / 打错** | SGLang 按 vLLM 名解析 → 指标全失败 | `datalayer_extract_errors_total` | 03 §3.2 |
| 8 | **自定义 engine mapping 只加一个字段** | 该 engine 的内置指标**全部丢失** | 必须重述整个 engineConfig | 03 §8.4 |
| 9 | **DP 池的 `metrics-data-source` 设了 `port`** | 所有 rank 抓同一端口 | 别设 `port` | 02 §5.7 |
| 10 | **DataProducer 失败** | 前缀命中率归零，请求全成功 | 日志 `failed to prepare per request data` | 01 §3.1 |
| 11 | **配了 `flowControl:` 忘开 gate** | 除 saturationDetector 外全忽略 | 启动日志的 Info 级 WARNING | 02 §7.3 |
| 12 | **负 priority band 未 provision** | fallback 到 0，sheddable 失效 | 显式列出该 band | 05 §10 |
| 13 | **vLLM 未启用 KV connector** | P/D 白配，decode 自己重算 | sidecar 日志 `missing 'kv_transfer_params'` | 06 §3.4 |
| 14 | **`x-kv-cache-source-host-port` 格式错** | P2P pull 不生效，无任何日志 | `chat_completions.go:146-158` | 06 §4.4 |
| 15 | **encoder 全被 SSRF 过滤** | 退回 P/D 或纯 decode | 日志 `SSRF protection: all encoder targets filtered out` | 06 §5 |
| 16 | **`utilization-filter` 未设 `fallbackOnEmpty`** | 尖峰时整池 429 | 生产建议开 | 02 §5.1 |
| 17 | **插件 `Consumes()` 未声明就读数据** | 读取被静默丢弃，行为不对 | `plugin_data_scope_violations_total` | 01 §7 |
| 18 | **双 CRD 组共存** | 只读新组，legacy 全忽略 | `controller_config.go:78, 85` | 07 §1.3 |
| 19 | **Active-Active + 近似前缀** | 命中率腰斩 | 换精确 + Redis | 07 §1.3 |
| 20 | **无 body 的请求** | 完全绕过调度，随机选 pod | `request.go:44-47` | 01 §7 |
| 21 | **DP 场景漏 `llm-d.ai/active-ports` annotation** | 所有 rank 都被当成活跃，滚动启动期打到没就绪的 rank | `ready_endpoints` 与实际就绪 rank 数不符 | 03 §5 |
| 22 | **sidecar 的 `restartPolicy: Always` 漏了** | 它退化成普通 init 容器，跑完就退出 | pod 起不来或 8000 无人监听 | 07 §1.3 |

**与之相对的好消息**：下面这些是**启动期硬失败**，配错了 EPP 根本起不来，不用担心它们静默生效——`--pool-name` 与 `--endpoint-selector` 都给或都不给、插件参数有未知字段、`pluginRef` 指向不存在的插件、配了两个或零个 ProfileHandler、多 profile 用了 `single-profile-handler`、Alpha 插件没加 `--allow-experimental-plugins`、未知的 feature gate 名。

## 4. 三条必记

1. **503 查 K8s，429 查负载与 Filter**——这个分界线是代码里刻意维持的（01 §3.1）。
2. **前缀路由的失败几乎都是静默的**（表 #1~#6）。上线必须用 `request_cached_tokens` 验证，**不能只看配置文件**。
3. **开了 Flow Control 就必须监控 `stale_endpoints`**——饱和检测器是 fail-closed 的，一个采集故障会放大成全池不可用（05 §6.4）。

---

**上一篇**：[06 · P/D 分离与 Sidecar](06-核心代码分析-PD分离与Sidecar.md) ｜ **回到** [README 索引](README.md)
