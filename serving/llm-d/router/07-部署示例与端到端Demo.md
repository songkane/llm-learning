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
| sidecar 两阶段调用 | 06 §2 | decode pod 里的 `routing-sidecar` 容器 |
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
        │ routing-sidecar :8000 ◄───┼─Envoy  │  vLLM      :8000     │
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
| 5557 / 5558 | 两种 pod 的 vLLM（**引擎侧 bind**） | EPP 拨入订阅 / 请求 replay，方向见 §1.4 |
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
      # 和 scorer 一样必须显式指向精确 producer，否则 decider 读的是近似数据
      prefixMatchInfoProducerName: precise-prefix-cache-producer

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

   **注意这个参数有两个地方要写**：`prefix-cache-scorer` 和 `prefix-based-pd-decider` **各有一份**，而且两者的默认值都是"近似 producer"（`prefix_based_pd_decider.go:37-39` 的注释原文：*Empty defaults to the approximate-prefix producer*）。decider 把这个 key 声明成 `Consumes().Required`（`:149-156`），所以漏配不会启动失败——框架会按 `DefaultProducerRegistry` 静默补一个 `approx-prefix-cache-producer`，于是你得到**两个 producer 并存**：scorer 按 block size 16 打分，decider 按 block size 64 判断该不该分离。两套数据、两个粒度，全程无报错。
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
            # topic 必须是 kv@<pod>@<model> 三段式，否则 EPP 一条事件也收不到
            - --kv-events-config={"enable_kv_cache_events":true,"endpoint":"tcp://*:5557","replay_endpoint":"tcp://*:5558","topic":"kv@$(POD_NAME)@Qwen/Qwen3-8B"}
          ports:
            - { containerPort: 8000, name: prefill-http }
            - { containerPort: 5557, name: kv-events }
            - { containerPort: 5558, name: kv-replay }
          env:
            - name: POD_NAME                     # 供上面 topic 里的 $(POD_NAME) 展开
              valueFrom: { fieldRef: { fieldPath: metadata.name } }
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
            - --kv-events-config={"enable_kv_cache_events":true,"endpoint":"tcp://*:5557","replay_endpoint":"tcp://*:5558","topic":"kv@$(POD_NAME)@Qwen/Qwen3-8B"}
          ports:
            - { containerPort: 8200, name: http }
            - { containerPort: 5557, name: kv-events }
            - { containerPort: 5558, name: kv-replay }
          env:
            - name: POD_NAME
              valueFrom: { fieldRef: { fieldPath: metadata.name } }
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
    - { name: metrics, port: 9090, targetPort: 9090 }
```

**这里刻意没有 5557**，尽管上游的 EPP Service 有。原因见下面 §1.4——KV 事件有两种互斥的传输拓扑，本示例用的那种不需要 EPP 监听端口。

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

**`vllm-render`**（`token-producer` 的分词后端，对照 `deploy/environments/dev/base-kind-istio/vllm-render.yaml`）：

```yaml
apiVersion: apps/v1
kind: Deployment
metadata: { name: vllm-render, namespace: llm-d, labels: { app: vllm-render } }
spec:
  replicas: 1
  selector: { matchLabels: { app: vllm-render } }
  template:
    metadata: { labels: { app: vllm-render } }
    spec:
      containers:
        - name: vllm-render
          image: vllm/vllm-openai-cpu:v0.21.0   # CPU 镜像就够，它只分词不推理
          command: ["vllm", "launch", "render"]
          args: ["Qwen/Qwen3-8B", "--port=8082"]
          ports: [{ name: render-http, containerPort: 8082 }]
          readinessProbe:
            httpGet: { path: /health, port: 8082 }
            periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata: { name: vllm-render, namespace: llm-d }
spec:
  selector: { app: vllm-render }
  type: ClusterIP
  ports: [{ name: http, port: 8082, targetPort: 8082 }]
```

**这个组件容易被漏掉**：它不在推理数据通路上，纯粹是给 `token-producer` 提供 HTTP 分词接口用的（§1.2 里 `vllm.url` 指的就是它）。**它不可用 = 请求分不了词 = DataProducer 失败 = 前缀路由静默退化成纯负载均衡**（总表 #10）。用 CPU 镜像是刻意的——分词不需要 GPU，别为它占一张卡。注意 `args` 里的模型名要和 `token-producer.modelName`、以及 KV 事件 topic 的 model 段三者一致。

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

**升级期的一个坑**：`inference.networking.x-k8s.io` 与 `inference.networking.k8s.io` 两个 CRD 组可以共存，上面的 Role 也把两个都授权了。但 EPP 的行为是**只读新组、完全忽略 legacy 组**：

```go
// pkg/epp/server/controller_config.go:78, 85（节选）
// EPP will prefer the new group and IGNORE legacy resources.
```

所以升级期间如果只改了旧组的 `InferenceObjective` 对象，**priority 配置会静默不生效**。三个核心 label 也是同理，都有 `inference.networking.k8s.io/*` 的 legacy 别名作为 fallback（`datastore.go:61-63`）。

**多副本的提示**：`replicas: 1` 是刻意的。EPP 跑多副本时，**近似前缀索引不共享**、Flow Control 的容量上限要乘副本数，而本篇用的是精确索引，所以要 HA 就得把 `indexerConfig` 换成 Redis 后端，并保证所有副本的 `hashSeed` / `hashAlgorithm` 一致（04 §4.3）。上游 `docs/operations.md` 的原话：

> **Active-Active mode should be avoided when using approximate prefix routing.** Because EPP replicas do not share prefix state, each replica only has visibility into the prefix state of the requests it has individually handled.

### 1.4 KV 事件有两种互斥的传输拓扑，别混

这是配精确前缀缓存时最容易搞错的一处，因为**两种模式的 ZMQ 连接方向是相反的**，而配置项长得毫不相干。分叉点在 `producer.go:173-178`：只要 `kvEventsConfig.zmqEndpoint` 非空就走模式 A，否则靠 `podDiscoveryConfig` 走模式 B。

| | 模式 A：全局 socket | 模式 B：逐 pod 发现（**本示例用的**） |
|---|---|---|
| 谁监听、谁连接 | **EPP 监听**，引擎主动连上来 | **引擎监听**，EPP 逐个拨过去 |
| EPP 侧配置 | `kvEventsConfig.zmqEndpoint: tcp://0.0.0.0:5557` | 留空 `zmqEndpoint`，配 `podDiscoveryConfig.socketPort` |
| 引擎侧配置 | 指向 EPP 的 Service，如 `--zmq-endpoint=tcp://<epp-svc>:5557` | 在本 pod 上 bind，如 `tcp://*:5557` |
| EPP 要不要暴露 5557 | **要**（Service + containerPort） | **不要** |
| 代码路径 | `EnsureSubscriber(..., "local-subscriber", ..., remoteSocket=false)` → `sub.Listen()` | `Extract` → `ensureSubscriber` → `sub.Dial()` |
| 支持 replay 吗 | **不支持**（那次调用的 `replayEndpoint` 传的是空串） | 支持，靠 `replaySocketPort` |
| DP 多 rank | 共用一个 socket | 端口按 rank 偏移：`socketPort + rankIndex` |

同一个 `zmqSubscriber` 同时实现了两种：`zmq_subscriber.go:129-144` 按 `remote` 标志决定是 `Listen`（bind）还是 `Dial`（connect）。

**为什么要专门说这件事**：上游 `deploy/` 里的 EPP Service 和 Deployment 都暴露了 5557，因为它的 dev 环境走的是**模式 A**——`deploy/components/overlays/simulator/` 给引擎加的是 `--zmq-endpoint=tcp://${EPP_NAME}.${NAMESPACE}.svc.cluster.local:5557`。如果你照抄那份 Service，却按本示例配了 `podDiscoveryConfig`，就会得到一个**没人连的空监听端口**，同时真正的订阅走的是另一条路。功能上不致命，但会让排障时的端口检查完全误导人。

**选哪个**：要 replay 恢复能力（EPP 重启后不用慢慢重建索引）就只能用模式 B；只是想快速验证、或者引擎侧不方便暴露端口，模式 A 更省事。

### 1.5 vLLM 侧的两个参数，以及那个必须配的 topic

字段以 vLLM 的 `KVTransferConfig`（`vllm/config/kv_transfer.py`）和 `KVEventsConfig`（`vllm/config/kv_events.py`）为准。

**`--kv-transfer-config`** —— 让 vLLM 具备跨实例搬 KV 的能力，P/D 的前提：

| 字段 | 本示例的值 | 说明 |
|------|-----------|------|
| `kv_connector` | `NixlConnector` | 注册名见 `kv_connector/factory.py:176-180`，必须大小写一致 |
| `kv_role` | `kv_both` | 三选一：`kv_producer` / `kv_consumer` / `kv_both`。prefill 只产、decode 只消，但两边都配 `kv_both` 最省事，也便于角色互换 |

**漏了它的后果**：sidecar 的 prefill 请求拿不到 `kv_transfer_params`，decode 侧只能自己重算 prefill——**P/D 白配，但请求全部成功**（总表 #13）。

**`--kv-events-config`** —— 让 vLLM 把 KV 变动广播出来，精确前缀索引的前提：

| 字段 | 默认值 | 本示例的值 | 说明 |
|------|--------|-----------|------|
| `enable_kv_cache_events` | `false` | `true` | 总开关 |
| `publisher` | 随开关自动变 `zmq` | 省略 | `__post_init__` 里开关为 true 时自动置 `zmq`，不用显式写 |
| `endpoint` | **`tcp://*:5557`** | 同默认值 | 引擎 bind 的 PUB 地址。默认值正好就是模式 B 要的 |
| `replay_endpoint` | `None` | `tcp://*:5558` | **不配就没有 replay**，EPP 重启后索引只能靠新流量慢慢重建 |
| `buffer_steps` | `10000` | 省略 | replay 能回放多少步的历史 |
| **`topic`** | **`""`** | **`kv@<pod>@<model>`** | **见下** |

**`topic` 是最容易漏、而且漏了完全静默的一个**。两边的默认值天生不匹配：

```
vLLM  端：topic 默认 ""，且原样作为 ZMQ 消息的第一帧发出（kv_events.py:464）
llm-d 端：SUB socket 订阅过滤器默认 "kv@"（pool.go:153）
          ZMQ 的 SUB 过滤是【首帧前缀匹配】
          "" 不以 "kv@" 开头 → 一条都收不到
```

而且不只是要以 `kv@` 开头，**格式必须是 `kv@<pod-id>@<model-name>` 三段**，因为 llm-d 要从里面切出 pod 身份和模型名：

```go
// pkg/kvevents/engineadapter/common.go:46-52
func parseTopic(topic string) (string, string) {
	topicParts := strings.Split(topic, "@")
	if len(topicParts) == 3 {
		return topicParts[1], topicParts[2]
	}
	return topic, ""      // ← 段数不对就退化：整串当 podID，模型名为空
}
```

段数不对时不报错，只是模型名变成空串。这在模式 A（靠 topic 认 pod）下会直接错乱；模式 B 下 pod 身份由 `SourceEndpoint` 覆盖（`pool.go:397-399`）所以还能work，但模型名依旧是空的。

所以本示例用 downward API 注入 `POD_NAME`，再拼成 `kv@$(POD_NAME)@Qwen/Qwen3-8B`。**model 段要和 `token-producer.modelName` 完全一致**——两边都参与 block hash 的计算，不一致就是命中率恒 0（04 §4.3）。

## 2. 端到端 Demo

跑 [00 篇 §2 的统一示例](00-总览与架构.md#2-统一示例贯穿-0007-篇)：2048 token 的共享系统提示 S，请求 A 是 `S + 40 token 的问题`，请求 B 是 `S + 24 token 的问题`。目标是**亲眼看到 B 命中了 A 留下的 KV**。

### 2.1 部署并等就绪

```bash
export NS=llm-d
kubectl apply -f vllm-render.yaml \
               -f prefill-deploy.yaml -f decode-deploy.yaml \
               -f epp-config.yaml -f epp-deploy.yaml \
               -f inferencepool.yaml -f httproute.yaml
kubectl -n $NS rollout status deploy/vllm-render deploy/llama-8b-prefill \
                              deploy/llama-8b-decode deploy/llama-8b-epp
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

先把两个请求体生成成文件。**不要在 shell 里手拼 JSON**——系统提示有几千字符，引号和换行一定会出问题；用 `jq` 构造，再用 `--data-binary @file` 发：

```bash
GW=$(kubectl -n $NS get gateway inference-gateway -o jsonpath='{.status.addresses[0].value}')
MODEL=Qwen/Qwen3-8B

# 共享的长系统提示。重复次数只是为了凑长度，具体多少 token 下一步实测
SYS=$(python3 -c "print('You are a senior Go engineer. Follow these rules strictly. ' * 150, end='')")

jq -n --arg m "$MODEL" --arg s "$SYS" --arg q '请解释 sync.Map 的适用场景。' \
  '{model:$m, messages:[{role:"system",content:$s},{role:"user",content:$q}]}' > req-a.json
jq -n --arg m "$MODEL" --arg s "$SYS" --arg q '请解释 channel 的关闭语义。' \
  '{model:$m, messages:[{role:"system",content:$s},{role:"user",content:$q}]}' > req-b.json
```

**先量出真实 token 数再对照后面的推演**，别用"4 字符 ≈ 1 token"估：

```bash
# 用 EPP 分词用的那个 render 实例来数，口径和 token-producer 完全一致
TOK() { jq -c '{model:.model, messages:.messages}' "$1" \
  | kubectl -n $NS exec -i deploy/vllm-render -- \
      curl -s localhost:8082/tokenize -H 'Content-Type: application/json' --data-binary @- \
  | jq '.count'; }
A_TOKENS=$(TOK req-a.json); B_TOKENS=$(TOK req-b.json)
echo "A=$A_TOKENS  B=$B_TOKENS  共享前缀≈$((B_TOKENS - 12)) token"

# 精确路线 block size 16 下，各自能切出多少个完整块（残块会被丢弃）
echo "A blocks=$((A_TOKENS / 16))  B blocks=$((B_TOKENS / 16))"
```

拿到真实数字后，这一发请求应该发生的事（下面用 `A_TOKENS` 指代实测值，示意值按 2088 写）：

```
EPP:  阶段 C  Locate → 6 个 endpoint，decode-filter/prefill-filter 各自筛出 4 / 2 个
      阶段 D  token-producer      → A_TOKENS 个 token（示意 2088）
              precise producer    → ⌊A_TOKENS/16⌋ 个完整 block（示意 130），
                                    Index.Lookup 全空（首次）
      阶段 E  decode profile  → prefix 分全 0，纯看队列与 KV → 选中某个 D
              prefill profile → decider：未命中 A_TOKENS > 512 → 要分离 → 选中某个 P
              primary = decode
sidecar: 读 x-prefiller-host-port → 调 P:8000（max_tokens=1）
         → 拿回 kv_transfer_params → 交给本地 vLLM:8200 → NIXL 拉 KV → 出 token
```

```bash
curl -s "http://$GW/v1/chat/completions" -H 'Content-Type: application/json' \
  --data-binary @req-a.json | jq -r '.choices[0].message.content' | head -3
```

逐条验证：

```bash
# ① prefill pod 真的被用上了（期望 > 0）
kubectl -n $NS exec deploy/llama-8b-prefill -- curl -s localhost:8000/metrics | grep num_requests_running

# ② sidecar 确认进了 P/D 分支，且没有抱怨缺 kv_transfer_params
kubectl -n $NS logs deploy/llama-8b-decode -c routing-sidecar | grep -E 'using P/D protocol|missing'
#    期望：看到 "using P/D protocol"，看不到 "missing 'kv_transfer_params'"
#    看到 missing → vLLM 没启用 KV connector，P/D 白配，decode 自己重算了 prefill

# ③ A 自己不该有缓存命中（它是第一个请求）
kubectl -n $NS exec deploy/llama-8b-epp -- curl -s localhost:9090/metrics | grep request_cached_tokens
```

### 2.4 请求 B：同一个前缀，验证命中

```bash
curl -s "http://$GW/v1/chat/completions" -H 'Content-Type: application/json' \
  --data-binary @req-b.json | jq -r '.choices[0].message.content' | head -3
```

这一发应该发生的事**和 A 完全不同**（`SHARED` = 共享系统提示的 token 数，示意 2048）：

```
阶段 D  precise producer → ⌊B_TOKENS/16⌋ 个 block（示意 129），
                           前 ⌊SHARED/16⌋ 块（示意 128）与 A 的 hash 完全相同
                           Index.Lookup → 命中那些块在 A 落中的那个 D 上
                           matchBlocks=128, totalBlocks=129（示意）
阶段 E  decode profile  → prefix-cache-scorer 给那个 D 打 128/129 × 2 ≈ 1.98 分
                           足以盖过队列与 KV 的劣势 → B 落到同一个 D
        prefill profile → decider：未命中 = B_TOKENS − 命中块×16（示意 24）< 512
                           → 判定不值得分离，不跑 prefill！
```

**这里的两个 512 边界要自己核一遍**：A 要走 P/D 需要 `A_TOKENS ≥ 512`，B 要不走 P/D 需要 `B_TOKENS − 命中 token 数 < 512`。用上一步实测的数字代入确认，否则 demo 的两个分支可能都落在同一边。

**两个观察点，一个证明前缀路由生效、一个证明 decider 生效**：

```bash
# ① 前缀命中——这是唯一可靠的验证手段，不能只看配置
kubectl -n $NS exec deploy/llama-8b-epp -- \
  curl -s localhost:9090/metrics | grep request_cached_tokens
#    期望：分布里出现接近共享前缀长度的那一档。仍然全 0 → 查 §3 表里的 #1~#6

# ② A 和 B 落到了同一个 pod（需要 --emit-endpoint-scores，看 Envoy access log 的
#    dynamic metadata，或直接比对两个 decode pod 的请求计数）
kubectl -n $NS logs deploy/llama-8b-epp | grep 'Calculated score'   # 需 -v=4

# ③ B 没有走 prefill：prefill pod 的计数应该还是 A 那一次，没有增加
kubectl -n $NS exec deploy/llama-8b-prefill -- curl -s localhost:8000/metrics | grep num_requests_total
kubectl -n $NS logs deploy/llama-8b-decode -c routing-sidecar | grep 'skip disaggregated prefill'
```

第 ③ 条是这个 demo 最有意思的地方：**前缀缓存命中反过来让 P/D 分离变得不必要了**。B 的那几千个共享 token 已经在本地 decode pod 的 KV 里，只剩几十个 token 需要 prefill，本地算比跨节点搬运更快，于是 decider 走了 06 篇 §2 的分支 ④，sidecar 退化成透明代理。**这两个机制不是各自独立的，它们会互相影响** —— 把 `nonCachedTokens` 改回上游的 16，B 就又会去走 prefill 了，可以改一下 ConfigMap 重启 EPP 对比看。

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
| 6 | **Sliding window / Mamba 模型** | KV 事件被跳过（纯 SWA 全失效、HMA 部分失效） | 看 `kv_cache_events_stores_skipped_total` 指标，**不是静默的** | 04 §8.3 |
| 7 | **`llm-d.ai/engine-type` 没打 / 打错** | SGLang 按 vLLM 名解析 → 指标全失败 | `datalayer_extract_errors_total` | 03 §3.2 |
| 8 | **自定义 engine mapping 只加一个字段** | 该 engine 的内置指标**全部丢失** | 必须重述整个 engineConfig | 03 §8.4 |
| 9 | **DP 池的 `metrics-data-source` 设了 `port`** | 所有 rank 抓同一端口 | 别设 `port` | 02 §5.7 |
| 10 | **DataProducer 失败** | 前缀命中率归零，请求全成功 | 日志 `failed to prepare per request data` | 01 §3.1 |
| 11 | **配了 `flowControl:` 忘开 gate** | 除 saturationDetector 外全忽略 | 启动日志的 Info 级 WARNING | 02 §7.3 |
| 12 | **负 priority band 未 provision** | fallback 到 0，sheddable 失效 | 显式列出该 band | 05 §10 |
| 13 | **vLLM 未启用 KV connector** | P/D 白配，decode 自己重算 | sidecar 日志 `missing 'kv_transfer_params'` | 06 §3.4 |
| 14 | **`x-kv-cache-source-host-port` 格式错 / SSRF 拒绝** | P2P pull 不生效（**有 Info 级日志**，别放弃看日志） | grep `ignoring malformed KV cache source header` 或 `KV cache source not in allowlist` | 06 §4.4 |
| 15 | **encoder 全被 SSRF 过滤** | 退回 P/D 或纯 decode | 日志 `SSRF protection: all encoder targets filtered out` | 06 §5 |
| 16 | **`utilization-filter` 未设 `fallbackOnEmpty`** | 尖峰时整池 429 | 生产建议开 | 02 §5.1 |
| 17 | **插件 `Consumes()` 未声明就读数据** | 读取被静默丢弃，行为不对 | `plugin_data_scope_violations_total` | 01 §7 |
| 18 | **双 CRD 组共存** | 只读新组，legacy 全忽略 | `controller_config.go:78, 85` | 07 §1.3 |
| 19 | **Active-Active + 近似前缀** | 命中率腰斩 | 换精确 + Redis | 07 §1.3 |
| 20 | **无 body 的请求** | 完全绕过调度，随机选 pod | `request.go:44-47` | 01 §7 |
| 21 | **DP 场景漏 `llm-d.ai/active-ports` annotation** | 所有 rank 都被当成活跃，滚动启动期打到没就绪的 rank | `ready_endpoints` 与实际就绪 rank 数不符 | 03 §5 |
| 22 | **sidecar 的 `restartPolicy: Always` 漏了** | 它退化成普通 init 容器，跑完就退出 | pod 起不来或 8000 无人监听 | 07 §1.3 |
| 23 | **`prefix-based-pd-decider` 漏 `prefixMatchInfoProducerName`** | decider 读近似数据、scorer 读精确数据，两个粒度并存 | 看有没有多出一个 `approx-prefix-cache-producer` | 07 §1.2 |
| 24 | **ZMQ 两种拓扑混配** | EPP 上一个没人连的 5557 空监听，真正订阅走另一条路 | `zmqEndpoint` 与 `podDiscoveryConfig` 只该配一个 | 07 §1.4 |
| 25 | **vLLM 的 `topic` 没配成 `kv@…` 三段式** | SUB 过滤器不匹配，**一条 KV 事件都收不到**，索引恒空 | `kv_block` 指标为 0；vLLM 侧 `topic` 默认是空串 | 07 §1.5 |
| 26 | **漏配 `replay_endpoint`** | EPP 重启后索引只能靠新流量重建，期间命中率为 0 | vLLM 侧默认 `None` | 07 §1.5 |
| 27 | **`vllm-render` 没部署或不可用** | 请求分不了词 → DataProducer 失败 → 退化成纯负载均衡 | 日志 `failed to prepare per request data` | 07 §1.3 |

**与之相对的好消息**：下面这些是**启动期硬失败**，配错了 EPP 根本起不来，不用担心它们静默生效——`--pool-name` 与 `--endpoint-selector` 都给或都不给、插件参数有未知字段、`pluginRef` 指向不存在的插件、配了两个或零个 ProfileHandler、多 profile 用了 `single-profile-handler`、Alpha 插件没加 `--allow-experimental-plugins`、未知的 feature gate 名。

## 4. 三条必记

1. **503 查 K8s，429 查负载与 Filter**——这个分界线是代码里刻意维持的（01 §3.1）。
2. **前缀路由的失败几乎都是静默的**（表 #1~#6）。上线必须用 `request_cached_tokens` 验证，**不能只看配置文件**。
3. **开了 Flow Control 就必须监控 `stale_endpoints`**——饱和检测器是 fail-closed 的，一个采集故障会放大成全池不可用（05 §6.4）。

---

**上一篇**：[06 · P/D 分离与 Sidecar](06-核心代码分析-PD分离与Sidecar.md) ｜ **回到** [README 索引](README.md)
