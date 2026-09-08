# 06 · 部署与 Operator：这一套在 K8s 上长什么样

> **源码基线**：`main @ 946acce`。前五篇讲的都是「跑起来之后怎么工作」，本篇讲「怎么把它跑起来」。
> 主要文件：`deploy/operator/`（Go 写的 Operator）、`deploy/helm/`、`deploy/inference-gateway/`、`recipes/`

## 0. 两条入口路径

[00 篇](00-总览与架构.md) 说过 Dynamo 有两种请求入口拓扑，它们的部署形态差别很大：

| | Dynamo 原生 Frontend | Gateway API + GAIE |
|---|---|---|
| 请求流 | `client → Frontend → Router → workers` | `client → Gateway → EPP → Frontend sidecar(direct) → workers` |
| 谁选 worker | Frontend 内嵌的 Router | **Gateway 侧的 EPP 插件** |
| Frontend 角色 | 独立 Deployment，集群入口 | **降级成每个 worker pod 里的 sidecar**，`--router-mode direct` |
| 适合 | 本地开发、单集群、Dynamo 想独占入口 | 平台已标准化在 Gateway API 上，边缘要做鉴权/限流/可观测 |

第二条路径就是 [01 篇 §5.1](01-请求的一生-主控制流.md#51-七种-router-mode) 里 `direct` 模式的用武之地，也是 [01 篇 §1.1](01-请求的一生-主控制流.md#11-三种起法) 强调「Frontend 是库」的意义所在。

## 1. 六个 CRD

Group 统一是 **`nvidia.com`**（`api/v1alpha1/groupversion_info.go:32`），`v1alpha1` 和 `v1beta1` 并存并有 conversion webhook（`api/CONVERSION.md`，还配了 roundtrip fuzz 测试）。

```
deploy/operator/config/crd/bases/
  nvidia.com_dynamographdeployments.yaml
  nvidia.com_dynamographdeploymentrequests.yaml
  nvidia.com_dynamocomponentdeployments.yaml
  nvidia.com_dynamographdeploymentscalingadapters.yaml
  nvidia.com_dynamomodels.yaml
  nvidia.com_dynamoworkermetadatas.yaml
```

| Kind | 缩写 | 谁写 | 干什么 |
|---|---|---|---|
| **DynamoGraphDeployment** | DGD | 用户 | **主对象**：一张服务图，`spec.services` 里每个组件一项 |
| **DynamoGraphDeploymentRequest** | DGDR | 用户 | **意图对象**：给 model + SLA，让系统自己搜配置生成 DGD |
| **DynamoComponentDeployment** | DCD | operator | DGD 拆出来的单组件，再变成 Deployment |
| **DynamoGraphDeploymentScalingAdapter** | DGDSA | operator | 扩缩容适配器，实现标准 `scale` 子资源 |
| **DynamoModel** | — | 用户 / operator | LoRA / adapter 发现，`status.endpoints[]` |
| **DynamoWorkerMetadata** | DWM | **worker 进程自己** | **服务发现载体**，见 §3 |

最后一个是理解 Dynamo K8s 部署的关键：**它不是 operator 写的，是每个 worker 在运行时自己写的**。

### 1.1 DGD Spec

```85:93:deploy/operator/api/v1alpha1/dynamographdeployment_types.go
type DynamoGraphDeploymentSpec struct {
    Services map[string]*DynamoComponentDeploymentSharedSpec `json:"services,omitempty"`
    BackendFramework string `json:"backendFramework,omitempty"` // sglang|vllm|trtllm
    PVCs []PVC `json:"pvcs,omitempty"`
    Experimental *DynamoGraphDeploymentExperimentalSpec `json:"experimental,omitempty"`
}
```

**`services` 是个 map 而不是 list**，key 就是组件名（`Frontend`、`prefill`、`decode`）。每项带 `componentType`（frontend / worker）和 `subComponentType`（prefill / decode / encode），这两个字段决定 operator 怎么处理它。

### 1.2 DGDR Spec

```143:150:deploy/operator/api/v1alpha1/dynamographdeploymentrequest_types.go
type DynamoGraphDeploymentRequestSpec struct {
    Model string `json:"model"`
    Backend string `json:"backend"`   // auto|vllm|sglang|trtllm
    ProfilingConfig ProfilingConfigSpec `json:"profilingConfig"`
    AutoApply bool `json:"autoApply,omitempty"`
    DeploymentOverrides *DeploymentOverridesSpec `json:"deploymentOverrides,omitempty"`
}
```

状态机 `Initializing → Pending → Profiling → Deploying → Ready | Failed`（`:97`）。完整链路在 [03 篇 §8](03-核心代码分析-SLA-Planner.md#8-dgdr零配置部署的完整链路) 讲过。

**`autoApply: false` 是个实用的安全阀**：跑完 profiling 生成 DGD spec 但不部署，人看过再手动应用。生产环境第一次用 DGDR 建议这么来。

## 2. DGD 被 reconcile 成什么

入口 `DynamoGraphDeploymentReconciler.Reconcile`（`internal/controller/dynamographdeployment_controller.go:118`），先分流（`dgd_workload_program.go:75`）：

```
selectWorkloadProgram
  ├── Grove pathway      → PodCliqueSet / PodClique / PodCliqueScalingGroup
  └── Component pathway  → DynamoComponentDeployment → Deployment / LeaderWorkerSet
```

Grove 那条是给 NVL72 那种需要拓扑感知 gang 调度的场景准备的（[ai-dynamo/grove](https://github.com/ai-dynamo/grove)，独立仓库，不在本系列）。

**两条路径共用的 shared resources**（`dgd_shared_resources_reconciler.go:71~111`）：

| # | 资源 |
|---|---|
| 1 | RBAC：ServiceAccount / Role / RoleBinding |
| 2 | PVC |
| 3 | **K8s discovery 专用 RBAC**（`dgd_discovery_reconciler.go:58`）——worker 要能写 DWM CR |
| 4 | GMS ResourceClaimTemplate |
| 5 | Checkpoint（SnapshotJob 相关） |
| 6 | **EPP / GAIE 资源**（`dgdEPPReconciler`） |
| 7 | wait-leader ConfigMap |
| 8 | SSH keys Secret |

**Component pathway**（`dgd_component_program.go:66~143`）每个 service 产出：

- `DynamoComponentDeployment` → **Deployment** + **Service**
- 多节点组件 → **LeaderWorkerSet**（需要 LWS feature gate）
- 按 `modelRef` 建 **Headless Service**（`model_service.go:40`）
- **DynamoGraphDeploymentScalingAdapter**
- 可选 **InferencePool**（GAIE 用）

> **EndpointSlice 不是 operator 建的**。operator 只建 Headless Service，EndpointSlice 由 K8s 自己的 Service controller 生成。注意 `model_service.go:172` 那句 `PublishNotReadyAddresses: false`——**没 ready 的 pod 不出现在 EndpointSlice 里**，这直接决定了 §3 的发现语义。

## 3. K8s 原生服务发现：不用 etcd 的实现

这是 1.x 最实在的一个改进。[00 篇 §3](00-总览与架构.md#3-架构的第一刀三个独立的通信平面) 说发现平面可以选 kubernetes 后端，具体是这么实现的：

### 3.1 写入侧（worker 进程）

`lib/runtime/src/discovery/kube.rs:235~257`：

1. worker 注册 endpoint / model card → 更新进程内的 `DiscoveryMetadata`
2. `build_cr()` + **`apply_cr()`**（Server-Side Apply）把它写成一个 **`DynamoWorkerMetadata` CR**（`crd.rs:168`）

### 3.2 读取侧（discovery daemon）

`lib/runtime/src/discovery/kube/daemon.rs` 同时 watch 两样东西：

| watch | 位置 | 提供什么 |
|---|---|---|
| **EndpointSlice** | `:122`，label selector `nvidia.com/dynamo-discovery-backend=kubernetes` | **谁活着**（readiness + IP） |
| **DynamoWorkerMetadata CR** | `:230` | **它是什么**（模型、endpoint、transport、codec） |

然后做 **join**（`daemon/state.rs` 的 join table）：EndpointSlice 的 readiness × CR 的 metadata → `DiscoveryEvent::Added/Removed`。`extract_endpoint_info()`（`kube/utils.rs:98`）从 EndpointSlice 里取 `(instance_id, cr_name, pod_uid)` 作关联键。

### 3.3 为什么要拆成两个对象

这个设计初看绕，其实很合理——**它把「存活性」和「身份」交给了各自最擅长的机制**：

- **存活性**用 EndpointSlice：kubelet 的 readiness probe 本来就在做这件事，而且做得比任何应用层心跳都可靠（它能看到进程崩溃、节点失联、被驱逐）。这正是 etcd 模式下需要 lease + keep-alive 才能勉强模拟的东西。
- **身份**用 CR：模型名、endpoint 列表、transport 地址、codec 这些是应用层信息，K8s 不懂，只能自己写。CR 还自带 SSA、版本、RBAC 和 watch。

**两者 join 才算数**：CR 在但 EndpointSlice 里没有（pod 没 ready 或已死）→ 不可用；EndpointSlice 有但 CR 还没写好（进程刚起）→ 不可用。这个 join 天然消除了「注册了但还不能服务」和「已经死了但注册还在」两个经典窗口期。

对比 etcd 模式：靠 lease TTL 过期摘除，TTL 短则误摘、长则残留，永远在两难之间。**用 K8s 原生机制的价值就在这里。**

`946accea5e` 附近的提交 `#14294` 还把这个 join 从 debounce 改成了 join table，说明这块在持续打磨。

operator 侧对应的动作是建 discovery RBAC（`dgd_discovery_reconciler.go`），以及 DynamoModel controller 读 EndpointSlice 更新 `status.endpoints`（`modelendpoint/discovery.go:34`）。

> **注意发现平面和请求平面是正交的**（00 篇 §3）。用 K8s 发现，请求平面照样可以配成 NATS；反过来也行。

## 4. GAIE 路径

`deploy/inference-gateway/` 下有两块：`ext-proc/`（EPP 插件）和 `sidecar/`（塞进 worker pod 的 Frontend）。

### 4.1 `ext-proc/`：EPP 插件的两种模式

实现的是 **Gateway API Inference Extension** 的 ext_proc 协议（`proto/envoy/service/ext_proc/v3/external_processor.proto`）。

| 模式 | 位置 | 组成 |
|---|---|---|
| **Embedded Router** | `ext-proc/src/epp.rs:111` | 完整 `DistributedRuntime` + `PrefillRouter` + `ManagedKvRouter`——**和集群内 Frontend 用同一套 KV 打分** |
| **Standalone EppRouter** | `ext-proc/src/epp_router.rs:6` | **不要 etcd / NATS**：`PodDiscovery` + `Selector` + `VllmRenderClient`（自己 tokenize）→ 输出 Envoy routing header |

第二种模式正是 [02 篇 §0](02-核心代码分析-KV感知路由.md#0-先分清两个-kv_router) 说的「把 `lib/kv-router` 拆成独立 crate」的收益兑现：**EPP 可以只带算法内核，不背整个分布式运行时**。`EppRouter::from_selector`（`:61`）把 KV router 的 selection service 和 K8s pod reflector 组装起来。

注意 standalone 模式要**自己 tokenize**（`VllmRenderClient`）。这就回到了 [01 篇 §5](01-请求的一生-主控制流.md#5-路由决策在最后一环) 讲的那个结构性差异——路由要精确 KV 匹配就必须有 token，Frontend 内嵌时顺手就有，搬到 Gateway 边缘就得自己再算一遍。**GAIE 路径为标准化付出的代价，就是这一次重复 tokenize。**

### 4.2 `sidecar/`：decode pod 里的那个 Frontend

`deploy/inference-gateway/sidecar/README.md:6~18`：

- pod 内 HTTP `:8000` → decode engine `:8001`
- 带 `x-prefiller-host-port` header 时走 P/D adapter；否则直通 decode

> **当前状态**：后端 P/D adapter **部分还是 501 未实现**（README `:34`），decode 直通可用。**GAIE + P/D 分离这个组合现在不完整**，选型时要注意。

## 5. `recipes/`：36 个开箱配置

结构是 `recipes/<模型>/<后端>/<拓扑>/deploy.yaml`。以 `recipes/deepseek-r1/sglang/` 为例：

```
README.md          16×/32× H200，基于 SGLang WideEP
deepep.json        专家并行配置
disagg-8gpu/deploy.yaml     Frontend + prefill + decode，各 8 GPU
disagg-16gpu/deploy.yaml    各 16 GPU
```

一个典型 DGD 长这样：

```4:37:recipes/deepseek-r1/sglang/disagg-8gpu/deploy.yaml
apiVersion: nvidia.com/v1alpha1
kind: DynamoGraphDeployment
metadata:
  name: sgl-dsr1-8gpu
spec:
  envs:
    - name: HF_HOME
      value: /opt/model
  pvcs:
    - name: model-cache
      create: false
  services:
    Frontend:
      componentType: frontend
      replicas: 1
      volumeMounts:
        - name: model-cache
          mountPoint: /opt/model
      extraPodSpec:
        mainContainer:
          image: nvcr.io/nvidia/ai-dynamo/sglang-runtime:1.3.0
    decode:
      componentType: worker
      subComponentType: decode
      replicas: 1
      resources:
        limits:
          gpu: "8"
      volumeMounts:
        - name: model-cache
          mountPoint: /opt/model
      sharedMemory:
        size: 80Gi
      extraPodSpec:
        mainContainer:
          image: nvcr.io/nvidia/ai-dynamo/sglang-runtime:1.3.0
          workingDir: /workspace
          command:
            - python3
            - -m
            - dynamo.sglang
```

`prefill` 那一段是对称的，只是 `subComponentType: prefill` 且 args 里 `--disaggregation-mode prefill`。

三个部署实践点：

1. **`sharedMemory.size: 80Gi`** 不是可选项。`/dev/shm` 不够大，多进程引擎会在加载权重或 NCCL 初始化时莫名失败，报错还往往指向别处。
2. **`pvcs: create: false`** 表示复用已有的模型缓存 PVC。冷启动拉 DeepSeek-R1 的权重是以十分钟计的，**共享 PVC 基本是生产必需**。
3. **`replicas: 1` 但 `gpu: "8"`**：一个副本占满一台 8 卡机。这是张量并行的组织方式——**扩容的单位是「机」不是「卡」**，[03 篇](03-核心代码分析-SLA-Planner.md) 里 Planner 算出来的副本数要乘以这个系数才是 GPU 消耗。

recipe 里的镜像 tag（`1.3.0`）常常落后于代码基线，用之前先对一下版本。

## 6. 从零到能发请求

按依赖顺序：

| 步 | 做什么 | 备注 |
|---|---|---|
| 1 | 装 CRD + operator（Helm，`deploy/helm/`） | 六个 CRD 见 §1 |
| 2 | 准备模型 PVC | 见 §5 第 2 点 |
| 3 | 应用 DGD（抄 recipe）或 DGDR（零配置） | DGDR 首次建议 `autoApply: false` |
| 4 | operator reconcile 出 Deployment / Service / DGDSA | §2 |
| 5 | worker 起来，写 DWM CR；Service controller 生成 EndpointSlice | §3 |
| 6 | Frontend 的 ModelWatcher join 到实例，现场组装 pipeline | [01 篇 §2](01-请求的一生-主控制流.md#2-模型是被发现出来的不是配置出来的) |
| 7 | 发请求 | |

**排障时最该先看的三件事**（按 §3 的 join 语义推出来）：

1. **DWM CR 在不在**（`kubectl get dynamoworkermetadatas`）——不在说明 worker 没注册成功，多半是 discovery RBAC 或 `DYN_DISCOVERY_BACKEND` 没配对。
2. **EndpointSlice 里有没有这个 pod** ——没有说明 readiness probe 没过，是引擎本身的问题（加载权重、显存不足）。
3. **两个都在但 Frontend 还是不知道**——join 键对不上，看 `instance_id` / `pod_uid`。

这三步能把「模型没出现在 `/v1/models` 里」这类最常见的问题定位到具体环节。

## 7. 必记要点

1. **六个 CRD，`DynamoWorkerMetadata` 是 worker 自己写的**，不是 operator 写的。
2. **DGD 的 `services` 是 map**，`componentType` + `subComponentType` 决定 operator 怎么处理。
3. **两条 reconcile 路径**：Component（Deployment / LWS）和 Grove（PodClique，拓扑 gang）。
4. **EndpointSlice 由 K8s 自己生成**，operator 只建 Headless Service，且 `PublishNotReadyAddresses: false`。
5. **K8s 原生发现 = EndpointSlice（存活性）× DWM CR（身份）做 join**。把「谁活着」交给 kubelet 的 readiness，比 etcd lease TTL 可靠得多，也天然消掉了两个注册窗口期。
6. **GAIE 的 standalone EPP 不需要 etcd/NATS**，但要自己 tokenize——这是为标准化付的代价。
7. **GAIE + P/D 分离目前不完整**（sidecar 的 P/D adapter 部分 501）。
8. **DGDR 首次用配 `autoApply: false`**，人看过生成的 spec 再部署。
9. **`sharedMemory` 必配、模型 PVC 必共享、扩容单位是「机」不是「卡」**。
10. **排障三连**：DWM CR 在吗 → EndpointSlice 里有吗 → join 键对得上吗。

---

> **上一篇** [05 · P/D 分离与 NIXL](05-核心代码分析-PD分离与NIXL.md) ｜ **下一篇** [07 · 横向对比](07-横向对比-与llm-d和Mooncake.md)
> **回到** [Dynamo 系列首页](README.md)
