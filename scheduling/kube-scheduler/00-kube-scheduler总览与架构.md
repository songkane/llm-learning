# kube-scheduler 总览与架构

> 本篇回答：**K8s 原生调度器做什么、由哪些部分组成、一个 Pod 从创建到落到节点上经过了什么。**
>
> **源码基线：`kubernetes/kubernetes` v1.36.3**（最新稳定 release）
>
> ```bash
> cd $GOPATH/src/k8s.io/kubernetes
> git checkout v1.36.3
> git describe --tags        # v1.36.3
> ```
>
> **本系列的组织方式**：00~03 篇是**通用原理**，只讲调度器本身怎么工作，用普通工作负载做例子；04 篇起才进入特定场景（大模型训练/推理）分析它的能力与短板；06~07 篇讲怎么扩展它。如果你是为了解决某个具体场景问题而来，也建议先读完 00~01 建立地基。
>
> 版本差异见 [附录：版本说明](#附录版本说明)。

---

## 1. 定位：一句话与一个反直觉

> kube-scheduler 是一个**为单个 Pod 选择节点**的控制器：从调度队列取出一个 Pod，过滤出可行节点，打分选最优，然后把 `pod.spec.nodeName` 写回 apiserver。

一句话概括它的职责边界：

| 它做 | 它不做 |
|------|--------|
| 决定 Pod 放在**哪个节点** | 决定 Pod **能不能创建**（那是 ResourceQuota / admission 的事） |
| 基于 `requests` 做资源账本 | 关心容器实际用了多少（那是 kubelet / VPA 的事） |
| 写 `pod.spec.nodeName` | 拉镜像、起容器（那是 kubelet 的事） |
| 优先级抢占 | 保证 Pod 一直运行（那是 controller 的事） |

它的核心设计假设是「**Pod 之间彼此独立**」—— 每个 Pod 独立决策、互不影响。这个假设让它极其可扩展（官方规模目标 5000 节点 / 15 万 Pod），也是它一切能力与局限的根源。

**一个反直觉的事实**：从 v1.36 开始，主干代码里出现了「成组调度」的分支：

```go
// pkg/scheduler/schedule_one.go
func (sched *Scheduler) ScheduleOne(ctx context.Context) {
    podInfo, err := sched.NextPod(logger)
    ...
    if sched.genericWorkloadEnabled && podInfo.Pod.Spec.SchedulingGroup != nil {
        podGroupInfo, err := sched.podGroupInfoForPod(ctx, podInfo)
        ...
        sched.scheduleOnePodGroup(ctx, podGroupInfo)     // 成组调度分支
    } else {
        sched.scheduleOnePod(ctx, podInfo)               // 经典的逐 Pod 分支
    }
}
```

不过 `genericWorkloadEnabled` 对应的 feature gate **默认关闭**，所以**默认行为仍然是逐 Pod 调度**。本篇 §6 客观介绍这条新路径的存在与结构，它的适用性与取舍留到 [04 篇](04-面向大模型场景的能力与局限.md) 讨论。

### 1.1 贯穿全篇的示例

00~03 篇统一用这个**普通集群**做例子（04 篇起换成 GPU 集群，与 Volcano/Kueue 系列对齐）：

6 个节点，每台 8 核 16 Gi：

| 节点 | CPU 空闲 | 内存空闲 | 特点 |
|------|---------|---------|------|
| node-1 | 0.5 / 8 | 1 / 16 Gi | 已被占满 |
| node-2 | 1 / 8 | 2 / 16 Gi | 快满了 |
| node-3 | 8 / 8 | 16 / 16 Gi | 空闲，但有污点 `maintenance=true:NoSchedule` |
| node-4 | 6 / 8 | 12 / 16 Gi | 空闲 |
| node-5 | 6 / 8 | 12 / 16 Gi | 空闲 |
| node-6 | 6 / 8 | 12 / 16 Gi | 空闲，**已缓存应用镜像** |

负载：一个 `web` Deployment（3 副本，每个 2 核 4 Gi，挂 PVC）。

用它可以观察到调度器的全部关键行为 —— 资源过滤、污点排除、打分决胜、副本打散、卷绑定。[01 篇 §6](01-核心原理-调度周期与扩展点.md) 会用这个示例把 15 个扩展点完整走一遍并手算分数。

---

## 2. 它的边界：与 Volcano / Kueue 的分工

读原生调度器时容易产生的疑问是「那为什么还有 Volcano、Kueue」。一句话划清边界：

```mermaid
flowchart LR
    A["作业提交"] --> B["Kueue：够配额吗？<br/>（作业级准入）"]
    B -->|放行| C{"schedulerName?"}
    C -->|default-scheduler| D["kube-scheduler<br/>逐 Pod 选节点"]
    C -->|volcano| E["Volcano<br/>成组选节点"]
    D --> F["kubelet 拉起容器"]
    E --> F
```

| | **kube-scheduler** | **Kueue** | **Volcano** |
|--|-------------------|-----------|-------------|
| 层次 | Pod 级**调度**（选节点 + Bind） | 作业级**准入** | Pod 级**调度**（替代 kube-scheduler） |
| 执行手段 | 写 `pod.spec.nodeName` | 改 `job.spec.suspend` / 摘 scheduling gate | 写 `pod.spec.nodeName` |
| 多租户配额 | ❌ 只有 `ResourceQuota`（硬上限、不可借用、超出直接拒绝创建） | ✅ | ✅ |
| 节点级过滤/打分 | ✅ **它就是标准** | ❌ 不做 | ✅（复用 kube-scheduler 的插件代码） |
| 是否可共存 | — | Kueue 放行后由它调度 | 与它并存（按 `schedulerName` 分流） |

两个要点：

1. **kube-scheduler 与 Volcano 是互斥的**，由 `pod.spec.schedulerName` 决定归属，同一个 Pod 只会被一个调度器决策和 Bind。
2. **它是地基**：Volcano 的 `predicates` / `nodeorder` 插件直接 import 了 `k8s.io/kubernetes/pkg/scheduler/framework/plugins`，Kueue 则把 Pod 交还给它调度。**读懂 kube-scheduler 是读懂另外两个的前提。**

完整的三方能力矩阵与选型建议见 [04 篇 §5](04-面向大模型场景的能力与局限.md) 与 [scheduling/README](../README.md)。

---

## 3. 组件与源码结构

kube-scheduler 是**单一进程**（`kube-scheduler` 二进制），多副本时靠 leader election 选主。

```mermaid
flowchart TB
    subgraph API["apiserver"]
        P["Pod（spec.nodeName 为空）"]
        N["Node / PV / PVC / PodGroup ..."]
    end

    subgraph SCHED["kube-scheduler 进程"]
        direction TB
        EH["eventhandlers.go<br/>informer 事件 → 队列/缓存"]
        Q["backend/queue<br/>activeQ / backoffQ / unschedulablePods"]
        C["backend/cache<br/>NodeInfo 账本 + assumedPods"]
        SNAP["nodeInfoSnapshot<br/>本轮快照"]
        SO["schedule_one.go<br/>ScheduleOne 主循环"]
        FW["framework/runtime<br/>frameworkImpl：跑各扩展点"]
        PL["framework/plugins<br/>25 个内置插件"]
        AD["backend/api_dispatcher<br/>异步 API 调用（v1.34+）"]
    end

    P --> EH --> Q
    N --> EH --> C
    C -->|UpdateSnapshot| SNAP
    Q -->|Pop| SO
    SNAP --> SO
    SO --> FW --> PL
    SO -->|assume| C
    SO -->|Bind| AD --> API
```

### 3.1 源码导航（v1.36 路径，注意与老版本不同）

```
cmd/kube-scheduler/                    # 入口：Setup() / Run()
pkg/scheduler/
├── scheduler.go                       # Scheduler 结构体、New()、Run()
├── schedule_one.go                    # ★ ScheduleOne：调度周期 + 绑定周期
├── schedule_one_podgroup.go           # ★ v1.36 新增：PodGroup 成组调度
├── eventhandlers.go                   # informer 事件处理
├── extender.go                        # HTTP Extender（老机制，仍保留）
├── backend/                           # ★ v1.31+ 从 pkg/scheduler 拆出来
│   ├── queue/                         #   调度队列（三个队列）
│   ├── cache/                         #   集群状态缓存 + Snapshot
│   ├── heap/                          #   带索引的堆
│   ├── api_cache/                     #   v1.34+ 异步 API 的本地视图
│   └── api_dispatcher/                #   v1.34+ 异步 API 调用
├── framework/
│   ├── runtime/framework.go           # ★ frameworkImpl：扩展点编排
│   ├── preemption/preemption.go       # ★ 抢占通用逻辑（Evaluator）
│   ├── plugins/                       # ★ 内置插件
│   │   ├── noderesources/ nodeaffinity/ interpodaffinity/
│   │   ├── podtopologyspread/ tainttoleration/ volumebinding/
│   │   ├── defaultpreemption/ defaultbinder/ schedulinggates/
│   │   ├── dynamicresources/          #   DRA
│   │   ├── gangscheduling/            #   ★ v1.36 新增
│   │   ├── topologyaware/             #   ★ v1.36 新增（Placement）
│   │   ├── podgrouppodscount/         #   ★ v1.36 新增（Placement 打分）
│   │   └── nodedeclaredfeatures/      #   ★ 新增
│   └── types.go / cycle_state.go ...
├── apis/config/                       # KubeSchedulerConfiguration
├── metrics/                           # Prometheus 指标
└── profile/                           # 多 profile（多 schedulerName）

staging/src/k8s.io/kube-scheduler/
└── framework/interface.go             # ★ 所有扩展点接口定义（对外 API）
```

> **两个路径陷阱**：① 队列和缓存在 `pkg/scheduler/backend/` 下（老版本在 `pkg/scheduler/internal/`）；② 扩展点接口定义在 **staging** 的 `k8s.io/kube-scheduler/framework/interface.go`，不在 `pkg/scheduler/framework/interface.go`。写自定义插件时 import 的是前者。

---

## 4. 一个 Pod 的一生

```mermaid
sequenceDiagram
    autonumber
    participant U as 用户/控制器
    participant API as apiserver
    participant EH as eventhandlers
    participant Q as SchedulingQueue
    participant SO as ScheduleOne
    participant C as Cache
    participant K as kubelet

    U->>API: 创建 Pod（spec.nodeName 为空）
    API->>EH: Pod Add 事件
    EH->>Q: 入 activeQ（先过 PreEnqueue）

    loop 串行、一个接一个
        SO->>Q: ① NextPod()（阻塞式 Pop）
        SO->>C: ② UpdateSnapshot()（增量刷快照）
        Note over SO: ③ schedulingAlgorithm：<br/>PreFilter → Filter → PostFilter?<br/>→ PreScore → Score → 选最高分
        alt 找到节点
            SO->>C: ④ assume()：缓存里先记上<br/>（乐观假设，不等 API）
            Note over SO: ⑤ Reserve → Permit
            SO->>SO: ⑥ go runBindingCycle()（★ 异步）
            Note over SO: WaitOnPermit → PreBind<br/>→ Bind → PostBind
            SO->>API: Bind：写 pod.spec.nodeName
            API->>K: kubelet watch 到，拉起容器
        else 没有可行节点
            Note over SO: PostFilter（抢占）：<br/>找受害者 → 驱逐 → 设 NominatedNodeName
            SO->>Q: ⑦ 进 unschedulablePods，<br/>等事件触发或超时回 activeQ
        end
    end
```

**这张图里有四个必须理解的设计**：

### 4.1 调度周期串行，绑定周期并行

```go
// schedule_one.go
scheduleResult, assumedPodInfo, status := sched.schedulingCycle(...)   // 同步、串行
if !status.IsSuccess() { ... ; return }
// bind the pod to its host asynchronously (we can do this b/c of the assumption step above).
go sched.runBindingCycle(ctx, state, fwk, scheduleResult, assumedPodInfo, start, podsToActivate)
```

**调度周期（选节点）必须串行** —— 否则两个 Pod 会同时看到同一份空闲资源。**绑定周期（写 API、等 PV 绑定）可以并行** —— 因为 `assume` 已经把资源在缓存里扣掉了。

这是 kube-scheduler 吞吐的关键：慢操作（PVC 绑定、DRA 分配、apiserver 往返）都在异步的绑定周期里。

### 4.2 assume：乐观并发

```go
// assumeAndReserve
assumedPodInfo := podInfo.DeepCopy()
// assume modifies `assumedPod` by setting NodeName=scheduleResult.SuggestedHost
err := sched.assume(logger, state, assumedPodInfo, scheduleResult.SuggestedHost)
```

在 apiserver 确认之前，缓存里就认为「这个 Pod 已经在这个节点上了」。绑定失败则 `ForgetPod` 回滚。这让调度器不必等 API 往返就能处理下一个 Pod。

> 三个系列里都有 assume 的身影：Volcano 是 `Statement` + `Commit`，Kueue 是 `cache.AddOrUpdateWorkload` + 异步 patch，kube-scheduler 是 `assume` + `ForgetPod`。**「先在内存记账，再异步落 API，失败回滚」是调度器的共同范式。**

### 4.3 抢占不是当轮生效

`PostFilter`（`DefaultPreemption`）做的事是：找到受害者 → 发起驱逐 → 给抢占者设 `pod.status.nominatedNodeName` → **本轮结束，Pod 回队列**。下一轮它才可能真正调度成功。

### 4.4 队列有三个，不是一个

Pod 调度失败后不会立刻重试，而是进入退避体系（§5）。

---

## 5. 三个队列

```mermaid
flowchart LR
    NEW["新 Pod"] -->|PreEnqueue 通过| AQ["activeQ<br/>（堆，按 QueueSort 排序）"]
    NEW -->|"PreEnqueue 拒绝<br/>(如 SchedulingGates)"| UP["unschedulablePods<br/>（map，不参与调度）"]
    AQ -->|Pop| SCHED["调度周期"]
    SCHED -->|成功| BOUND["已绑定"]
    SCHED -->|Unschedulable| UP
    SCHED -->|"Error / 可重试"| BQ["backoffQ<br/>（按退避到期时间排序）"]
    UP -->|"集群事件 + QueueingHint 判定<br/>或 5min 超时兜底"| BQ
    BQ -->|退避到期| AQ
    BQ -.->|"activeQ 空时可直接 Pop<br/>(SchedulerPopFromBackoffQ)"| SCHED
```

源码注释写得很清楚（`backend/queue/scheduling_queue.go`）：

```go
// PriorityQueue implements a scheduling queue.
// The head of PriorityQueue is the highest priority pending pod. This structure
// has two sub queues and a additional data structure, namely: activeQ,
// backoffQ and unschedulablePods.
//   - activeQ holds pods that are being considered for scheduling.
//   - backoffQ holds pods that moved from unschedulablePods and will move to
//     activeQ when their backoff periods complete.
//   - unschedulablePods holds pods that were already attempted for scheduling and
//     are currently determined to be unschedulable.
type PriorityQueue struct {
    *nominator
    ...
}
```

### 5.1 QueueingHint —— v1.34 已 GA 的重要优化

老版本的问题：一个 Pod 因为「CPU 不够」进了 `unschedulablePods`，那么**任何** Pod 删除事件都会把它拉回 activeQ 重试 —— 大量无效调度。

QueueingHint 让插件对「这个事件对我这个失败原因有意义吗」表态。以 `NodeResourcesFit` 为例，别的 Pod 缩容时它会判断「腾出来的资源是不是我缺的那种」：

```go
// plugins/noderesources/fit.go
func (f *Fit) isSchedulableAfterPodScaleDown(targetPod, originalPod, modifiedPod *v1.Pod) bool {
    if modifiedPod.Spec.NodeName == "" {
        // If the update event is for a unscheduled Pod, it wouldn't make targetPod schedulable.
        return false                      // ★ 没落到节点上的 Pod 缩容，不释放任何资源
    }
    ...
    podRequests := resource.PodRequests(targetPod, opts)
    for rName, rValue := range podRequests {
        if rValue.IsZero() {
            // We only care about the resources requested by the pod we are trying to schedule.
            continue                      // ★ 只关心「我请求了的」资源维度
        }
        switch rName {
        case v1.ResourceCPU:
            if originalMaxResourceReq.MilliCPU > modifiedMaxResourceReq.MilliCPU {
                return true               // CPU 确实被释放了 → 值得重试
            }
        ...
        }
    }
    return false                          // ★ 释放的不是我缺的资源 → 别叫我
}
```

这个判断很典型：一个只缺 CPU 的 Pod，不会因为「别人释放了内存」而被唤醒。

`SchedulerQueueingHints` 在 **v1.34 已 GA 并 LockToDefault**，也就是说这个机制现在是不可关闭的标准行为。

### 5.2 nominator：抢占的跨轮记忆

`PriorityQueue` 内嵌了 `*nominator`，记录「哪个 Pod 被提名到哪个节点」（对应 `pod.status.nominatedNodeName`）。作用有两个：

1. 抢占者下一轮优先尝试被提名的节点；
2. 其他 Pod 在 Filter 时会把「被提名到这个节点的高优 Pod」算作已占资源（`podsWithAffinity` 之外的另一类虚拟占用），避免资源被低优 Pod 抢走。

---

## 6. v1.36 新增的成组调度路径（Alpha）

§1 提到的 `scheduleOnePodGroup` 分支，这里说明它由什么组成。**默认关闭，不影响默认行为** —— 了解它的价值在于知道社区的演进方向，以及读代码时不会被这些新文件搞晕。

### 6.1 涉及的 feature gates（全部 Alpha，默认关闭）

```go
// pkg/features/kube_features.go
GangScheduling: {
    {Version: version.MustParse("1.35"), Default: false, PreRelease: featuregate.Alpha},
},
GenericWorkload: {
    {Version: version.MustParse("1.35"), Default: false, PreRelease: featuregate.Alpha},
},
TopologyAwareWorkloadScheduling: {
    {Version: version.MustParse("1.36"), Default: false, PreRelease: featuregate.Alpha},
},
WorkloadAwarePreemption: {
    {Version: version.MustParse("1.36"), Default: false, PreRelease: featuregate.Alpha},
},
WorkloadWithJob: {
    {Version: version.MustParse("1.36"), Default: false, PreRelease: featuregate.Alpha},
},
```

依赖关系（`pkg/features/kube_features.go` 的依赖表）：

```text
GenericWorkload（基础：Workload / PodGroup API）
├── GangScheduling
│   └── WorkloadAwarePreemption
├── TopologyAwareWorkloadScheduling
└── WorkloadWithJob（Job controller 自动建 Workload + PodGroup）
```

### 6.2 新增的 API 与插件

| 新增物 | 位置 | 作用 |
|--------|------|------|
| `PodGroup`（`scheduling.k8s.io/v1alpha2`） | `pkg/apis/scheduling` | `spec.schedulingPolicy.gang.minCount`、`spec.schedulingConstraints.topology` |
| `pod.spec.schedulingGroup.podGroupName` | core/v1 | Pod 声明自己属于哪个 PodGroup（**不可变**） |
| `GangScheduling` 插件 | `plugins/gangscheduling/` | `PreEnqueue` + `Permit` 实现 all-or-nothing |
| `TopologyPlacementGenerator` 插件 | `plugins/topologyaware/` | 生成候选 **Placement**（一组节点） |
| `PodGroupPodsCount` 插件 | `plugins/podgrouppodscount/` | 给 Placement 打分（权重 1） |
| `PlacementGenerate` / `PlacementScore` 扩展点 | `staging/.../framework/interface.go` | 全新的两个扩展点 |
| `scheduleOnePodGroup` | `schedule_one_podgroup.go` | PodGroup 调度周期 |

### 6.3 成组语义靠哪两个扩展点实现

`GangScheduling` 插件没有引入任何新机制，只是组合了前面讲过的两个扩展点：

| 扩展点 | 判断 | 返回 |
|--------|------|------|
| `PreEnqueue` | 同组 Pod 数量 < `minCount`？ | `UnschedulableAndUnresolvable` —— 人没到齐，整组都不进队列 |
| `Permit` | 已调度的同组 Pod < `minCount`？ | `Wait` + 超时；同时 `Activate` 同组未调度的 Pod 加速凑齐 |

凑够 `minCount` 后，`Permit` 遍历同组所有等待者放行：

```go
// plugins/gangscheduling/gangscheduling.go
if scheduledPodsCount < int(policy.Gang.MinCount) {
    unscheduledPods := podGroupState.UnscheduledPods()
    pl.handle.Activate(klog.FromContext(ctx), unscheduledPods)      // 唤醒同伴
    return fwk.NewStatus(fwk.Wait, "waiting for minCount pods from a gang to be scheduled"),
        permitTimeoutDuration                                        // ★ 5 分钟，硬编码
}

// quorum 达成：放行所有在 Permit 上等待的同组 Pod
for podUID := range podGroupState.AssumedPods() {
    if waitingPod := pl.handle.GetWaitingPod(podUID); waitingPod != nil {
        waitingPod.Allow(Name)
    }
}
return nil, 0
```

这是经典的 coscheduling 套路（与社区 `scheduler-plugins` 的 coscheduling 同源），现在进了主干。

**一个值得留意的结构性事实**：`Permit` 返回 `Wait` 时，Pod 已经在上一步 `assume` 过了 —— 等待期间它占着缓存里的资源（[01 篇 §4.3](01-核心原理-调度周期与扩展点.md)）。这也是为什么必须有 `permitTimeoutDuration` 兜底。这个设计的影响见 [04 篇 §4](04-面向大模型场景的能力与局限.md)。

### 6.4 拓扑感知：Placement 机制

`TopologyAwareWorkloadScheduling` 引入了一套全新流程（`schedule_one_podgroup.go`）：

```go
func (sched *Scheduler) podGroupSchedulingPlacementAlgorithm(...) podGroupAlgorithmResult {
    // ① 插件生成候选 Placement（每个 Placement 是一组节点，如一个 rack）
    placements, status := schedFwk.RunPlacementGeneratePlugins(ctx, podGroupCycleState, podGroupInfo.PodGroupInfo, allNodes)

    for _, placement := range placements {
        // ② 把这个 Placement "假设"进快照
        err := sched.nodeInfoSnapshot.AssumePlacement(placement)
        // ③ 在这个 Placement 内跑完整的成组调度
        result := sched.podGroupSchedulingDefaultAlgorithm(ctx, schedFwk, podGroupCycleState, podGroupInfo, postFilterMode)
        sched.nodeInfoSnapshot.ForgetPlacement()          // ★ 回滚
        if result.status.IsSuccess() || result.waitingOnPreemption {
            successfulResults[placement] = &result
        }
    }
    // ④ 用 PlacementScore 插件挑最优 Placement
    bestPlacement, status := sched.findBestPlacement(ctx, schedFwk, podGroupCycleState, podGroupInfo, successfulResults)
    return *successfulResults[bestPlacement]
}
```

**这是一个「逐候选域 dry-run + 回滚 + 选最优」的模式** —— 与前面 15 个扩展点「单 Pod × 单节点」的粒度完全不同，是扩展点粒度上的质变（对应 `PlacementGenerate` / `PlacementScore` 两个新扩展点，见 [06 篇 §11](06-扩展实战-自定义调度插件开发.md)）。

### 6.5 workload 级抢占

```go
func (sched *Scheduler) runWorkloadAwarePreemption(...) *fwk.Status {
    plugins := schedFwk.PodGroupPostFilterPlugins()
    ...
    if pg.Spec.SchedulingConstraints != nil && len(pg.Spec.SchedulingConstraints.Topology) > 0 {
        return fwk.NewStatus(fwk.Unschedulable, "workload aware preemption is not supported for pod groups with scheduling constraints")
    }
    restoreFn, err := sched.nodeInfoSnapshot.BackupSnapshot()
    defer restoreFn()                                     // ★ 快照备份 + 恢复

    return plugins[0].PodGroupPostFilter(ctx, pg, podGroupInfo.UnscheduledPods, func(ctx context.Context) *fwk.Status {
        res := sched.podGroupSchedulingAlgorithm(ctx, schedFwk, podGroupCycleState, podGroupInfo, runWithoutPostFilters)
        return res.status
    })
}
```

注意那个限制：**带拓扑约束的 PodGroup 暂不支持 workload 抢占**（`if pg.Spec.SchedulingConstraints != nil && len(...Topology) > 0` 直接返回 `Unschedulable`）。「成组抢占」与「拓扑约束」的组合本身是个难题 —— 要同时满足「驱逐后能凑齐整组」和「整组落在同一拓扑域」，搜索空间过大。

### 6.6 小结

| | 说明 |
|--|------|
| 代码位置 | `schedule_one_podgroup.go`、`plugins/{gangscheduling,topologyaware,podgrouppodscount}/` |
| 触发条件 | `pod.spec.schedulingGroup != nil` 且对应 gate 开启 |
| 成熟度 | 全部 **Alpha 默认关闭**，API 为 `v1alpha2` |
| 对默认行为的影响 | **无** —— gate 不开则 `ScheduleOne` 永远走 `scheduleOnePod` 分支 |

这条路径适合什么场景、与现有方案（Volcano / Kueue）如何取舍，见 [04 篇 §4](04-面向大模型场景的能力与局限.md)。

---

## 7. 默认插件与权重

`pkg/scheduler/apis/config/v1/default_plugins.go`（v1.36 用 `MultiPoint` 统一声明）：

```go
MultiPoint: v1.PluginSet{
    Enabled: []v1.Plugin{
        {Name: names.SchedulingGates},
        {Name: names.PrioritySort},
        {Name: names.NodeUnschedulable},
        {Name: names.NodeName},
        {Name: names.TaintToleration, Weight: ptr.To[int32](3)},      // ★ 权重最高
        {Name: names.NodeAffinity, Weight: ptr.To[int32](2)},
        {Name: names.NodePorts},
        {Name: names.NodeResourcesFit, Weight: ptr.To[int32](1)},
        {Name: names.VolumeRestrictions},
        {Name: names.NodeVolumeLimits},
        {Name: names.VolumeBinding},
        {Name: names.VolumeZone},
        {Name: names.PodTopologySpread, Weight: ptr.To[int32](2)},
        {Name: names.InterPodAffinity, Weight: ptr.To[int32](2)},
        {Name: names.DefaultPreemption},
        {Name: names.NodeResourcesBalancedAllocation, Weight: ptr.To[int32](1)},
        {Name: names.ImageLocality, Weight: ptr.To[int32](1)},
        {Name: names.DefaultBinder},
    },
},
```

按 feature gate 追加：

```go
func applyFeatureGates(config *v1.Plugins) {
    if utilfeature.DefaultFeatureGate.Enabled(features.NodeDeclaredFeatures) { ... }
    if utilfeature.DefaultFeatureGate.Enabled(features.DynamicResourceAllocation) {
        applyDynamicResources(config)     // DynamicResources 插到 DefaultPreemption 之前，weight 2
    }
    if utilfeature.DefaultFeatureGate.Enabled(features.GangScheduling) {
        applyGangScheduling(config)       // 追加 GangScheduling
    }
    if utilfeature.DefaultFeatureGate.Enabled(features.TopologyAwareWorkloadScheduling) {
        applyTopologyAwareWorkloadScheduling(config)   // 追加 TopologyPlacementGenerator + PodGroupPodsCount(w=1)
    }
}
```

`DynamicResources` 插在 `DefaultPreemption` **之前**，注释解释了原因：

> *"This plugin should come before DefaultPreemption because if there is a problem with a Pod and PostFilter gets called to resolve the problem, it is better to first deallocate an idle ResourceClaim than it is to evict some Pod that might be doing useful work."*

**关于权重要记住的两点**：

- `TaintToleration` 权重最高（3），但它在「没有 `PreferNoSchedule` 污点」的集群里给所有节点固定满分，**实际零区分度**（[01 篇 §6.6](01-核心原理-调度周期与扩展点.md) 有推导）。
- `NodeResourcesFit` 权重只有 1，且默认策略是 `LeastAllocated`（**打散**）。想要装箱（`MostAllocated`）必须显式配置，且默认只对 CPU / 内存生效。

---

## 8. 系列导航

**通用原理**（不绑定任何场景，用普通工作负载举例）：

| 篇 | 主题 | 核心问题 |
|----|------|---------|
| 00（本篇） | 总览与架构 | 定位与职责边界、组件、Pod 的一生、三队列、v1.36 新增的成组路径 |
| [01](01-核心原理-调度周期与扩展点.md) | 调度周期与扩展点 | 15 个扩展点各在什么时候跑、Status 语义、CycleState，**完整 trace 一个 Pod（含逐项分数计算）** |
| [02](02-核心代码分析-调度队列与缓存.md) | 调度队列与缓存 | 三队列流转、QueueingHint、Cache 增量快照、assume/forget |
| [03](03-核心代码分析-过滤打分与抢占.md) | 过滤打分与抢占 | findNodesThatFitPod 的采样与并行、打分归一化、抢占五步 |

**场景与扩展**：

| 篇 | 主题 | 核心问题 |
|----|------|---------|
| [04](04-面向大模型场景的能力与局限.md) | 大模型场景 | 换成 GPU 集群示例：能做好什么、做不到什么、DRA、怎么和 Volcano/Kueue 配合 |
| [05](05-实战Demo.md) | 实战 Demo | 改配置、装箱策略、拓扑打散、抢占、开成组调度 gate 实测、排障 |
| [06](06-扩展实战-自定义调度插件开发.md) | 扩展实战 | **每个扩展点一个可编译 demo**：out-of-tree 注册、CycleState、QueueingHint、Permit、部署与验证 |
| [07](07-免编译扩展-Extender与外部扩展点.md) | 免编译扩展 | **不改二进制**的六种方式：Extender(HTTP)、配置层、SchedulingGates、Webhook、DRA、Descheduler，各含部署 Demo |

---

## 附录：版本说明

### A.1 为什么钉 v1.36.3

本系列所有源码引用均以 **`v1.36.3`** 为准（编写时最新稳定 release）。

```bash
cd $GOPATH/src/k8s.io/kubernetes
git checkout v1.36.3
```

kube-scheduler 的目录结构近几个版本变动很大，用错版本会完全找不到文件：

| 内容 | 老路径（≤ v1.30） | v1.36 路径 |
|------|-----------------|-----------|
| 调度队列 | `pkg/scheduler/internal/queue/` | `pkg/scheduler/backend/queue/` |
| 缓存 | `pkg/scheduler/internal/cache/` | `pkg/scheduler/backend/cache/` |
| 扩展点接口 | `pkg/scheduler/framework/interface.go` | `staging/src/k8s.io/kube-scheduler/framework/interface.go` |
| 异步 API 调用 | — | `pkg/scheduler/backend/api_dispatcher/`（v1.34+） |

### A.2 v1.36 相比常见资料的关键差异

大量中文资料基于 v1.18~v1.25，以下内容已经不同：

| 项 | 老资料说法 | v1.36 事实 |
|----|-----------|-----------|
| 「kube-scheduler 没有 gang」 | 正确（当时） | **已有**（`GangScheduling` Alpha，`schedule_one_podgroup.go`） |
| 「不理解网络拓扑」 | 正确（当时） | **已有**（`TopologyAwareWorkloadScheduling` Alpha，Placement 机制） |
| 「`percentageOfNodesToScore` 默认 50%」 | 老版本 | 默认 0 = **自适应**（`numFeasibleNodesToFind` 按集群规模算，下限 100 节点） |
| 「DRA 还是 Alpha 实验特性」 | ≤ v1.31 | **v1.34 GA、v1.35 GA + LockToDefault**（不可关闭） |
| 「失败 Pod 靠周期性重试」 | 老版本 | **QueueingHint**（v1.34 GA + LockToDefault），事件驱动精准重试 |
| 「抢占是同步的」 | 老版本 | `SchedulerAsyncPreemption` v1.33 Beta **默认开** |
| 「Bind 是同步 API 调用」 | 老版本 | `SchedulerAsyncAPICalls` v1.34 Beta（默认关），可异步化 |
| 「插件按 Filter/Score 分别配置」 | 老版本 | 默认配置统一用 **`MultiPoint`** |

### A.3 调度相关 feature gate 状态（v1.36.3）

| Gate | 状态 | 说明 |
|------|------|------|
| `SchedulerQueueingHints` | **GA + LockToDefault**（1.34） | 不可关闭 |
| `SchedulerAsyncPreemption` | Beta 默认 **开**（1.33） | 抢占异步化 |
| `SchedulerPopFromBackoffQ` | Beta 默认 **开**（1.33） | activeQ 空时直接从 backoffQ 取 |
| `SchedulerAsyncAPICalls` | Beta 默认 **关**（1.34） | 异步 API 调用 |
| `DynamicResourceAllocation` | **GA + LockToDefault**（1.35） | DRA，不可关闭（见 04 篇） |
| `NodeDeclaredFeatures` | Beta 默认 **开**（1.36） | 节点能力声明 |
| `OpportunisticBatching` | Beta 默认 **开**（1.35） | 相同签名 Pod 复用调度结果 |
| `GenericWorkload` | Alpha 默认关（1.35） | Workload / PodGroup API 基础 |
| `GangScheduling` | Alpha 默认关（1.35） | 依赖 `GenericWorkload` |
| `TopologyAwareWorkloadScheduling` | Alpha 默认关（1.36） | 依赖 `GenericWorkload` |
| `WorkloadAwarePreemption` | Alpha 默认关（1.36） | 依赖 `GangScheduling` |
| `WorkloadWithJob` | Alpha 默认关（1.36） | Job controller 自动建 Workload+PodGroup |

### A.4 升级复查

```bash
cd $GOPATH/src/k8s.io/kubernetes
git diff --stat v1.36.3..<新tag> -- \
  pkg/scheduler/ staging/src/k8s.io/kube-scheduler/ \
  pkg/features/kube_features.go
```
