# 实战 Demo

> 从零跑通：安装 → 配额体系 → 批作业 → 分布式训练（JobSet + TAS）→ 推理服务（LWS）→ 跨队列借用与回收 → 部分准入 → 排障。
>
> 所有 YAML 使用 `kueue.x-k8s.io/v1beta2`，**版本基线 v0.19.1**。GPU 部分需要集群已装 NVIDIA device plugin；没有 GPU 时把 `nvidia.com/gpu` 换成 `cpu` 也能完整验证配额逻辑。

---

## 0. 安装与验证

### 0.1 安装

```bash
# 官方支持 Kubernetes 1.34+
kubectl apply --server-side -f https://github.com/kubernetes-sigs/kueue/releases/download/v0.19.1/manifests.yaml

# 或用 Helm
helm install kueue oci://registry.k8s.io/kueue/charts/kueue \
  --version 0.19.1 --namespace kueue-system --create-namespace
```

验证：

```bash
kubectl get pods -n kueue-system
# NAME                                        READY   STATUS
# kueue-controller-manager-xxxxxxxxx-xxxxx    1/1     Running   ← 只有一个 Deployment

kubectl get crd | grep kueue
# clusterqueues.kueue.x-k8s.io
# localqueues.kueue.x-k8s.io
# workloads.kueue.x-k8s.io
# cohorts.kueue.x-k8s.io
# resourceflavors.kueue.x-k8s.io
# topologies.kueue.x-k8s.io
# admissionchecks.kueue.x-k8s.io
# workloadpriorityclasses.kueue.x-k8s.io
# provisioningrequestconfigs.kueue.x-k8s.io
# multikueueclusters.kueue.x-k8s.io / multikueueconfigs.kueue.x-k8s.io
```

### 0.2 装 CLI（强烈建议）

```bash
# kueuectl（也可作为 kubectl 插件：kubectl kueue ...）
# 从 release 页面下载对应平台二进制，或：
go install sigs.k8s.io/kueue/cmd/kueuectl@latest

kueuectl list clusterqueue
kueuectl list workload
kueuectl list pods --for job/my-job
```

### 0.3 配置组件（Configuration）

配置在 ConfigMap `kueue-manager-config`（namespace `kueue-system`）：

```bash
kubectl -n kueue-system edit cm kueue-manager-config
```

本篇 Demo 使用的配置：

```yaml
apiVersion: config.kueue.x-k8s.io/v1beta2
kind: Configuration
namespace: kueue-system
manageJobsWithoutQueueName: false      # ★ 保持 false，避免误挂起系统 Job
integrations:
  frameworks:
  - batch/job
  - jobset.x-k8s.io/jobset
  - leaderworkerset.x-k8s.io/leaderworkerset
  - kubeflow.org/pytorchjob
  - ray.io/rayjob
  - pod
waitForPodsReady:
  enable: true
  timeout: 15m
  blockAdmission: true
  recoveryTimeout: 5m
  requeuingStrategy:
    timestamp: Eviction
    backoffLimitCount: 10
    backoffBaseSeconds: 120
    backoffMaxSeconds: 1800
fairSharing:
  enable: false                        # 先用经典抢占，第 6 节再开
featureGates:
  TopologyAwareScheduling: true        # 0.14+ 已默认开，这里显式声明
```

改完需要重启（Configuration 不支持热加载）：

```bash
kubectl -n kueue-system rollout restart deploy/kueue-controller-manager
kubectl -n kueue-system logs deploy/kueue-controller-manager | head -50
```

---

## 1. 建立配额体系

沿用系列统一示例：8 节点 × 8 A100 = 64 卡，5 台 on-demand（40 卡）+ 3 台 spot（24 卡），两个团队共享。

### 1.1 给节点打标签（模拟异构与拓扑）

```bash
# 机型
for i in 0 1 2 3 4; do kubectl label node gpu-node-$i instance-type=on-demand --overwrite; done
for i in 5 6 7;     do kubectl label node gpu-node-$i instance-type=spot --overwrite; done

# 网络拓扑：两个 block，每 block 两个 rack
kubectl label node gpu-node-0 gpu-node-1 gpu-node-2 gpu-node-3 \
  cloud.provider.com/topology-block=block-1 --overwrite
kubectl label node gpu-node-4 gpu-node-5 gpu-node-6 gpu-node-7 \
  cloud.provider.com/topology-block=block-2 --overwrite
kubectl label node gpu-node-0 gpu-node-1 cloud.provider.com/topology-rack=rack-1 --overwrite
kubectl label node gpu-node-2 gpu-node-3 cloud.provider.com/topology-rack=rack-2 --overwrite
kubectl label node gpu-node-4 gpu-node-5 cloud.provider.com/topology-rack=rack-3 --overwrite
kubectl label node gpu-node-6 gpu-node-7 cloud.provider.com/topology-rack=rack-4 --overwrite
```

### 1.2 apply 配额对象

```yaml
# quota.yaml
apiVersion: kueue.x-k8s.io/v1beta2
kind: Topology
metadata: {name: gpu-topology}
spec:
  levels:
  - nodeLabel: cloud.provider.com/topology-block
  - nodeLabel: cloud.provider.com/topology-rack
  - nodeLabel: kubernetes.io/hostname       # ★ 只能在最低层
---
apiVersion: kueue.x-k8s.io/v1beta2
kind: ResourceFlavor
metadata: {name: a100-ondemand}
spec:
  nodeLabels: {instance-type: on-demand}
  topologyName: gpu-topology                # ★ 启用 TAS
---
apiVersion: kueue.x-k8s.io/v1beta2
kind: ResourceFlavor
metadata: {name: a100-spot}
spec:
  nodeLabels: {instance-type: spot}
  topologyName: gpu-topology
---
apiVersion: kueue.x-k8s.io/v1beta2
kind: Cohort
metadata: {name: llm-pool}
---
apiVersion: kueue.x-k8s.io/v1beta2
kind: ClusterQueue
metadata: {name: team-a}
spec:
  namespaceSelector: {}                     # ★ 必须显式，默认 null 全拒绝
  cohortName: llm-pool
  queueingStrategy: BestEffortFIFO
  resourceGroups:
  - coveredResources: ["cpu", "memory", "nvidia.com/gpu"]
    flavors:
    - name: a100-ondemand                   # ★ 顺序即优先级
      resources:
      - {name: cpu,            nominalQuota: 160}
      - {name: memory,         nominalQuota: 1280Gi}
      - {name: nvidia.com/gpu, nominalQuota: 20, borrowingLimit: 12, lendingLimit: 8}
    - name: a100-spot
      resources:
      - {name: cpu,            nominalQuota: 96}
      - {name: memory,         nominalQuota: 768Gi}
      - {name: nvidia.com/gpu, nominalQuota: 12, borrowingLimit: 12}
  preemption:
    reclaimWithinCohort: LowerPriority
    withinClusterQueue: LowerPriority
---
apiVersion: kueue.x-k8s.io/v1beta2
kind: ClusterQueue
metadata: {name: team-b}
spec:
  namespaceSelector: {}
  cohortName: llm-pool
  queueingStrategy: BestEffortFIFO
  resourceGroups:
  - coveredResources: ["cpu", "memory", "nvidia.com/gpu"]
    flavors:
    - name: a100-ondemand
      resources:
      - {name: cpu,            nominalQuota: 160}
      - {name: memory,         nominalQuota: 1280Gi}
      - {name: nvidia.com/gpu, nominalQuota: 20, borrowingLimit: 12, lendingLimit: 8}
    - name: a100-spot
      resources:
      - {name: cpu,            nominalQuota: 96}
      - {name: memory,         nominalQuota: 768Gi}
      - {name: nvidia.com/gpu, nominalQuota: 12, borrowingLimit: 12}
  preemption:
    reclaimWithinCohort: LowerPriority
    withinClusterQueue: LowerPriority
---
apiVersion: v1
kind: Namespace
metadata: {name: team-a}
---
apiVersion: v1
kind: Namespace
metadata: {name: team-b}
---
apiVersion: kueue.x-k8s.io/v1beta2
kind: LocalQueue
metadata: {namespace: team-a, name: default}
spec: {clusterQueue: team-a}
---
apiVersion: kueue.x-k8s.io/v1beta2
kind: LocalQueue
metadata: {namespace: team-b, name: default}
spec: {clusterQueue: team-b}
---
apiVersion: kueue.x-k8s.io/v1beta2
kind: WorkloadPriorityClass
metadata: {name: high}
value: 100000
description: "high priority (serving / urgent training)"
---
apiVersion: kueue.x-k8s.io/v1beta2
kind: WorkloadPriorityClass
metadata: {name: low}
value: 100
description: "low priority (best-effort training)"
```

```bash
kubectl apply -f quota.yaml
```

### 1.3 验证配额生效

```bash
kueuectl list clusterqueue
# NAME     COHORT     PENDING WORKLOADS   ADMITTED WORKLOADS
# team-a   llm-pool   0                   0
# team-b   llm-pool   0                   0

# ★ 第一件要检查的事：Active 条件
kubectl get cq team-a -o jsonpath='{.status.conditions}' | jq
# [{"type":"Active","status":"True","reason":"Ready","message":"Can admit new workloads"}]
```

`Active=False` 时看 `reason`，对照 00 篇 §5.2：`FlavorNotFound` / `TopologyNotFound` / `AdmissionCheckNotFound` / `Stopped` / `MultiKueueWithProvisioningRequest` 等。

**手算一遍配额上限**（对应 01 篇 §2.4）：

```text
team-a.a100-ondemand:
  localQuota   = 20 − 8 = 12          自留 12 卡不外借
  上交 cohort  = 20 − 12 = 8
cohort.SubtreeQuota(ondemand) = 8 + 8 = 16
team-a 实际可用上限 = min(nominal + borrowingLimit, 12 + cohort可用)
                    = min(20 + 12, 12 + 16) = 28  →  受 borrowingLimit 约束为 32? 
                    实测以 status.flavorsUsage 为准
```

> 不用死记公式，直接跑第 5 节的借用 Demo 观察 `status.flavorsUsage[].resources[].borrowed` 就能验证。

---

## 2. Demo A：最简批作业（理解 suspend 机制）

```yaml
# sample-job.yaml
apiVersion: batch/v1
kind: Job
metadata:
  generateName: sample-job-
  namespace: team-a
  labels:
    kueue.x-k8s.io/queue-name: default      # ★ 接入 Kueue
spec:
  parallelism: 3
  completions: 3
  suspend: true                             # 也可不写，webhook 会自动设
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: dummy
        image: registry.k8s.io/e2e-test-images/agnhost:2.53
        args: ["pause"]
        resources:
          requests: {cpu: "1", memory: 200Mi}
```

```bash
kubectl create -f sample-job.yaml
```

观察三个对象的联动：

```bash
# ① Job 从 suspend 变 running
kubectl -n team-a get job -w
# NAME              SUSPEND   COMPLETIONS
# sample-job-abcde  true      0/3          ← Pod 一个都没有
# sample-job-abcde  false     0/3          ← 被 admit，Pod 开始创建

# ② Workload 的条件流转（★ 最重要）
kueuectl -n team-a list workload
# NAME                     JOB TYPE   JOB NAME           LOCALQUEUE   CLUSTERQUEUE   STATUS     AGE
# job-sample-job-abcde-x   Job        sample-job-abcde   default      team-a         ADMITTED   3s

kubectl -n team-a get workload -o yaml | yq '.items[0].status'
# conditions:
#   - type: QuotaReserved   status: "True"   reason: QuotaReserved
#   - type: Admitted        status: "True"   reason: Admitted
# admission:
#   clusterQueue: team-a
#   podSetAssignments:
#   - name: main
#     count: 3
#     flavors: {cpu: a100-ondemand, memory: a100-ondemand}
#     resourceUsage: {cpu: "3", memory: 600Mi}

# ③ Pod 上被注入了 flavor 的 nodeLabels
kubectl -n team-a get pod -o jsonpath='{.items[0].spec.nodeSelector}' | jq
# {"instance-type":"on-demand"}      ← ★ 这是 RunWithPodSetsInfo 注入的
```

### 2.1 验证「装不下就不创建 Pod」

```bash
# 提交一个远超配额的作业
kubectl -n team-a create -f - <<'EOF'
apiVersion: batch/v1
kind: Job
metadata:
  generateName: too-big-
  labels: {kueue.x-k8s.io/queue-name: default}
spec:
  parallelism: 1
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: c
        image: registry.k8s.io/e2e-test-images/agnhost:2.53
        args: ["pause"]
        resources: {requests: {cpu: "10000"}}
EOF

kubectl -n team-a get pod            # ← 一个 Pod 都没有
kubectl -n team-a describe workload | tail -20
# Conditions:
#   Type            Status  Reason            Message
#   QuotaReserved   False   ExceedsMaxQuota   couldn't assign flavors to pod set main:
#                                             insufficient quota for cpu in flavor a100-ondemand
```

**`ExceedsMaxQuota` 意味着「等也没用」**（01 篇 §1.1）—— 请求量超过了 CQ + Cohort 的结构性上限。删掉它：

```bash
kubectl -n team-a delete job -l 'job-name' --field-selector=status.successful=0 2>/dev/null
kubectl -n team-a delete job --all
```

---

## 3. Demo B：分布式训练（JobSet + TAS）

前置：安装 JobSet（`kubectl apply --server-side -f https://github.com/kubernetes-sigs/jobset/releases/download/v0.9.2/manifests.yaml`），并确保 Configuration 的 `integrations.frameworks` 含 `jobset.x-k8s.io/jobset`。

### 3.1 硬拓扑约束：8 个 Pod 必须同 block

```yaml
# train-jobset.yaml
apiVersion: jobset.x-k8s.io/v1alpha2
kind: JobSet
metadata:
  name: pretrain-t
  namespace: team-a
  labels:
    kueue.x-k8s.io/queue-name: default
    kueue.x-k8s.io/priority-class: high
spec:
  replicatedJobs:
  - name: worker
    replicas: 4                      # 4 个 Pod（示例缩小规模）
    template:
      spec:
        parallelism: 1
        completions: 1
        backoffLimit: 0
        template:
          metadata:
            annotations:
              # ★ 硬约束：所有 Pod 必须落在同一个 block
              kueue.x-k8s.io/podset-required-topology: cloud.provider.com/topology-block
          spec:
            restartPolicy: Never
            containers:
            - name: trainer
              image: pytorch/pytorch:2.3.0-cuda12.1-cudnn8-runtime
              command: ["sleep", "600"]
              resources:
                limits: {nvidia.com/gpu: 2, cpu: 8, memory: 64Gi}
```

```bash
kubectl apply -f train-jobset.yaml
```

观察 TAS 分配结果：

```bash
kubectl -n team-a get workload -o yaml | yq '.items[0].status.admission.podSetAssignments[0].topologyAssignment'
# levels:
#   - kubernetes.io/hostname
# slices:
#   - domainCount: 4
#     valuesPerLevel:
#       - individual:
#           prefix: gpu-node-
#           roots: ["0", "1", "2", "3"]      ← ★ 前缀压缩（03 篇 §5.3）
#     podCounts:
#       universal: 1

# 验证 Pod 真的落在同一个 block
kubectl -n team-a get pod -o custom-columns=\
NAME:.metadata.name,NODE:.spec.nodeName,SELECTOR:.spec.nodeSelector | column -t
```

### 3.2 观察 scheduling gate

TAS 作业的 Pod 会先带一个门控，等 nodeSelector 注入完才摘掉：

```bash
kubectl -n team-a get pod -o jsonpath='{.items[0].spec.schedulingGates}'
# 准入前：[{"name":"kueue.x-k8s.io/topology"}]
# 准入后：（空）
```

### 3.3 制造拓扑不满足，验证硬约束

把 `replicas` 提到 6（超过单个 block 的 4 台机器可提供的量），重新 apply：

```bash
kubectl -n team-a describe workload | tail -15
# Conditions:
#   Type            Status  Reason                     Message
#   QuotaReserved   False   TopologyPlacementFailed    topology "cloud.provider.com/topology-block"
#                                                      allows to fit only 4 out of 6 pod(s)
```

改成软约束 `kueue.x-k8s.io/podset-preferred-topology` 再试 —— 会看到它逐级放宽到 rack 之上、最终跨 block 分散并成功准入。

### 3.4 slice 约束：TP 组同机

```yaml
metadata:
  annotations:
    kueue.x-k8s.io/podset-slice-required-topology: kubernetes.io/hostname
    kueue.x-k8s.io/podset-slice-size: "2"     # 每 2 个 Pod 必须同机
```

`topologyAssignment.podCounts` 会变成每个 hostname 域 2 个 Pod。

---

## 4. Demo C：推理服务（LeaderWorkerSet 多机 TP）

前置：安装 LWS（`kubectl apply --server-side -f https://github.com/kubernetes-sigs/lws/releases/download/v0.7.0/manifests.yaml`），Configuration 的 `integrations.frameworks` 含 `leaderworkerset.x-k8s.io/leaderworkerset`。

```yaml
# serve-lws.yaml
apiVersion: leaderworkerset.x-k8s.io/v1
kind: LeaderWorkerSet
metadata:
  name: vllm-tp2
  namespace: team-b
  labels:
    kueue.x-k8s.io/queue-name: default
    kueue.x-k8s.io/priority-class: high        # ★ 推理高优
spec:
  replicas: 2
  leaderWorkerTemplate:
    size: 2                                    # 每副本 2 个 Pod（TP=2）
    leaderTemplate:
      metadata:
        annotations:
          kueue.x-k8s.io/podset-slice-required-topology: kubernetes.io/hostname
          kueue.x-k8s.io/podset-slice-size: "2"
      spec:
        containers:
        - name: vllm-leader
          image: vllm/vllm-openai:latest
          args: ["--model=Qwen/Qwen2.5-1.5B-Instruct", "--tensor-parallel-size=2"]
          ports: [{containerPort: 8000}]
          resources: {limits: {nvidia.com/gpu: 1, cpu: 8, memory: 32Gi}}
    workerTemplate:
      metadata:
        annotations:
          kueue.x-k8s.io/podset-slice-required-topology: kubernetes.io/hostname
          kueue.x-k8s.io/podset-slice-size: "2"
      spec:
        containers:
        - name: vllm-worker
          image: vllm/vllm-openai:latest
          resources: {limits: {nvidia.com/gpu: 1, cpu: 8, memory: 32Gi}}
```

```bash
kubectl apply -f serve-lws.yaml

# LWS 自己创建 Workload（Kueue 识别它是顶层对象）
kueuectl -n team-b list workload
# NAME                    JOB TYPE            JOB NAME    ...   STATUS
# leaderworkerset-...     LeaderWorkerSet     vllm-tp2          ADMITTED

# Workload 有多个 PodSet（leader + workers）
kubectl -n team-b get workload -o yaml | yq '.items[0].spec.podSets[].name'
```

### 4.1 长服务的 gated Pod

```bash
# 把 replicas 提到很大，超出配额
kubectl -n team-b patch lws vllm-tp2 --type=merge -p '{"spec":{"replicas":20}}'

kubectl -n team-b get pod | head
# NAME            READY   STATUS             
# vllm-tp2-5-0    0/1     SchedulingGated    ← ★ Pod 已创建但被门控挡住
```

这是长服务与批作业的差异（04 篇 §2.2）：Deployment / StatefulSet / LWS 没有 `suspend`，Kueue 用 scheduling gate 实现等待。

---

## 5. Demo D：跨队列借用与回收

### 5.1 让 team-a 借用 team-b 的配额

```bash
# team-b 完全空闲，team-a 提交一个超出自己 nominalQuota 的作业
kubectl -n team-a create -f - <<'EOF'
apiVersion: batch/v1
kind: Job
metadata:
  generateName: borrow-
  labels:
    kueue.x-k8s.io/queue-name: default
    kueue.x-k8s.io/priority-class: low        # ★ 低优，方便后面被抢
spec:
  parallelism: 13                             # 13 卡 > nominalQuota 的 12 卡自留
  completions: 13
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: c
        image: registry.k8s.io/e2e-test-images/agnhost:2.53
        args: ["pause"]
        resources: {limits: {nvidia.com/gpu: 1}}
EOF
```

观察借用量：

```bash
kubectl get cq team-a -o yaml | yq '.status.flavorsUsage'
# - name: a100-ondemand
#   resources:
#     - name: nvidia.com/gpu
#       total: "13"
#       borrowed: "1"          ← ★ 超出 localQuota(12) 的部分记为 borrowed
```

**这就验证了 01 篇 §2.2 的冒泡逻辑**：前 12 卡花自留额度，第 13 卡才向 cohort 记账。

### 5.2 team-b 回收自己的配额

```bash
# team-b 提交高优作业，需要 20 卡（它的 nominalQuota）
kubectl -n team-b create -f - <<'EOF'
apiVersion: batch/v1
kind: Job
metadata:
  generateName: reclaim-
  labels:
    kueue.x-k8s.io/queue-name: default
    kueue.x-k8s.io/priority-class: high       # ★ 高优
spec:
  parallelism: 20
  completions: 20
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: c
        image: registry.k8s.io/e2e-test-images/agnhost:2.53
        args: ["pause"]
        resources: {limits: {nvidia.com/gpu: 1}}
EOF
```

观察抢占过程：

```bash
# ① 抢占事件
kubectl -n team-a get events --sort-by=.lastTimestamp | grep -i preempt
# Warning  Preempted  workload/job-borrow-xxx
#   Preempted to accommodate a workload (UID: ..., JobUID: ...) due to reclamation within the cohort;
#   preemptor path: llm-pool/team-b; preemptee path: llm-pool/team-a
#   ↑ ★ preemptor path / preemptee path 直接指出 cohort 树上的抢占关系

# ② 受害者 Workload 的条件
kubectl -n team-a get workload -o yaml | yq '.items[0].status.conditions'
# - type: Evicted     status: "True"   reason: Preempted
# - type: Preempted   status: "True"   reason: InCohortReclamation     ← ★
# - type: Requeued    status: "True"   reason: ...

# ③ 抢占者的中间状态（可能一闪而过）
kubectl -n team-b describe workload | grep -A3 QuotaReserved
# QuotaReserved  False  WaitingForPreemptedWorkloads
#   ... Pending the preemption of 1 workload(s)
#   ↑ ★ 抢占跨两轮：本轮驱逐，下轮 admit（02 篇 §4）

# ④ 退避状态
kubectl -n team-a get workload -o yaml | yq '.items[0].status.requeueState'
# count: 1
# requeueAt: "2026-08-17T..."

# ⑤ 驱逐统计
kubectl -n team-a get workload -o yaml | yq '.items[0].status.schedulingStats'
# evictions:
#   - reason: Preempted
#     underlyingCause: ""
#     count: 1
```

### 5.3 验证「保底」：lendingLimit 生效

```bash
# 清理，然后让 team-b 尝试借光 team-a 的配额
kubectl -n team-a delete job --all; kubectl -n team-b delete job --all
sleep 10

kubectl -n team-b create -f - <<'EOF'
apiVersion: batch/v1
kind: Job
metadata:
  generateName: greedy-
  labels: {kueue.x-k8s.io/queue-name: default}
spec:
  parallelism: 40
  completions: 40
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: c
        image: registry.k8s.io/e2e-test-images/agnhost:2.53
        args: ["pause"]
        resources: {limits: {nvidia.com/gpu: 1}}
EOF

kubectl get cq team-b -o yaml | yq '.status.flavorsUsage'
# total 最多到 20（nominal）+ 8（team-a 愿借出的 lendingLimit）= 28
# 而不是 40 —— team-a 的 12 卡自留额度被保护住了
```

**这就是 Kueue 表达 SLA 的方式**（04 篇 §3）：`lendingLimit` 是保底，`borrowingLimit` 是上限。

---

## 6. Demo E：部分准入与公平共享

### 6.1 PartialAdmission（弹性训练）

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  generateName: elastic-
  namespace: team-a
  labels: {kueue.x-k8s.io/queue-name: default}
  annotations:
    # ★ 允许缩到最少 4 个 Pod
    kueue.x-k8s.io/job-min-parallelism: "4"
spec:
  parallelism: 20
  completions: 20
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: c
        image: registry.k8s.io/e2e-test-images/agnhost:2.53
        args: ["pause"]
        resources: {limits: {nvidia.com/gpu: 1}}
```

```bash
kubectl -n team-a get job -o jsonpath='{.items[0].spec.parallelism}'
# 12        ← ★ 被缩减到当前可用配额（不是 20），Job 的 parallelism 被改写

kubectl -n team-a get workload -o yaml | yq '.items[0].status.admission.podSetAssignments[0].count'
# 12
```

这条路径对应 02 篇 §3.4 的 `PodSetReducer.Search()`。

### 6.2 打开 Fair Sharing

```yaml
# Configuration
fairSharing:
  enable: true
  preemptionStrategies: [LessThanOrEqualToFinalShare, LessThanInitialShare]
---
# ClusterQueue 上加权重
spec:
  fairSharing:
    weight: 2        # team-a 权重 2，team-b 权重 1 → team-a 借用时更"便宜"
```

```bash
kubectl -n kueue-system rollout restart deploy/kueue-controller-manager

# 观察 share
kubectl get cq team-a -o jsonpath='{.status.fairSharing}'
# {"weightedShare":0}       ← 用量在 nominalQuota 内 → share = 0（01 篇 §5.1）

# 让它借用后再看
kubectl get cq team-a -o jsonpath='{.status.fairSharing.weightedShare}'
# 62                        ← 借用量 / cohort可借出量 × 1000 / weight
```

⚠️ 开 Fair Sharing 后 `preemption.borrowWithinCohort` **必须是 `Never`**（两者互斥）。

---

## 7. 排障手册

### 7.1 分层定位法

```mermaid
flowchart TD
    A["作业没跑起来"] --> B{"有 Workload 吗?"}
    B -->|没有| B1["① Job 有 queue-name label 吗?<br/>② integrations.frameworks 包含这个类型吗?<br/>③ managedJobsNamespaceSelector 匹配吗?<br/>④ 看 controller-manager 日志"]
    B -->|有| C{"QuotaReserved?"}
    C -->|False| D{"看 reason"}
    C -->|True| E{"Admitted?"}
    E -->|False| E1["AdmissionCheck 没通过<br/>看 status.admissionChecks[]<br/>指标 kueue_admission_checks_wait_time_seconds"]
    E -->|True| F{"Job.suspend 还是 true?"}
    F -->|是| F1["jobframework 没跟上<br/>看 controller 日志 / RBAC"]
    F -->|否| G{"Pod 是 Pending?"}
    G -->|是| G1["★ kube-scheduler 层面的问题：<br/>碎片 / 亲和 / 污点<br/>→ 考虑开 TAS 或叠 Volcano"]
    G -->|SchedulingGated| G2["TAS gate 或长服务 gate 未摘<br/>看 tas controller 日志"]

    D -->|Misconfigured| D1["LocalQueue/ClusterQueue 不存在<br/>或 namespaceSelector 不匹配（默认 null！）"]
    D -->|ExceedsMaxQuota| D2["结构性超限：改 nominalQuota /<br/>borrowingLimit，等没用"]
    D -->|WaitingForQuota| D3["时序问题：等就行；<br/>想加速则配抢占"]
    D -->|NoMatchingFlavor| D4["flavor 的 nodeLabels/nodeTaints<br/>与 PodSet 不匹配"]
    D -->|TopologyPlacementFailed| D5["TAS 约束太紧：<br/>required 改 preferred"]
    D -->|Suspended| D6["CQ/LQ 的 stopPolicy 生效"]
    D -->|WaitingForPodsReady| D7["blockAdmission 生效，<br/>前面的作业 Pod 还没 ready"]
```

### 7.2 常用命令

```bash
# —— Workload 视角（90% 的问题看这里）——
kueuectl -n <ns> list workload
kubectl -n <ns> describe workload <wl>              # ★ 首选
kubectl -n <ns> get workload <wl> -o yaml | yq '.status'

# —— 队列视角 ——
kueuectl list clusterqueue
kubectl get cq -o custom-columns=\
NAME:.metadata.name,COHORT:.spec.cohortName,\
PENDING:.status.pendingWorkloads,ADMITTED:.status.admittedWorkloads,\
ACTIVE:'.status.conditions[?(@.type=="Active")].status'
kubectl get cq <cq> -o yaml | yq '.status.flavorsUsage'      # 借用对账
kubectl get cq <cq> -o yaml | yq '.status.fairSharing'

kueuectl list localqueue -n <ns>

# —— pending 队列位次（需 VisibilityOnDemand，Beta 默认开）——
kubectl get --raw "/apis/visibility.kueue.x-k8s.io/v1beta2/namespaces/<ns>/localqueues/default/pendingworkloads" | jq

# —— 控制器日志 ——
kubectl -n kueue-system logs deploy/kueue-controller-manager --tail=200
kubectl -n kueue-system logs deploy/kueue-controller-manager | grep -E \
 'Scheduling cycle|Obtained heads|Nomination done|Attempting to schedule|Workload assumed|Skipping workload'

# 提高日志级别（改 Deployment 的 --zap-log-level 或 Configuration）
# V(2) 能看到每轮 cycle 的阶段耗时；V(3)/V(5) 能看到单个 Workload 的决策细节

# —— 队列快照 dump（调试用）——
kubectl -n kueue-system logs deploy/kueue-controller-manager | grep -i 'Dump\|snapshot'
```

### 7.3 日志关键行对照

调度器 `V(2)` 日志能直接对上 02 篇的六步：

```text
"Scheduling cycle starts"                                     ← schedule() 入口
"Obtained heads" headCount=3 waitDuration=1.2s                ← ① Heads()
"Snapshot taken" duration=8ms                                 ← ② Snapshot()
"Nomination done" entries=2 inadmissibleEntries=1 duration=15ms ← ③ nominate()
"Attempting to schedule workload"                             ← ⑤ processEntry()
"Skipping workload as FlavorAssigner assigned NoFit mode"     ←   mode == NoFit
"Workload requires preemption, but there are no candidate..."  ←   Preempt 无受害者
"Re-computing the assignment as preemption targets overlap"    ←   目标重叠重算
"Workload assumed in the cache"                               ←   assume 成功
"Workload successfully admitted and assigned flavors"          ←   patch 成功
"Workload re-queued" requeueReason=NoFit status=              ← ⑥ requeueAndUpdate()
"Scheduling cycle complete" duration=45ms
```

### 7.4 高频问题速查

| 现象 | 原因 | 处理 |
|------|------|------|
| Job 提交后没有 Workload | 没打 `kueue.x-k8s.io/queue-name` label；或该类型不在 `integrations.frameworks` | 补 label / 加 integration 后重启 |
| `QuotaReserved=False, reason=Misconfigured` | `namespaceSelector` 默认 `null` | CQ 写 `namespaceSelector: {}` |
| `ExceedsMaxQuota` 但集群明显有空闲 | 空闲资源在别的 flavor 上，或 `borrowingLimit` 太小 | 检查 flavor nodeLabels 是否匹配；调 `borrowingLimit` |
| `NoMatchingFlavor` | Pod 的 nodeSelector/亲和与所有 flavor 都冲突；或 flavor 有 taint 而 Pod 无 toleration | 对齐 label；或给 flavor 配 `tolerations` |
| `Admitted=False` 但配额已保留 | AdmissionCheck 卡住（扩容中 / MultiKueue 派发中） | 看 `status.admissionChecks[].message` |
| 一直卡在 `WaitingForPodsReady` | 前面某作业的 Pod 起不来，`blockAdmission` 阻塞全局 | 查那个作业；或调大 `timeout` / 关 `blockAdmission` |
| Pod 是 `Pending`（已 Admitted） | **kube-scheduler 层面**：碎片 / 亲和 / 资源不足 | 开 TAS；或 Pod 用 `schedulerName: volcano` |
| Pod 长期 `SchedulingGated` | TAS gate 没摘（tas controller 异常）或长服务未准入 | 看 controller 日志与 Workload 状态 |
| 大作业永远排不上 | `BestEffortFIFO` 下被小作业持续插队 | 改 `StrictFIFO`；或配 `reclaimWithinCohort` + 高优先级 |
| 整个队列卡住 | `StrictFIFO` 队头装不下 | 改 `BestEffortFIFO`；或拆队列 |
| Workload 被 deactivate | 退避次数超 `backoffLimitCount`，或超 `maximumExecutionTimeSeconds` | 看 `Evicted` reason；改 `spec.active=true` 重启 |
| CQ `Active=False` | flavor / topology / admissionCheck 不存在，或 MultiKueue 误配 | 看 `status.conditions[].reason` |
| Cohort 里所有队列都不调度了 | Cohort 树成环 | `Snapshot()` 会静默跳过，检查 `parentName` |
| 改了 Configuration 没生效 | Configuration 不热加载 | `rollout restart deploy/kueue-controller-manager` |

### 7.5 性能观测

```promql
# 一轮调度耗时（p99）
histogram_quantile(0.99, rate(kueue_admission_attempt_duration_seconds_bucket[5m]))

# 准入成功率
rate(kueue_admission_attempts_total{result="success"}[5m])
  / rate(kueue_admission_attempts_total[5m])

# 各队列 pending 深度
kueue_pending_workloads

# 抢占争抢激烈程度（重叠导致的跳过）
rate(kueue_admission_cycle_preemption_skips[5m])

# 等 AdmissionCheck 的时间（诊断扩容慢）
histogram_quantile(0.9, rate(kueue_admission_checks_wait_time_seconds_bucket[10m]))

# 作业从提交到拿到配额的等待
histogram_quantile(0.5, rate(kueue_quota_reserved_wait_time_seconds_bucket[10m]))
```

---

## 8. 一页速查表

```text
# —— 用户侧（Job / JobSet / LWS / RayJob / Deployment ...）——
labels:
  kueue.x-k8s.io/queue-name: <localqueue>        # ★ 接入 Kueue（必须）
  kueue.x-k8s.io/priority-class: <wpc>           # WorkloadPriorityClass（推荐）
  kueue.x-k8s.io/max-exec-time-seconds: "86400"  # 超时自动 deactivate
annotations:
  kueue.x-k8s.io/job-min-parallelism: "4"        # 部分准入（batch/Job）
  # PodTemplate 级（TAS）：
  kueue.x-k8s.io/podset-required-topology: <label>       # 硬约束
  kueue.x-k8s.io/podset-preferred-topology: <label>      # 软约束
  kueue.x-k8s.io/podset-unconstrained-topology: "true"   # 仅记账
  kueue.x-k8s.io/podset-slice-required-topology: <label> # 子组硬约束（TP）
  kueue.x-k8s.io/podset-slice-size: "4"
  kueue.x-k8s.io/podset-group-name: <group>              # 多 PodSet 同 flavor/域

# —— 管理员侧 ——
ResourceFlavor: nodeLabels / nodeTaints / tolerations / topologyName
Topology:       levels（hostname 只能最低层）
Cohort:         parentName / resourceGroups / fairSharing.weight
ClusterQueue:
  namespaceSelector: {}                # ★ 默认 null = 全拒绝
  cohortName / queueingStrategy(BestEffortFIFO|StrictFIFO)
  resourceGroups[].flavors[]           # ★ 顺序 = 优先级
    nominalQuota   我应得的
    borrowingLimit 最多超出多少（null=无限；无 cohort 时必须 null）
    lendingLimit   最多借出多少（自留 = nominal − lending）★ 保底
  flavorFungibility: whenCanBorrow / whenCanPreempt / preference
  preemption: reclaimWithinCohort / withinClusterQueue / borrowWithinCohort
  stopPolicy: None|Hold|HoldAndDrain
  fairSharing.weight / admissionScope.admissionMode
LocalQueue:     clusterQueue
AdmissionCheck: controllerName（provisioning-request | multikueue）

# —— 排障三连 ——
kubectl -n <ns> describe workload <wl>            # 看 conditions + reason
kubectl get cq <cq> -o yaml | yq '.status'        # 看 Active + flavorsUsage
kubectl -n kueue-system logs deploy/kueue-controller-manager | grep <workload-name>
```

---

回到 [00 总览](00-Kueue总览与架构.md) ｜ 上一篇 [04 能力地图](04-面向大模型训练与推理的能力地图.md) ｜ 对比阅读 [Volcano 系列](../volcano/00-Volcano总览与架构.md)
