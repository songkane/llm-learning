# llm-d 源码学习

llm-d 是一套 Kubernetes 原生的分布式推理服务栈。本目录按**项目**收齐它已经写完的两套笔记——它们来自两个仓库、两套源码基线，所以仍分子目录，而不是合成一篇。

| 子专题 | 目录 | 仓库 | 源码基线 | 回答的问题 |
|--------|------|------|---------|-----------|
| Router（EPP） | [`router/`](router/) | [`llm-d-router`](https://github.com/llm-d/llm-d-router) | `main @ 90a28bc` | 这个请求发给哪个副本 |
| WVA | [`autoscaling/`](autoscaling/) | [`llm-d-autoscaling`](https://github.com/llm-d/llm-d-autoscaling) | `release-0.9 @ d5d5864` | 该开几个副本、加在哪个机型 |

> **WVA 决定「该有几个副本」，Router 决定「这个请求发给哪个副本」。** 两者互不依赖；建议先读 Router——WVA 的容量模型建立在对 KV token 供需的理解上，读过 Router 的 Data Layer（03 篇）会更顺。

问题域索引（只导航、不落正文）：

- 请求路由 → [`scheduling/README`](../../scheduling/README.md) 的「推理请求路由」一节
- 副本扩缩 → [`autoscaling/README`](../../autoscaling/README.md)

同层的另一套服务栈：[NVIDIA Dynamo](../dynamo/)。
