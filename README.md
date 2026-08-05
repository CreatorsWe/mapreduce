# MapReduce 轻量级分布式计算框架

[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat&logo=go)](https://golang.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## 📖 项目简介

本项目是从零实现的**轻量级MapReduce分布式计算框架**，旨在以最简代码（约2000行）揭示分布式调度、容错和并行计算的核心原理。框架采用经典的 **Master-Worker** 架构，支持用户自定义Map和Reduce函数，能弹性扩展Worker节点，并具备基本的**任务失败重试**能力。

这不仅是一个工具，更是理解Hadoop、Spark等大数据系统底层机制的绝佳实践。

## ✨ 核心特性

*   **Master-Worker架构**：Master负责任务调度与状态管理，Worker主动拉取任务并执行，实现负载均衡。
*   **自定义计算逻辑**：提供简洁的`map`和`reduce`函数接口，用户只需实现业务逻辑即可处理不同类型数据。
*   **基础容错机制**：Master能检测Worker故障，并将失败任务**重新调度**至其他健康Worker，保证作业最终完成。
*   **Go原生并发模型**：充分利用Goroutine和Channel，实现高效的RPC通信与任务队列管理。
*   **RPC通信**：基于Go标准库`net/rpc`，实现Master与Worker间可靠、简单的网络交互。

## 🏗️ 系统架构

```mermaid
graph TD
    Client[客户端] --> Master
    subgraph Master节点
        Master[Master<br>任务调度 & 状态管理]
        TaskQueue[任务队列]
        WorkerMap[Worker注册表]
    end

    Master -- 分配任务/下发指令 --> Worker1
    Master -- 分配任务/下发指令 --> Worker2
    Master -- 分配任务/下发指令 --> WorkerN

    subgraph Worker节点
        Worker1[Worker 1] --> |RPC拉取任务| Master
        Worker2[Worker 2] --> |RPC拉取任务| Master
        WorkerN[Worker N] --> |RPC拉取任务| Master
    end

    Worker1 -- 读取 --> Input[输入数据分片]
    Worker2 -- 读取 --> Input
    WorkerN -- 读取 --> Input

    Worker1 -- 写入 --> Intermediates[中间结果<br>本地文件]
    Worker2 -- 写入 --> Intermediates
    WorkerN -- 写入 --> Intermediates

    Worker1 -- 读取 --> Intermediates
    Worker2 -- 读取 --> Intermediates
    WorkerN -- 读取 --> Intermediates

    Worker1 -- 写入 --> Output[最终输出]
    Worker2 -- 写入 --> Output
    WorkerN -- 写入 --> Output
