## Point

mapreduce 总体过程：

1. Worker 注册
2. Master 任务初始化
3. 持续监听与任务指派
4. Map 任务执行:
   + 执行 map 用户逻辑
   + shuffle 阶段(分区): Map 不会将中间结果写入一个文件中, 避免多个 Reduce 读取. 而是所有 Map 将相同的 key 写入同一个分区文件中. 这样聚合指定 key 的 Reduce 只拉去指定分区即可, 实现并行处理.
   + 改变状态,通知 Master 执行完成
5. Reduce 任务执行:
    + 分区拉取阶段: 等待 map 执行完成, 与 master 通信后拉取指定 key 所在分区文件.
    + 计算阶段: 必须等待所有 map 执行完成后才能进行聚合, 即必须拉取 key 的所有 value .一个 Reduce 任务会处理指定分区的所有 key.
    + 写入 Reduce 最终输出文件
    + 改变状态,通知 Master 执行完成


## `Pull&Push Model`

`Master` 几乎不会主动联系 `worker`，都是 `worker` 通过 gRPC 调用 `Master` 的远程服务。


## Worker 请求时，Master 需要检查三个东西:

1. id  --> WorkerNotExist -->  注册
2. generation --> NoSameGeneration  --> Shutdown, 改变 Status 为 WorkerStatusShutdown，从此该 worker 不会复用
3. Status(Master 存储) --> Status 状态为 Dead、Shudown 状态不对:
    + Dead  --> 回复重新注册 Reregister （Worker 状态清零，所执行的任务重新分配）
    + Shutdown  --> 表明 Master 向 worker 发送 Shutdown 信号，但是 worker 没有执行，且继续工作，继续发送 Shutdown 信号，后续可以直接加入黑名单
    + TaskApply 状态应该是 Idle；TaskCompletion 状态应该是 Buzy。


## 超时机制

worker 存活对时间不需要太敏感，因此采用 “周期扫描” 的方法检查超时：每隔一段固定时间（15s）扫描所有 Worker 的 LastPing 时间戳，如果 Now() - LastPing > 10s 视为超时，
重置 Worker 状态，重新分发任务，设置 Worker 状态为 Dead。

## 错误状态处理

所有的错误状态（WorkerNotExist、WorkerNoSameGeneration、WorkerDead、WorkerShutdown 等）都只在 Heartbeat 中处理，其他服务返回 Signal_wait，避免多个服务同时处理错误状态。
Worker 的状态一旦被设置为 Shutdown 就不再接受该 worker 请求，且状态不会更改；Dead 状态可以再次 Register，但他们都需要将执行的任务重置状态，以便重新分发。
