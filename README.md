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
