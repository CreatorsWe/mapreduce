todo-improve:
    switch method of input file from that a Map task handle a file to that a Map task handle a part (path, from, to).
    a file can be read by muliti-object.

失联逻辑：
    WorkerInfo 失联必须清除所有的状态（包括完成的任务和正在执行的任务），收到 Shutdwon 信号则关闭；收到 Register 则重新注册（但以失联）
    Master 立即将失联的 workerInfo 执行的所有任务进行重新分配，并清除记录的 workerInfo 状态
