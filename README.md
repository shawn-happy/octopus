# octopus

# 介绍

operators模块主要支持Spark,Flink计算引擎，后续会模拟Seatunnel，统一配置，然后选择不同的计算引擎，可以做数据集成，数据开发，数据质量等计算任务。

actus模块目前支持mysql，doris，用于通过sql建模。主要封装了一个可以通过jdbc连接的数据库，通过jdbc来进行模型设计。未来会扩展更多的数据类型，提供元数据服务，提供mq，nosql等其他数据源类型。