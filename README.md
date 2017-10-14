# lambda-demo
big data lambda architecture

## 架构概述
结合创业公司资源分析和市面上的开源技术方案，最终采用以下架构：
- 架构整体参考 Lambda 大数据架构，系统分为批处理层、实时层以及服务层。
- 批处理层使用 HDFS 作为主数据集。Spark Core 作为批处理计算框架。
- 实时层使用 Spark Streaming 作为微批处理框架。（对于实时性要求过高的则采用 storm 更合适）
- 批处理视图以及实时视图均采用 Cassandra 作为视图存储。
- 服务层采用 Spring Boot 套件开发。
- 客户端数据采集使用 Flume + Kafka。
  - 地方大幅度
  - 上游所有数据都流经 kafka。
  - 如果有复杂的 ETL 过程，则采用 gobblin。
  - 若数据结构单一，不存在复杂的 ETL 则可以继续使用 flume 直接落地至 HDFS。
- 数据序列化传输使用 Avro。

以上架构未引入交互式数据分析框架。

## TODO
1. 上架构图
