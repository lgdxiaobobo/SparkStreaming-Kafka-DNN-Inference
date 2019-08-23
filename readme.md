# SparkStreaming-Kafka Distributed DNN Inference

#### 0. Pre

- submit时需要添加spark-streaming-kafka-0-8-assembly_2.11-2.4.3.jar
- Python需要包：confluent_kafka

#### 1. 启动Zookeeper seever与Kafka server, 创建Topic

kafka目录下：

```shell
$ bin/zookeeper-server-start.sh config/zookeeper.properties
$ bin/kafka-server-start.sh config/server.properties
$ bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic imageStream
```

#### 2. 创建SparkStreamingContext开始监听

spark目录下：

```shell
$ bin/spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.4.3.jar ../project/inference_direct_kafka.py localhost:9092 imageStream
```

#### 3. 启动Producer发送图片数据

项目目录下：

```shell
$ python confluent_kafka_producer.py imageStream test_images
```

#### 4. 测试结果

![屏幕快照 2019-08-22 19.29.15](http://ww3.sinaimg.cn/large/006y8mN6ly1g69moefjzoj31k80pitgi.jpg)