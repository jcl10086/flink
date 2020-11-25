package org.jcl.kafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer


object KafkaConsumer {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "master:9092")
    properties.setProperty("group.id", "test")
    val consumer = new FlinkKafkaConsumer[String]("hello", new SimpleStringSchema(), properties)
    consumer.setStartFromEarliest()

    env.addSource(consumer).map(x => (x,1))
      .keyBy(_._1).sum(1)
        .map(x => (x._1,x._2)).print()

    env.execute("kafka")
  }
}
