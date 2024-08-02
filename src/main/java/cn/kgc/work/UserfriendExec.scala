package cn.kgc.work

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import java.util.Properties

object UserfriendExec {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val prop = new Properties()
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kb135:9092")
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test001")
    prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

    val stream = env.addSource(new FlinkKafkaConsumer[String]("user_friends_row", new SimpleStringSchema(), prop))

    val stream2 = stream.map(x => {
        x.split(",")
      })
      .filter(x => x.length == 2)
      .map(x => (x(0), x(1).split(" ")))
      .flatMap(x => {
        for (friend <- x._2) yield x._1 + "," + friend
      })


    stream2.print()

    env.execute()
  }
}
