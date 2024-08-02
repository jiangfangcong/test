package cn.kgc.source

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig

import java.util.Properties
import scala.util.Random

case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    // 1.创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // env.setParallelism(1)
    // 2.加载数据源
    // val stream = env.fromElements(1, 2, 3, 4, 5, "hello", 3.14)

    /*val stream = env.fromCollection(List(
      SensorReading("sensor_1", 1698731556, 26.3),
      SensorReading("sensor_2", 1698731556, 26.7),
      SensorReading("sensor_3", 1698731556, 26.1),
      SensorReading("sensor_3", 1698731550, 26.0)
    ))*/

    // val stream = env.readTextFile("E:\\mavenspace\\flink_stu\\src\\main\\resources\\sensor.txt")

    //    val stream = env.socketTextStream("192.168.153.135", 7777)
    //    val stream1 = stream.map(x => x.split(",")).flatMap(x => x).map(x => (x, 1)).keyBy(x=>x._1).sum(1)

    /*val prop = new Properties()
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kb135:9092")
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "sensorGroup1")
    prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

    val stream = env.addSource(
      new FlinkKafkaConsumer[String]("sensor", new SimpleStringSchema(), prop)
    )
    val wordCount = stream.flatMap(x => x.split(" ")).map(x => (x, 1)).keyBy(x => x._1)
      .reduce((x: (String, Int), y: (String, Int)) => (x._1, x._2 + y._2))*/

    val steam = env.addSource(new MySensorSource)

    // 4.输出，也叫下沉
    steam.print("")

    env.execute("sourcetest")
  }
}

// 模拟自定义数据源
class MySensorSource extends SourceFunction[SensorReading] {
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    val random = new Random()
    while (true) {
      val d = Math.random()
      ctx.collect(SensorReading("随机数：" + random.nextInt(), System.currentTimeMillis(), d))
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {

  }
}
