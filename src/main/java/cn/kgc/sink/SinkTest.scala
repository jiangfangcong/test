package cn.kgc.sink

import cn.kgc.source.SensorReading
import org.apache.flink.api.common.serialization.{SerializationSchema, SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object SinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.readTextFile("E:\\mavenspace\\flink_stu\\src\\main\\resources\\sensor.txt")
    val dataStream = stream.map(
      data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )

    // dataStream.print()

    // stream.writeToSocket("192.168.153.135",7777,new SimpleStringSchema())

    dataStream.addSink(
      StreamingFileSink.forRowFormat(
        new Path("E:\\mavenspace\\flink_stu\\src\\main\\resources\\out.txt"),
        new SimpleStringEncoder[SensorReading]()
      ).build()
    )

    env.execute("sinktest")
  }
}
