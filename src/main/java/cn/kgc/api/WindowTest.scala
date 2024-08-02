package cn.kgc.api

import cn.kgc.source.SensorReading
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import java.time.Duration

object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.socketTextStream("kb135", 7777)
    val dataStream = stream.map(
      data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )



    // 时间语义 1、处理时间（之前学习都基于处理时间）
    // 2、事件发生时间  指定时间语义，代码如下：

    // 有序流的waterMark生成
    /*dataStream.assignTimestampsAndWatermarks(
      WatermarkStrategy.forMonotonousTimestamps()
        .withTimestampAssigner(
          new SerializableTimestampAssigner[SensorReading] {
            override def extractTimestamp(element: SensorReading, recordTimestamp: Long): Long = {
              element.timestamp
            }
          }
        )
    )*/

    // 乱序流的waterMark生成
    val dataStream2 = dataStream.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(1))
        .withTimestampAssigner(
          new SerializableTimestampAssigner[SensorReading] {
            override def extractTimestamp(element: SensorReading, recordTimestamp: Long): Long = {
              element.timestamp * 1000
            }
          }
        )
    )

    env.execute("windowtest")
  }
}
