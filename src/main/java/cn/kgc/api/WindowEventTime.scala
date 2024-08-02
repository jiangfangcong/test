package cn.kgc.api

import cn.kgc.source.SensorReading
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration

object WindowEventTime {
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
    val dataStream2 = dataStream.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3))
        .withTimestampAssigner(
          new SerializableTimestampAssigner[SensorReading] {
            override def extractTimestamp(element: SensorReading, recordTimestamp: Long): Long = {
              element.timestamp * 1000
            }
          }
        )
    )

    val windowStream = dataStream2.keyBy(_.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(15)))

    /*val resultStream = windowStream.reduce(
      (curReduce, newReduce) => {
        SensorReading(curReduce.id, curReduce.timestamp, curReduce.temperature.min(newReduce.temperature))
      }
    )*/

    val resultStream = windowStream.process(new MyEventProcessWindowFunction)

    resultStream.print("result: ")

    env.execute()
  }
}

class MyEventProcessWindowFunction extends ProcessWindowFunction[SensorReading,SensorReading,String,TimeWindow]{
  override def process(
                        key: String,
                        context: Context,
                        elements: Iterable[SensorReading],
                        out: Collector[SensorReading]): Unit = {
    val window = context.window
    println(window.getStart,window.getEnd)

    var temp = 100D
    var timestamp = 0L
    while (elements.iterator.hasNext){
      temp = temp.min(elements.iterator.next().temperature)
      timestamp = elements.iterator.next().timestamp
    }
    out.collect(SensorReading(key,timestamp,temp))
  }
}
