package cn.kgc.api

import cn.kgc.api.process.{MyProcessAllWindowFunction, MyProcessWindowFunction}
import cn.kgc.source.SensorReading
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.{ProcessingTimeSessionWindows, SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang

object WindowTest2 {
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

    val windowedStream = dataStream.keyBy(x => x.id)
      .window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(2))) // 滑动窗口
     // .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))      // 滚动窗口
     // .window(ProcessingTimeSessionWindows.withGap(Time.minutes(30)))   // 会话窗口 30分钟内没有任何操作，退出窗口
     // .countWindow(10)  // 计数窗口
    //  .countWindow(10,2)  // 计数滑动窗口

    val value = windowedStream.process(new MyProcessWindowFunction)

    value.print("")
    //    val resultStream = windowedStream.max("temperature")

    /*val resultStream = windowedStream.reduce((curRec, newRec) => {
      SensorReading(curRec.id, curRec.timestamp, curRec.temperature.max(newRec.temperature))
    })
    resultStream.print("max temperature:")*/

    /*windowedStream.process(new MyProcessFunction)*/

    env.execute()
  }
}

class MyProcessFunction extends ProcessWindowFunction[SensorReading, SensorReading, String, TimeWindow] {

  override def process(
                        key: String,
                        context: Context,
                        elements: Iterable[SensorReading],
                        out: Collector[SensorReading]): Unit = {
    val window = context.window
    val state = context.windowState

    var id = key
    var timestamp = 0L
    var temp = 0D
    while (elements.iterator.hasNext) {
      var reading = elements.iterator.next()
      timestamp = reading.timestamp
      temp = temp + reading.temperature
    }
    SensorReading(id, timestamp, temp)
  }
}
