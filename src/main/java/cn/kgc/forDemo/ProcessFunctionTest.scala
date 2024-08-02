package cn.kgc.forDemo

import cn.kgc.source.SensorReading
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang

object ProcessFunctionTest {
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

    dataStream.process(new ProcessFunction[SensorReading,String]{
      override def processElement(
        value: SensorReading,
        ctx: ProcessFunction[SensorReading, String]#Context,
        out: Collector[String]): Unit = {
        if (value.id.equals("mary")){
          out.collect(value.id)
        }else if (value.id.equals("bob")){
          out.collect(value.id)
          out.collect(value.id)
        }
        println(ctx.timerService().currentWatermark())
      }
    }).print()

    env.execute("windowtest")
  }
}


