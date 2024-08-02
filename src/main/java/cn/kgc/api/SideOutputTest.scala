package cn.kgc.api

import cn.kgc.source.SensorReading
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.co.{CoMapFunction, CoProcessFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutputTest {
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

    // 分流
    val normalStream = dataStream.process(new SplitTempProcessor)
    //    normalStream.print("normal")

    val lowStream = normalStream.getSideOutput(new OutputTag[(String, Long, Double)]("low"))
    //    lowStream.print("low")

    val alarmStream = normalStream.getSideOutput(new OutputTag[(String, Long, Double)]("alarm"))
    //    alarmStream.print("alarm")

    val highStream = normalStream.getSideOutput(new OutputTag[(String, Long, Double)]("high"))
    //    highStream.print("high")

    // 合流
    val unionStream = lowStream.union(highStream) //两条要合并的流类型要相同
    //unionStream.print("unionStream:")

    val connectedStream = normalStream.connect(alarmStream)
    /*val connectedStream2 = connectedStream.map(
      data1 => {
        (data1.id, data1.temperature)
      },
      data2 => {
        (data2._1, data2._3)
      }
    )*/
    /*val connectedStream2 = connectedStream.map(new CoMapFunction[SensorReading, (String, Long, Double), (String, Double)] {
      override def map1(value: SensorReading): (String, Double) = {
        (value.id, value.temperature)
      }

      override def map2(value: (String, Long, Double)): (String, Double) = {
        (value._1, value._3)
      }
    })
    connectedStream2.print("connectedStream2:")*/

    val value1 = connectedStream.process(new CoProcessFunction[SensorReading, (String, Long, Double), (String, Double)] {
      override def processElement1(value: SensorReading, ctx: CoProcessFunction[SensorReading, (String, Long, Double),
        (String, Double)]#Context, out: Collector[(String, Double)]): Unit = {
        out.collect((value.id, value.temperature))
      }

      override def processElement2(value: (String, Long, Double), ctx: CoProcessFunction[SensorReading, (String, Long, Double),
        (String, Double)]#Context, out: Collector[(String, Double)]): Unit = {
        out.collect((value._1, value._3))
      }
    })
    value1.print()


    env.execute("sideoutput")
  }
}

/**
 * 自定义processFunction
 * 分流操作
 */
class SplitTempProcessor extends ProcessFunction[SensorReading, SensorReading] {
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    if (value.temperature > 40.0 && value.temperature < 80.0) {
      out.collect(value)
    } else if (value.temperature <= 40.0) {
      ctx.output(new OutputTag[(String, Long, Double)]("low"), (value.id, value.timestamp, value.temperature))
    } else if (value.temperature >= 100.0) {
      ctx.output(new OutputTag[(String, Long, Double)]("alarm"), (value.id, value.timestamp, value.temperature))
    } else {
      ctx.output(new OutputTag[(String, Long, Double)]("high"), (value.id, value.timestamp, value.temperature))
    }
  }
}
