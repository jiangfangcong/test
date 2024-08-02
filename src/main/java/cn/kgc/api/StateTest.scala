package cn.kgc.api

import cn.kgc.source.SensorReading
import org.apache.flink.api.common.functions.{FlatMapFunction, RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object StateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    env.getCheckpointConfig.setCheckpointInterval(1000)
    env.getCheckpointConfig.setCheckpointStorage("E:\\mavenspace\\flink_stu\\output")

    val stream = env.socketTextStream("kb135", 7777)

    val dataStream = stream.map(
      data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )

    // 需求：通过获取每个传感器对环境监控的温度，如果温度突然跳变5度以上，发出报警
    val alertStream = dataStream.keyBy(x => x.id).flatMap(new TempChangeAlert)
    alertStream.print("jumpTemp:")

    env.execute("statetest")
  }
}

// 学习状态编程
// 传感器传入SensorReading对象的temp值要被记录下来（状态编程），作用是方便与下一次数据的比较
// 传感器传入SensorReading对象的temp值与上一次的温度进行比较（状态编程取值）
class TempChangeAlert extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {


  val ttlConfig = StateTtlConfig
    .newBuilder(Time.seconds(10))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .build()

  // 定义状态对象，保存当前温度值，获取上一次温度值
  lazy val lastTempState: ValueState[Double] =
    getRuntimeContext.getState(new ValueStateDescriptor[Double]("last_temp", classOf[Double]))

  lazy val firstTagState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("firsttag", classOf[Boolean]))



  /*var lastTempState2:ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    lastTempState2 = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last_temp", classOf[Double]))
  }*/

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    val lastTempValue = lastTempState.value() // 取上一次的值与当前值比较
    val tag = firstTagState.value()

    if (!tag) {
      firstTagState.update(true)
    } else {
      val diff = (value.temperature - lastTempValue).abs
      if (diff > 5) {
        out.collect((value.id, lastTempValue, value.temperature))
      }
    }

    lastTempState.update(value.temperature) // 将当前值上传，方便下次比较
  }
}

class MyMap extends RichMapFunction[SensorReading, String] {
  lazy val value1: ValueState[String] = getRuntimeContext.getState(new ValueStateDescriptor[String]("temp", classOf[String]))

  override def map(value: SensorReading): String = {
    value.id
  }
}
