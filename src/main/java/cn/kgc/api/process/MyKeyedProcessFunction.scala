package cn.kgc.api.process

import cn.kgc.source.SensorReading
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

object MyKeyedProcessFunction extends KeyedProcessFunction[String, SensorReading, (String, Long, Double)] {
  override def processElement(
                               value: SensorReading,
                               ctx: KeyedProcessFunction[String, SensorReading, (String, Long, Double)]#Context,
                               out: Collector[(String, Long, Double)]): Unit = {

    val key = ctx.getCurrentKey
    val service = ctx.timerService()
  }
}
