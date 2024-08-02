package cn.kgc.api.process

import cn.kgc.source.SensorReading
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.util.Collector

object MyProcessJoinFunction extends ProcessJoinFunction[SensorReading,SensorReading,(String,Long,Double)]{
  override def processElement(
    left: SensorReading,
    right: SensorReading,
    ctx: ProcessJoinFunction[SensorReading, SensorReading, (String, Long, Double)]#Context,
    out: Collector[(String, Long, Double)]): Unit = {

  }
}
