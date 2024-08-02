package cn.kgc.api.process

import cn.kgc.source.SensorReading
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector

object MyKeyedBroadcastProcessFunction extends KeyedBroadcastProcessFunction[String,SensorReading,SensorReading,(String,Long,Double)]{
  override def processElement(
    value: SensorReading,
    ctx: KeyedBroadcastProcessFunction[String, SensorReading, SensorReading, (String, Long, Double)]#ReadOnlyContext,
    out: Collector[(String, Long, Double)]): Unit = {

  }

  override def processBroadcastElement(
    value: SensorReading,
    ctx: KeyedBroadcastProcessFunction[String, SensorReading, SensorReading, (String, Long, Double)]#Context,
    out: Collector[(String, Long, Double)]): Unit = {
    
  }
}
