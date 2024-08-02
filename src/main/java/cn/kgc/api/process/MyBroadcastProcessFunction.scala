package cn.kgc.api.process

import cn.kgc.source.SensorReading
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector

object MyBroadcastProcessFunction extends BroadcastProcessFunction[SensorReading,(String,Long,Double),(String,Double)]{
  override def processElement(
                               value: SensorReading,
                               ctx: BroadcastProcessFunction[SensorReading, (String, Long, Double), (String, Double)]#ReadOnlyContext,
                               out: Collector[(String, Double)]): Unit = {
    
  }

  override def processBroadcastElement(value: (String, Long, Double),
                                       ctx: BroadcastProcessFunction[SensorReading, (String, Long, Double), (String, Double)]#Context,
                                       out: Collector[(String, Double)]): Unit = {

  }
}

