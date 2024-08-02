package cn.kgc.api.process

import cn.kgc.source.SensorReading
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector

object MyCoProcessFunction extends CoProcessFunction[SensorReading,SensorReading,(String,Long,Double)]{
  override def processElement1(
                                value: SensorReading,
                                ctx: CoProcessFunction[SensorReading, SensorReading, (String, Long, Double)]#Context,
                                out: Collector[(String, Long, Double)]): Unit = {

  }

  override def processElement2(value: SensorReading,
                               ctx: CoProcessFunction[SensorReading, SensorReading, (String, Long, Double)]#Context,
                               out: Collector[(String, Long, Double)]): Unit = {

  }
}
