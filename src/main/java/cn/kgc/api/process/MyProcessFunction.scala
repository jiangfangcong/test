package cn.kgc.api.process

import cn.kgc.source.SensorReading
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang

object MyProcessFunction extends ProcessFunction[SensorReading,SensorReading]{
  override def processElement(
                               value: SensorReading,
                               ctx: ProcessFunction[SensorReading, SensorReading]#Context,
                               out: Collector[SensorReading]): Unit = {

  }
}
