package cn.kgc.api.process

import cn.kgc.source.SensorReading
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class MyProcessAllWindowFunction extends ProcessAllWindowFunction[SensorReading,(String,Long,Double),TimeWindow]{
  override def process(
                        context: Context,
                        elements: Iterable[SensorReading],
                        out: Collector[(String, Long, Double)]): Unit = {

  }
}
