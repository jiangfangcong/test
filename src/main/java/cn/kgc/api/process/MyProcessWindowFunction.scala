package cn.kgc.api.process

import cn.kgc.source.SensorReading
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class MyProcessWindowFunction extends ProcessWindowFunction[SensorReading, (String, Long, Double), String, TimeWindow] {

  override def process(
                        key: String,
                        context: Context,
                        elements: Iterable[SensorReading],
                        out: Collector[(String, Long, Double)]): Unit = {
    val window = context.window
    val state = context.windowState
    println(window.getStart, window.getEnd)
    val iterator = elements.iterator
    var sumTemp = 0D
    while (iterator.hasNext) {
      sumTemp = sumTemp + iterator.next().temperature
    }
    out.collect(key, 0L, sumTemp)
  }
}
