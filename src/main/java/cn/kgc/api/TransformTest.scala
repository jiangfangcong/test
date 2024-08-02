package cn.kgc.api

import cn.kgc.sink.MyJdbcSink
import cn.kgc.source.SensorReading
import org.apache.flink.api.common.functions.{FilterFunction, FlatMapFunction, MapFunction, ReduceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.socketTextStream("kb135", 7777)

    /*val dataStream = stream.map(
      data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )*/

    /* val dataStream = stream.map(new MapFunction[String, SensorReading] {
       override def map(value: String): SensorReading = {
         val arr = value.split(",")
         SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
       }
     })*/

    val dataStream = stream.map(new MyMapFunction)

    //dataStream.print()

    // val filterStream = dataStream.filter(x => x.id == "sensor_2")
    /*val filterStream = dataStream.filter(new FilterFunction[SensorReading] {
      override def filter(value: SensorReading): Boolean = {
        if (value.id == "sensor_2") true else false
      }
    })*/

    /*val filterStream = dataStream.filter(new MyFilterFunction)

    filterStream.print("sensor_2 value:")*/

    val keyStream = dataStream.keyBy(x => x.id)
    //    val maxStream = keyStream.max("temperature")
    // val maxStream = keyStream.maxBy("temperature")
    // val maxStream = keyStream.maxBy(2)
    //    val sumStream = keyStream.sum("temperature")
    //    sumStream.print("maxvalue:")

    // val reduceMinStream = keyStream.reduce((x, y) => if (x.temperature < y.temperature) x else y)
    val reduceMinStream = keyStream.reduce(new ReduceFunction[SensorReading] {
      override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
        if (value1.temperature < value2.temperature) {
          value1
        } else {
          SensorReading(value1.id, value1.timestamp, value2.temperature)
        }
      }
    })

    reduceMinStream.print()

    env.execute("transformtest")
  }
}

class MyMapFunction extends MapFunction[String, SensorReading] {
  override def map(value: String): SensorReading = {
    val arr = value.split(",")
    SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
  }
}

class MyFilterFunction extends FilterFunction[SensorReading] {
  override def filter(value: SensorReading): Boolean = {
    if (value.id == "sensor_2") true else false
  }
}
