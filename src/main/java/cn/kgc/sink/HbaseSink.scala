package cn.kgc.sink

import cn.kgc.source.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Admin, BufferedMutatorParams, Connection, ConnectionFactory, Put, Table}

object HbaseSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.socketTextStream("kb135", 7777)

    val dataStream = stream.map(
      data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )

    dataStream.addSink(
      new MyJdbcSink
    )

    env.execute("jdbcsink")
  }
}

class MyHbaseSink extends RichSinkFunction[SensorReading]{


  override def open(parameters: Configuration): Unit = {

  }

  override def invoke(value: SensorReading, context: SinkFunction.Context): Unit = super.invoke(value, context)

  override def close(): Unit = super.close()

}
