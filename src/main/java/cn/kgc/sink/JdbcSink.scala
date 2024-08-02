package cn.kgc.sink

import cn.kgc.source.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import java.sql.{Connection, DriverManager, PreparedStatement}

object JdbcSink {
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

class MyJdbcSink extends RichSinkFunction[SensorReading] { //SinkFunction[SensorReading]{

  var connection: Connection = _
  var insertStatement: PreparedStatement = _
  var updateStatement: PreparedStatement = _


  override def open(parameters: Configuration): Unit = {
    connection = DriverManager.getConnection(
      "jdbc:mysql://kb135:3306/kb23?useSSL=false", "root", "123456"
    )

    insertStatement = connection.prepareStatement("INSERT INTO sensor_temp VALUES (?,?,?)")
    updateStatement = connection.prepareStatement("update sensor_temp set timestamp=?,temp=? where id=?")

    println("insertStatement:" + insertStatement)
    println("updateStatement:" + updateStatement)
  }

  override def invoke(value: SensorReading, context: SinkFunction.Context): Unit = {
    updateStatement.setLong(1, value.timestamp)
    updateStatement.setDouble(2, value.temperature)
    updateStatement.setString(3, value.id)
    val num: Int = updateStatement.executeUpdate()
    if (num == 0) {
      insertStatement.setString(1, value.id)
      insertStatement.setLong(2, value.timestamp)
      insertStatement.setDouble(3, value.temperature)
      insertStatement.execute()
    }
  }

  override def close(): Unit = {
    if (insertStatement != null) {
      insertStatement.close()
    }
    if (updateStatement != null) {
      updateStatement.close()
    }

    if (connection != null) {
      connection.close()
    }
  }
}
