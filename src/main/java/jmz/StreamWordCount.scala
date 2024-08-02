package jmz

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // val lineDataStream = env.socketTextStream("kb135",7777)

    val parameterTool = ParameterTool.fromArgs(args)
    val hostname = parameterTool.get("host")
    val port = parameterTool.getInt("port")
    val lineDataStream = env.socketTextStream(hostname, port)
    val wordAndOne = lineDataStream.flatMap(_.split(" ")).map(x => (x, 1))
    val wordAndOneGroup = wordAndOne.keyBy(_._1)
    val sum = wordAndOneGroup.sum(1)
    sum.print()
    env.execute()
  }
}
