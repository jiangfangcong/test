package jmz

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object BoundedStreamWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val lineDataStream = env.readTextFile("input/word.txt")
    val wordAndOne = lineDataStream.flatMap(_.split(" ")).map(x => (x, 1))
    val wordAndOneGroup = wordAndOne.keyBy(_._1)
    val sum = wordAndOneGroup.sum(1)
    sum.print()
  }
}
