package jmz

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object BatchWordCount {
  def main(args: Array[String]): Unit = {
    // 创建流式环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    val lineDataSet = env.readTextFile("input/word.txt")
    val wordAndOne = lineDataSet.flatMap(_.split(" ")).map(x => (x, 1))
    val wordAndOneGroup = wordAndOne.groupBy(0)
    val sum = wordAndOneGroup.sum(1)
    sum.print()

    // 执行任务
    // env.execute("batchwordcount")
  }
}
