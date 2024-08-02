package cn.kgc.work

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object FindMaxScore {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.socketTextStream("kb135", 7777)

    val dataStream = stream.map(new StudentMapFunction)

    val kb23Stream = dataStream.filter(x => if (x.classId == "kb23") true else false)

    val kb23Stream2 = kb23Stream.filter(x => if (x.course == "java") true else false)

    val kb23Stream3 = kb23Stream2.keyBy(x => x.course)

    val stuStream = kb23Stream3.maxBy("score")

    stuStream.print("maxJavaScore")

    env.execute()
  }
}

case class StudentInfo(classId: String, course: String, name: String, score: Int)

class StudentMapFunction extends MapFunction[String, StudentInfo] {
  override def map(value: String): StudentInfo = {
    val arr = value.split(",")
    StudentInfo(arr(0), arr(1), arr(2), arr(3).toInt)
  }
}
