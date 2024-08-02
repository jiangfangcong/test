package cn.kgc.work

import cn.kgc.source.SensorReading
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.co.{CoMapFunction, CoProcessFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutputTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.socketTextStream("kb135", 7778)

    val dataStream = stream.map(new StudentMapFunction2)

    val scoreStream = dataStream.process(new MyProcessor)
    // scoreStream.print("优秀")

    val lianghao = scoreStream.getSideOutput(new OutputTag[(String, String, String, Int)]("良好"))
    // lianghao.print("良好")

    val jige = scoreStream.getSideOutput(new OutputTag[(String, String, String, Int)]("及格"))
    // jige.print("及格")

    val bujige = scoreStream.getSideOutput(new OutputTag[(String, String, String, Int)]("不及格"))
    // bujige.print("不及格")

    val youxiuAndlianghao = scoreStream.connect(lianghao)


    env.execute("sideoutput")
  }
}

class MyProcessor extends ProcessFunction[StudentInfo, StudentInfo] {
  override def processElement(value: StudentInfo, ctx: ProcessFunction[StudentInfo, StudentInfo]#Context, out: Collector[StudentInfo]): Unit = {
    if (value.score >= 90 && value.score <= 100) {
      out.collect(value)
    } else if (value.score >= 75 && value.score < 90) {
      ctx.output(new OutputTag[(String, String, String, Int)]("良好"), (value.classId, value.course, value.name, value.score))
    } else if (value.score >= 60 && value.score < 75) {
      ctx.output(new OutputTag[(String, String, String, Int)]("及格"), (value.classId, value.course, value.name, value.score))
    } else if (value.score >= 0 && value.score < 60) {
      ctx.output(new OutputTag[(String, String, String, Int)]("不及格"), (value.classId, value.course, value.name, value.score))
    }
  }
}

class StudentMapFunction2 extends MapFunction[String, StudentInfo] {
  override def map(value: String): StudentInfo = {
    val arr = value.split(",")
    StudentInfo(arr(0), arr(1), arr(2), arr(3).toInt)
  }
}
