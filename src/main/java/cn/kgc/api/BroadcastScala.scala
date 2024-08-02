package cn.kgc.api

import org.apache.flink.api.common.state.{MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

/**
 * "zs,login" "ls,login" "zs,pay" "ls,car" "ww,login"
 *
 * 需求:找到 "login,pay" 用户
 */

case class Action(userId: String, action: String)

case class Pattern(action1: String, action2: String)

object BroadcastScala {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val actionStream = env.socketTextStream("192.168.153.135", 7777)
      .map(x => Action(x.split(",")(0), x.split(",")(1)))
    //    val patternStream = env.fromElements(Pattern("login", "pay"))
    val patternStream = env.socketTextStream("192.168.153.135", 7778)
      .map(x => Pattern(x.split(",")(0), x.split(",")(1)))
    //    val actionStream = env.fromElements(Action("zs", "login"), Action("zs", "pay"))

    val patternDescriptor = new MapStateDescriptor[Unit, Pattern]("patterns", classOf[Unit], classOf[Pattern])
    val broadcastStream = patternStream.broadcast(patternDescriptor)

    actionStream.keyBy(x => x.userId).connect(broadcastStream)
      .process(new MyPatternFunction).print()

    env.execute()
  }
}

class MyPatternFunction extends KeyedBroadcastProcessFunction[String, Action, Pattern, (String, String, Pattern)] {

  //记录每一位用户的上一个操作行为
  lazy private val preActionState: ValueState[String] = getRuntimeContext.getState(new ValueStateDescriptor[String]("pre-action", classOf[String]))

  override def processElement(
                               value: Action,
                               ctx: KeyedBroadcastProcessFunction[String, Action, Pattern, (String, String, Pattern)]#ReadOnlyContext,
                               out: Collector[(String, String, Pattern)]): Unit = {
    println("processElement: " + value)

  }

  override def processBroadcastElement(
                                        value: Pattern,
                                        ctx: KeyedBroadcastProcessFunction[String, Action, Pattern, (String, String, Pattern)]#Context,
                                        out: Collector[(String, String, Pattern)]): Unit = {
    println("processBroadcastElement" + value)
  }
}
