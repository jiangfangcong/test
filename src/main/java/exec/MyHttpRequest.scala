package exec

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

case class HttpInfo(deviceIp: String, httpAddress: String, datetime: String)

case class RequestType(reqNum: String, reqType: String)

object MyHttpRequest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val httpInfoStream = env.socketTextStream("192.168.153.135", 7777)
      .map(x => HttpInfo(x.split("\t")(0), x.split("\t")(1), x.split("\t")(2)))

    val requestTypeStream = env.socketTextStream("192.168.153.135", 7778)
      .map(x => RequestType(x.split(",")(0), x.split(",")(1)))

    val requestType = new MapStateDescriptor[String, RequestType]("requestType", classOf[String], classOf[RequestType])

    val requestTypeBroadcastStream = requestTypeStream.broadcast(requestType)

    val resultStream = httpInfoStream.connect(requestTypeBroadcastStream).process(new MyBroadcastProcessFunction2)

    resultStream.print("result: ")

    env.execute()

  }
}

class MyBroadcastProcessFunction2 extends BroadcastProcessFunction[HttpInfo, RequestType, (String, String, String)] {
  override def processElement(
                               value: HttpInfo,
                               ctx: BroadcastProcessFunction[HttpInfo, RequestType, (String, String, String)]#ReadOnlyContext,
                               out: Collector[(String, String, String)]): Unit = {
    val value1 = ctx.getBroadcastState(new MapStateDescriptor[String, RequestType]("requesttype", classOf[String], classOf[RequestType]))
    val requestNum = value1.get(value.deviceIp.split(".")(0))
    var requestType = "外部"
    if (requestNum.equals("192")) {
      requestType = "内部"
    } else if (requestNum.equals("76") || requestNum.equals("78")) {
      requestType = "集团"
    }
    out.collect(value.deviceIp, value.httpAddress, requestType)
  }

  override def processBroadcastElement(
                                        value: RequestType,
                                        ctx: BroadcastProcessFunction[HttpInfo, RequestType, (String, String, String)]#Context,
                                        out: Collector[(String, String, String)]): Unit = {
    val requesttypeState = ctx.getBroadcastState(new MapStateDescriptor[String, RequestType]("requesttype", classOf[String], classOf[RequestType]))

    requesttypeState.put(value.reqNum, value)
  }
}
