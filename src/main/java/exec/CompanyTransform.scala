package exec

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

case class OriginalCompanyInfo(comName: String, owner: String, comId: String, tel: String, referee: String, startTime: String)

case class CompanyInfo(comName: String, owner: String, cityName: String, hasReferee: Int)

case class CityCode(cityId: String, cityName: String)

object CompanyTransform {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val originalCompanyInfoStream = env.socketTextStream("192.168.153.135", 7777)
      .map(x => OriginalCompanyInfo(x.split(",")(0), x.split(",")(1), x.split(",")(2), x.split(",")(3), x.split(",")(4), x.split(",")(5)))

    val cityCodeStream = env.socketTextStream("192.168.153.135", 7778)
      .map(x => CityCode(x.split(",")(0), x.split(",")(1)))

    val cityCodeList = new MapStateDescriptor[String, CityCode]("cityCodeList", classOf[String], classOf[CityCode])

    val cityCodeBroadcastStream = cityCodeStream.broadcast(cityCodeList)

    val resultStream = originalCompanyInfoStream.connect(cityCodeBroadcastStream).process(new MyBroadcastProcessFunction)

    resultStream.print("result: ")

    env.execute()
  }
}

class MyBroadcastProcessFunction extends BroadcastProcessFunction[OriginalCompanyInfo, CityCode, CompanyInfo] {
  override def processElement(
                               value: OriginalCompanyInfo,
                               ctx: BroadcastProcessFunction[OriginalCompanyInfo, CityCode, CompanyInfo]#ReadOnlyContext,
                               out: Collector[CompanyInfo]
                             ): Unit = {
    val cityState = ctx.getBroadcastState(new MapStateDescriptor[String, CityCode]("cityCodeList", classOf[String], classOf[CityCode]))
    val code = cityState.get(value.comId.substring(0, 6))
    var cityName: String = "not find"
    if (code != null)
      cityName = code.cityName
    val referee: String = value.referee
    var tag = "0"
    if (referee != null && referee.nonEmpty) tag = "1"
    out.collect(CompanyInfo(value.comName, value.owner, cityName, tag.toInt))

  }

  override def processBroadcastElement(
                                        value: CityCode,
                                        ctx: BroadcastProcessFunction[OriginalCompanyInfo, CityCode, CompanyInfo]#Context,
                                        out: Collector[CompanyInfo]
                                      ): Unit = {
    val cityBroadcastState = ctx.getBroadcastState(new MapStateDescriptor[String, CityCode]("cityCodeList", classOf[String], classOf[CityCode]))
    cityBroadcastState.put(value.cityId,value)
  }
}
