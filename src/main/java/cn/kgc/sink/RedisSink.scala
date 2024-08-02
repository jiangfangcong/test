package cn.kgc.sink


import cn.kgc.source.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import redis.clients.jedis.Jedis

object RedisSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    /*val stream = env.socketTextStream("192.168.153.135", 7777)
    val dataStream = stream.map(
      data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )*/

    val dataStream = env.addSource(new MyRedisSourceFunction)

    //redis配置
    /*val redisConfig =
      new FlinkJedisPoolConfig.Builder()
        .setHost("192.168.153.135")
        .setPort(6379)
        .build()
    dataStream.addSink(new RedisSink[SensorReading](redisConfig, new MyRedisMapper))*/

    dataStream.print("sensor: ")

    env.execute("")
  }
}

class MyRedisSourceFunction extends RichSourceFunction[(String, String)] {

  private var jedis: Jedis = _

  override def run(ctx: SourceFunction.SourceContext[(String, String)]): Unit = {
    jedis = new Jedis("192.168.153.135", 6379)

    val value = jedis.hget("sensor","ssss")

    ctx.collect("ssss",value)
  }

  override def cancel(): Unit = {

  }
}

class MySinkFunction extends RichSinkFunction[SensorReading] {

  private var jedis: Jedis = _

  override def open(parameters: Configuration): Unit = {
    jedis = new Jedis("192.168.153.135", 6379)

  }

  override def invoke(value: SensorReading, context: SinkFunction.Context): Unit = {
    jedis.hset("sensor", value.id, value.timestamp + "&" + value.temperature)
  }

  override def close(): Unit = {
    jedis.close()
  }

}

class MyRedisMapper extends RedisMapper[SensorReading] {
  override def getCommandDescription: RedisCommandDescription = {

    new RedisCommandDescription(RedisCommand.HSET, "sensor")
  }

  override def getKeyFromData(data: SensorReading): String = {
    data.id
  }

  override def getValueFromData(data: SensorReading): String = {
    data.timestamp + "\t" + data.temperature
  }
}
