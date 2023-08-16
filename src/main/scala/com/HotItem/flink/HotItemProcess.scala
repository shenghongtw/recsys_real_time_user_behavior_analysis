package com.HotItem.flink

import java.util.Properties
import java.sql.Timestamp
import java.time.Duration
import scala.collection.mutable.ListBuffer
import com.HotItem.flink.HotItemsProcess.{ItemViewCount, UserClickEvent}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector


object HotItemsProcess {
  // 使用者點擊事件
  case class UserClickEvent(userId:Long,itemId:Long,timeStamp:Long)
  // 餐廳瀏覽量
  case class ItemViewCount(itemId:Long,windowEnd:Long,count:Long)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // kafka設定
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    // 創建flinkKafkaConsumer資料源
    val stream = env
      .addSource(new FlinkKafkaConsumer[String](
        "clicks",
        new SimpleStringSchema(),
        properties)
      )

    stream.map(data =>{
      val dataArray: Array[String] = data.split(",")
      // 將資料封裝到UserClickEvent
      UserClickEvent(dataArray(0).trim.toLong,dataArray(1).trim.toLong,dataArray(2).trim.toLong)
      })
      // 設置watermark，允許遲到時間2秒
      .assignTimestampsAndWatermarks(WatermarkStrategy
        .forBoundedOutOfOrderness[UserClickEvent](Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[UserClickEvent] {
          // 指定時間戳
          override def extractTimestamp(t: UserClickEvent, l: Long): Long =
            t.timeStamp
        })

      )
      .keyBy(_.itemId)
      // 新增一個滑動窗口，統計1小時內餐廳點擊次數，5分更新一次
      .window(SlidingEventTimeWindows.of(Time.minutes(60), Time.minutes(5)))
      // 結合增量聚合函數與全窗口函數，輸出每個item在窗口內點擊次數總和
      .aggregate(new ClickCountAgg(),new ClickCountResult())

    // 执行程序
    env.execute("HotItems")


  }
}

// 增量聚合函數，每出現一筆click data就加1
class  ClickCountAgg extends AggregateFunction[UserClickEvent, Long, Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: UserClickEvent, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = ???
}

// 自訂義窗口函數，包装成ItemViewCount输出
class ClickCountResult() extends ProcessWindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def process(key: Long, context: Context, elements: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val count = elements.iterator.next()
    val end = context.window.getEnd

    // 以ItemViewCount形式輸出
    out.collect(ItemViewCount(key, end, count))
  }
}

