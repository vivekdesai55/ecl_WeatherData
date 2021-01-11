package dataPipeline

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

object consumeWeatherData extends App {

  val consumeData: weatherDataProcessing = new weatherDataProcessing()

  val spark = consumeData.createSparkSession("WeatherData", "local[*]")
  spark.sparkContext.setLogLevel("ERROR")
  var frame: DataFrame = consumeData.readStramDataFromkafka(spark, "kafka",
    "localhost", 9092, "WeatherData")

  frame = consumeData.convertJsonDataToTabular(frame, spark)
  frame.printSchema()

  val query: StreamingQuery = consumeData.writeStreamDataToConsole(frame)
  val query1: StreamingQuery = consumeData.writeStreamDataToParquet(frame)

  query.awaitTermination()
  query1.awaitTermination()

}

