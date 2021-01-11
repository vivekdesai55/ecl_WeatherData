package weatherDataProducer

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties
import scala.io.Source.fromURL

class weatherDataProducer {
  //API URL to Get weather data
  val url = "https://api.openweathermap.org/data/2.5/onecall?lat=12.9716&lon=77.5946&exclude=minutely,hourly,daily,alerts&appid=7f9e75dbfba0b07fe2e4e79fc4457342&units=metrics"
  //Data which needs to extracted from the json output (API output)
  val dataList: List[String] =  List("lat","lon","timezone","dt","sunrise","sunset","temp","humidity","dew_point","visibility","wind_speed")
  
  //Function to Featch data using URL -
  // Takes no parameters and return JSON data as string format
  def getAPIData(): String = fromURL(url).mkString
  // Filter required data set from the provided data list
  // Takes string and return true if present in dataList otherwise false
  def isin(item: String): Boolean = dataList.foldLeft(false)((r, c) => c.equals(item.split(":")(0).replace("\"","")) || r)
  //Function to format API output JSON data
  // takes JSON as string and return JSON as out[ut
  def formatJSONData(jsonStr: String): String = {

    val start = """{"""
    val end = """}"""
    var json = jsonStr.mkString
      .replace("{", "")
      .replace("}", "")
      .replace("]", "")
      .replace("[", "")
      .replace("\"current\":", "")
      .replace("\"weather\":", "")
      .split(",")
      .toList
    json.filter(isin).mkString(start, ",", end)
  }
  // Function to write JSON data to Kafka Topic
  def writeToKafka(topic: String, serverName: String, port: Int): Unit = {
    //Define properties required by kafka
    val props = new Properties()
    props.put("bootstrap.servers", serverName.concat(":").concat(port.toString)) // localhost:9092
    props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
    //Create a KafkaProducer which will have (Key, Value) pair
    val producer:  KafkaProducer[String, String] = new KafkaProducer[String, String](props)
    //Create record of (Key, Value) pair, which placed in configured kafka topics
    println(formatJSONData(getAPIData()))
    val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topic,"dataJSON", formatJSONData(getAPIData()))
    //Send record
    producer.send(record)
    //close KafkaProducer session
    producer.close()
  }

  
}