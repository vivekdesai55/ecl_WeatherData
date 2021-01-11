package weatherDataProducer

object produceWeatherData extends App {
  //API URL TO PULL WEATHER DATA
  val url = "https://api.openweathermap.org/data/2.5/onecall?lat=12.9716&lon=77.5946&exclude=minutely,hourly,daily,alerts&appid=7f9e75dbfba0b07fe2e4e79fc4457342&units=metrics"
  //Data which needs to extracted from the json output (API output)
  val dataList: List[String] =  List("lat","lon","timezone","dt","sunrise","sunset","temp","humidity","dew_point","visibility","wind_speed")
  
  //Pull data for every 10 seconds
  val timeMS: Int = 10000
  
  //Kafka confiuration details
  val server: String = "localhost"
  val port: Int = 9092
  val topic: String = "WeatherData"

  val produceData: weatherDataProducer = new weatherDataProducer(url, dataList)
    val Flag: Boolean = true
    while (Flag) {
      produceData.writeToKafka(topic, server, port)
      Thread.sleep(timeMS)
    } 
}

