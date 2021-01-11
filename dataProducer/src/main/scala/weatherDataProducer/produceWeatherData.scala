package weatherDataProducer

object produceWeatherData extends App {

//  val produceData: weatherDataProcessing = new weatherDataProcessing()
//  val Flag: Boolean = true
//  while (Flag) {
//    produceData.writeToKafka("WeatherData", "localhost", 9092)
//    Thread.sleep(10000)
//  }
  val produceData: weatherDataProducer = new weatherDataProducer()
  
    val Flag: Boolean = true
    while (Flag) {
      produceData.writeToKafka("WeatherData", "localhost", 9092)
      Thread.sleep(10000)
    }
  
}

