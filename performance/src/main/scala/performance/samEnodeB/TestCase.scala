package performance.samEnodeB

object TestCase extends App {
  
  val filename = "file:///C:/nifi_data/output/LSMR10_0930-0945-SINR%20Distribution-7.5.0.csv"
  
  println(filename)
  println(filename.split("/").toList.last.split("_").toList(0))
  
   
  
}