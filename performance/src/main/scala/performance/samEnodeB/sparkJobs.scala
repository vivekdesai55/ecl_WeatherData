package performance.samEnodeB

import scala.reflect.io.Path
import java.nio.file.Paths
import org.apache.spark.sql.SparkSession
import scala.util.{Failure, Success, Try}


object sparkJobs{
  

  def main(args: Array[String]): Unit = {
   val dir_in: Path = "C:\\nifi_data\\output"
   val dir_out: Path = "C:\\stream_output\\checkpoints"
   val dir_checkpoint: Path = "C:\\stream_output\\checkpoints"
   
   val datablock = "Average per-OFDMA Symbol RSSI-7.5.0"
   
   val dir_input = dir_in + "\\*" + datablock + ".csv"
//   val dir_checkpoint_datablock = dir_checkpoint + datablock.split(" ").mkString("_")
   
//   val dir_input = Paths.get(dir_in.toString(), datablock).toString()
   val dir_checkpoint_datablock = Paths.get(dir_checkpoint.toString(), datablock.split(" ").mkString("_"))
   
   
   
   println(dir_in)
   println(dir_out)
   println(dir_checkpoint)
   println(datablock)
   println(dir_input)
   println(dir_checkpoint_datablock)
   
   val spark = SparkSession.builder()
                           .appName("SparkStructuredStreaming")
                           .master("local[*]")
                           .getOrCreate()
   
   
  val obj1 = new streamReaderWriter()
  val schemaInput = streamSchema.Average_per_OFDMA_Symbol_RSSI_schema
  
  obj1.reader(spark, schemaInput, dir_input) match {
     case Success(readDataFrame) => {
       
       obj1.writerconsole(readDataFrame, dir_checkpoint_datablock.toString()) match{
         
         case Success(query) =>{
           query.awaitTermination()
         }
         case Failure(ex) =>{
           println(ex)
         }
       }
       
      
       
     }
     case Failure(ex) => {
       println(ex)
     }
   }
   
   
  
  
  }
  
  
}