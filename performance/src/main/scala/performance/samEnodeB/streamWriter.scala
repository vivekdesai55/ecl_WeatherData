package performance.samEnodeB

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.OutputMode
import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.streaming.StreamingQuery

object streamWriter {
  
  def writerconsole(stramDataFrame: DataFrame, dir_checkpoint: String): Try[StreamingQuery] = {
    
    try{
      
        val query = stramDataFrame.writeStream
                    .format("console")
                    .option("checkpointLocation", dir_checkpoint )
                    .outputMode(OutputMode.Update())
                    .start()
      
         Success(query)
      
    }catch{
      
      case ex: Throwable  => { print(ExceptionUtils.getStackTrace(ex)); Failure(ex) }
    }
    
    
  }

  
}