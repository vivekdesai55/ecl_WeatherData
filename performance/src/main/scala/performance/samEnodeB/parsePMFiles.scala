package performance.samEnodeB

import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.date_format
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.streaming.OutputMode




object parsePMFiles {
  
 def main(args: Array[String]): Unit = {
   
   
   val spark = SparkSession.builder()
                                .appName("SparkStructuredStreaming")
                                .master("local[*]")
                                .getOrCreate()
   
   val Dir_In = "C:\\nifi_data\\output"
   val Dir_Out = "C:\\stream_output\\checkpoints"
   val Dir_checkpoint = "C:\\stream_output\\checkpoints"
//   
  //val baseNameOfFile: UserDefinedFunction = spark.sqlContest.udf.register((longFilePath: String) => FilenameUtils.getBaseName(longFilePath).split("-").drop(1).drop(1).mkString("_").split(" ").mkString("_"))
//  val lsmrname: UserDefinedFunction = udf((longFilePath: String) => FilenameUtils.getBaseName(longFilePath).split("_")(0))
//   
  println(" Dir_In - " + Dir_In)
  println(" Dir_Out - " + Dir_Out)
  println(" Dir_checkpoint - " + Dir_checkpoint)
  
  //val counterGroupList = List("Average per-OFDMA Symbol RSSI-7.5.0","SINR Distribution-7.5.0")
  val stramDataFrame = spark
                   .readStream
                   .option("sep", ",")
                   .schema(streamSchema.Average_per_OFDMA_Symbol_RSSI_schema)
                   .option("header", "true")
                   .csv(Dir_In + "\\*" + "Average per-OFDMA Symbol RSSI-7.5.0.csv")
//                   .withColumn("OMCNAME", lsmrname(input_file_name) )
                   .withColumn("Date", date_format(to_date(col("EVENT_TIME"), "yyyy-MM-dd HH:mm:ss"), "yyyyMMdd"))
  
   val query = stramDataFrame.writeStream
                             .format("console")
                             .option("checkpointLocation", Dir_checkpoint )
                             .outputMode(OutputMode.Update())
                             .start()

   query.awaitTermination()
   
 }

  
  
  
}