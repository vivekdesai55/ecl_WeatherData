package performance.samEnodeB

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.OutputMode

object TestAggregation2 extends App {
  
  val sparksession = SparkSession.builder()
                           .appName("AppName")
                           .config("spark.sql.streaming.schemaInference","true")
                           .master("local[*]")
                           .getOrCreate()
  this.sparksession.sparkContext.setLogLevel("ERROR")
                           
  var dframe = sparksession.readStream
                           .option("schemaInference", "true")
                           .parquet("C:\\stream_output\\samEnodeB\\quater_hour\\SINR_Distribution")
                        
   
  dframe.printSchema()
  
  val datablock = "SINR_Distribution"
  val colList = AggregationRules.colDict(datablock)._1
  val Rule_Agg = AggregationRules.colDict(datablock)._2
  
   
  val result2 = dframe.groupBy(colList.head, colList.tail: _*).agg(Rule_Agg)
  
  val query = result2.writeStream
                        .format("console")
                        .option("checkpointLocation", "C:\\nifi_data\\agg_checkpoint_AGG" )
                        .outputMode(OutputMode.Update())
                        .option("truncate", false)
                        .start()
 
  query.awaitTermination()
  
}