package performance.samEnodeB

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row
import java.io.PrintWriter
import java.io.File



object TestAggregation extends App {

  
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
  val groupCols = Seq("SINR_Distribution","NE_VERSION","NE_ID","NE_NAME","cNum","EarfcnDl","datetime","circle")

 
  
  import sparksession.sqlContext.implicits._
   
  val cols = groupCols.map(col) ++ Seq(window($"EVENT_TIME", "1 hour", "1 hour", "-30 minutes" ))
  var aggDFrame = dframe.withWatermark("EVENT_TIME", "10 minutes")
                        .groupBy(cols: _*).agg(Rule_Agg)
 
  aggDFrame.printSchema() 
  
  
  //aggDFrame = aggDFrame.select($"window.start".as("Start_Time"), $"window.end".as("End_Time"))
  //aggDFrame = aggDFrame.withColumn("Start_Time", aggDFrame.select($"window.start"))
  
  aggDFrame = aggDFrame.columns
                        .foldLeft(aggDFrame){
                                (newdf, colname) =>
                                        newdf.withColumnRenamed(colname, colname.replace("sum", "")
                                                                                .replace("min", "")
                                                                                .replace("max", "")
                                                                                .replace("avg", "")
                                                                                .replace("(", "")
                                                                                .replace("(", "")
                                                                                .replace("window","EVENT_TIME")
                                                                              
                               
                        )}
                        //.select($"EVENT_TIME._1".as("Start_Time"), $"EVENT_TIME._1".as("End_Time"))
                        //.withColumn("EVENT_TIME", col("EVENT_TIME").cast("string"))
                        .withColumn("EVENT_TIME", col("EVENT_TIME").cast("string"))
                        
   aggDFrame.printSchema()               
   
  val query = aggDFrame.writeStream
                        .format("console")
                        .option("checkpointLocation", "C:\\nifi_data\\agg_checkpoint_AGG" )
                        .outputMode(OutputMode.Update())
                        .option("truncate", false)
                        .start()
 
//    val query1 = aggDFrame.writeStream
//                        .format("csv")
//                        .option("path", "C:\\nifi_data\\agg_csv_files")
//                        .option("checkpointLocation", "C:\\nifi_data\\agg_checkpoint_AGG_WRITE" )
//                        .outputMode("update")
//                        .start()
 
                        
//  val writerForText = new ForeachWriter[Row]{
//                          
//                          val writer = new PrintWriter(new File("C:\\nifi_data\\agg_csv_files\\Aggregated.csv"))
//                          
//                          def open(partitionId: Long, version:Long): Boolean ={
//                            true
//                          }
//                          
//                          def process(record: Row): Unit ={
//                            
//                            println(record)
//                            
//                            writer.append(record.toString())
//                            
//                            
//                          }
//                          
//                          def close(errorOrNull: Throwable): Unit ={
//                            
//                            writer.close()
//                            
//                          }
//                          
//                          
//                          
//                          
//                        }
//  val query1 = aggDFrame.writeStream
//                        .format("csv")
//                        .option("checkpointLocation", "C:\\nifi_data\\agg_checkpoint_AGG_WRITE" )
//                        .outputMode(OutputMode.Update())
//                        .foreach(writerForText)
//                        .start()
//                        
    
    
  query.awaitTermination()
      
  //.awaitTermination()
  
}