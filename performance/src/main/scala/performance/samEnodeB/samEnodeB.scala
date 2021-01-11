package performance.samEnodeB

import java.io.FileNotFoundException
import java.io.IOException



import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.date_format
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions.substring_index
import org.apache.spark.sql.functions.lit

object samEnodeB {
  
 
  class samEnodeBStreaming {
    
    /*
     * Class for reading Samsung LSMR files and write data to HDFS file system.
     * Class Variables :
     * 				-sparksession - Object of SparkSession
     * 				-streamDataFrame - Datafreame object
     * 				-query - StreamingQuery object
     * Class modules :
     * 				-createSparkSession - Function to create spark session, session object stored in class variable sparksession
     * 				-readerStream - Function to read data from csv files once file is available on input folder. Read data is stored in class variable streamDataFrame
     * 				-writerconsole - Function to writes data frame to console.
     * 				-writerToHDFS - Function to writes data frame to HDFS files system
     * 				-queryWait - Query await termination function.
     */
     
    
      //CLASS VARIABLES
      var sparksession: SparkSession = _
      var streamDataFrame: DataFrame = _
      //var streamDataFrameFilterNullColumn: DataFrame = _
      var query: StreamingQuery = _
    
      // CLASS FUNCTIONS
      def createSparkSession(AppName: String, executionMode:String){
      
        /*
         * AppName - Name of application
         * executionMode - spark job execution mode, local:Local desktop or yarn:CLuster mode
         */
        this.sparksession = SparkSession.builder()
                           .appName(AppName)
                           .master(executionMode)
                           .getOrCreate()
      
        this.sparksession.sparkContext.setLogLevel("ERROR")
      }
    
    
      def readerStream(schemaInput: StructType, dir_input: String, datablock: String){
          
          /*
           *schemaInput - Schema of input data
           *dir_input - Input directory from where files are read.
           * datablock - dataclock of input file.
           */
      
          //println(schemaInput)
          //println(dir_input)
          try{
              this.streamDataFrame = this.sparksession
                   .readStream
                   .option("sep", ",")
                   .option("maxFilesPerTrigger",2)
                   .schema(schemaInput)
                   .option("header", "true")
                   .csv(dir_input)
                   .withColumn("fullfilename", input_file_name ) // Add column with filename with its path.
                   .withColumn("datetime", date_format(to_date(col("EVENT_TIME"), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd")) // add date columns use for partitioning
                   .withColumn("filename", substring_index(col("fullfilename"), "/", -1)) // Add from fullfilename columns remove path and add file name.
                   .withColumn("circle", substring_index(col("filename"), "_", 1)) //with file name columns add
                   .drop("fullfilename","filename") // Drop columns 

              // Drop all the columns which start with char '_'
              val streamDataFrameFilterNullColumn  = this.streamDataFrame.columns.filter(_.startsWith("_"))
              println(streamDataFrameFilterNullColumn)
              streamDataFrameFilterNullColumn.foreach(col =>{
                this.streamDataFrame = this.streamDataFrame.drop(col)
              })

           }catch{
              case ex: FileNotFoundException  => { print(ExceptionUtils.getStackTrace(ex)) }
              case ex: IOException => { print(ExceptionUtils.getStackTrace(ex))  }
              case ex: Throwable  => { print(ExceptionUtils.getStackTrace(ex)) }
            }
      }
      
      def writerconsole(dir_checkpoint: String){

        /*
         * dir_checkpoint - Directory where checkpoint data will be stored.
         */
    
          try{
              this.query = this.streamDataFrame.writeStream
                          .format("console")
                          .option("checkpointLocation", dir_checkpoint )
                          .outputMode(OutputMode.Update())
                          .option("truncate", false)
                          .start()
         
          }catch{
                case ex: Throwable  => { print(ExceptionUtils.getStackTrace(ex)) }
             }
      }
      
      def writerToHDFS(dir_checkpoint: String, dir_output: String){

        /*
         * dir_checkpoint - Directory where checkpoint data will be stored.
         * dir_output- Directory where data will be stored
         */
    
          try{
              this.query = this.streamDataFrame.writeStream
                          .format("parquet") // Fileformat csv
                          .partitionBy("DATETIME", "circle")
                          .option("path", dir_output)
                          .option("checkpointLocation", dir_checkpoint + "_parquet" )
                          .outputMode(OutputMode.Append())
                          .start()
         
          }catch{
                case ex: Throwable  => { print(ExceptionUtils.getStackTrace(ex)) }
             }
      }
      
      
      def queryWait(){
        
        this.query.awaitTermination()
        
      }
      


    
    
    
  }
 }