package performance.samEnodeB

import java.io.FileNotFoundException
import java.io.IOException

import scala.util.{Failure, Success, Try}


import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.date_format
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StructType

object streamReader {
  
//  val baseNameOfFile: UserDefinedFunction = udf((longFilePath: String) => FilenameUtils.getBaseName(longFilePath).split("-").drop(1).drop(1).mkString("_").split(" ").mkString("_"))
//  val lsmrname: UserDefinedFunction = udf((longFilePath: String) => FilenameUtils.getBaseName(longFilePath).split("_")(0))
//   
  
  def reader(spark: SparkSession, datablock: StructType, dir_input: String): Try[DataFrame] = {
    
  try{
    val stramDataFrame: DataFrame = spark
        .readStream
        .option("sep", ",")
        .schema(datablock)
        .option("header", "true")
        .csv(dir_input)
//        .withColumn("OMCNAME", lsmrname(input_file_name) )
        .withColumn("Date", date_format(to_date(col("EVENT_TIME"), "yyyy-MM-dd HH:mm:ss"), "yyyyMMdd"))
        
     Success(stramDataFrame)
     
  }catch{
    case ex: FileNotFoundException  => { print(ExceptionUtils.getStackTrace(ex)); Failure(ex)  }
    case ex: IOException => { print(ExceptionUtils.getStackTrace(ex));Failure(ex)  }
    case ex: Throwable  => { print(ExceptionUtils.getStackTrace(ex));Failure(ex)  }
    
  }
   
    
  }
  
  
}