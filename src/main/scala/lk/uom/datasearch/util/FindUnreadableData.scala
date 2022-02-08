package lk.uom.datasearch.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import lk.uom.datasearch.homework.Util.cdr_schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.io.ParquetDecodingException
import org.apache.spark.sql.functions.{col, length}

import scala.collection.mutable.ListBuffer

object FindUnreadableData {

  /*
  * Sample run : ./bin/spark-submit --class lk.uom.datasearch.util.FindUnreadableData /media/education/0779713087/MSc/home-work-classify/target/scala-2.12/synthetic-cdr-mobility-models_2.12-1.4.2.jar /media/education/0779713087/MSc/Data 1
  * Sample run on server: /opt/spark-3.2.1/bin/spark-submit --class lk.uom.datasearch.util.FindUnreadableData /home/hadoop/jobs/synthetic-cdr-mobility-models_2.12-1.4.2.jar /SCDR 1
  */


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("FindUnreadableData")
      .getOrCreate();
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    import spark.implicits._

    //    val dataRoot = "/SCDR"
    var dataRoot = "/media/education/0779713087/MSc/Data";

    if (args(0) != null) {
      dataRoot = args(0);
    }
    println("data root : ", dataRoot)

    var version = "1";
    if (args(1) != null) {
      version = args(1)
    }
    val failedFilesOutPath = dataRoot+"/output/"+version+"/decodingFailedFiles.csv"

    // parallel Lists for parallel execution of the task
    val dataDirs = List(
      //                      "/synv_20130601_20131201"
      "/generated_voice_records_20121201_20130601",
      "/generated_voice_records_20130601_20131201"
    ).map(path => dataRoot + path)

    val fs = FileSystem.get(new Configuration())
    val failedFiles = new ListBuffer[String]()
    dataDirs.par.foreach(dir => {
      val status = fs.listStatus(new Path(dir)).par
      status.par.foreach(x => {
        val filePath = x.getPath.toString
        if (filePath.endsWith(".parquet")){
          print(filePath)
          try {
            val cdrDF = spark.read.option("mode", "DROPMALFORMED").format("parquet")
              .parquet(filePath)
            // This is a simple query to trigger the ParquetDecoding Exception due to long lengths of SUBSCRIBER_ID field
            cdrDF.filter(length(col("SUBSCRIBER_ID"))>10).show()
          }
          catch {
            case ex: Exception =>{
              failedFiles += filePath
              print("Failed File path: %s", filePath)
            }
          }

        }
      })
    }
    )
    val failed_files_schema: StructType = StructType(Array(
      StructField("FAILED_FILE", StringType, false),
    ))
    val failedFilesDF = failedFiles.toDF("FAILED_FILES")
    failedFilesDF.coalesce(1).write.option("header", "true").csv(failedFilesOutPath)


  }
  }
