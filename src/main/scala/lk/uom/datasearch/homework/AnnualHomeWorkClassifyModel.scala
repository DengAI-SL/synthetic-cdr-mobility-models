package lk.uom.datasearch.homework

import lk.uom.datasearch.homework.Util.{calculateTemporalAppearance, getLocationFromMaxAppearance, readParquetCDR, mergeHomeWork}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object AnnualHomeWorkClassifyModel {
  /*
   * Sample run : ./bin/spark-submit --class lk.uom.datasearch.homework.AnnualHomeWorkClassifyModel /media/education/0779713087/MSc/home-work-classify/target/scala-2.12/subscriber-home-work-classify-model_2.12-1.4.2.jar /media/education/0779713087/MSc/Data 1 0 54 false true
   * Sample run on server: /opt/spark-3.2.1/bin/spark-submit --class lk.uom.datasearch.homework.AnnualHomeWorkClassifyModel /home/hadoop/jobs/subscriber-home-work-classify-model_2.12-1.4.2.jar /SCDR 1 0 54 false true
   */

  var DEBUG = false
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("HomeWorkClassifyModel")
      .getOrCreate();
    spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

    var dataRoot = "/media/education/0779713087/MSc/Data";

    if (args(0) != null) {
      dataRoot = args(0);
    }
    println("data root : ", dataRoot)

    var version = "1";
    if (args(1) != null) {
      version = args(1)
    }
    var startWeek = 0;
    if (args(2) != null){
      startWeek = args(2).toInt
    }
    var endWeek = 53;
    if (args(3) != null){
      endWeek = args(3).toInt
    }

    if (args(4) != null){
      DEBUG = args(4).toBoolean
    }

    var FORCE_ONE_LOCATION = true
    if (args(4) != null){
      FORCE_ONE_LOCATION = args(5).toBoolean
    }


    val homeOutputLocation = dataRoot + "/output/tc/" + version + "/homeYear"
    val workOutputLocation = dataRoot + "/output/tc/" + version + "/workYear"

    val  cdrDF = readParquetCDR(dataRoot, startWeek, endWeek, spark)

    val distinctHWTimeFilterDF = calculateTemporalAppearance(cdrDF)

    val wholeYearAppearanceCountHomeDF = distinctHWTimeFilterDF.filter("HW_TIME_FILTER='HOME_HOUR'")
      .groupBy("SUBSCRIBER_ID", "INDEX_1KM").agg(count(lit(1)).as("APPEARANCE_COUNT"))
    val yearHomeLocation = getLocationFromMaxAppearance(wholeYearAppearanceCountHomeDF, homeOutputLocation, FORCE_ONE_LOCATION)


    val wholeYearAppearanceCountWorkDF = distinctHWTimeFilterDF.filter("HW_TIME_FILTER='WORK_HOUR'")
      .groupBy("SUBSCRIBER_ID", "INDEX_1KM").agg(count(lit(1)).as("APPEARANCE_COUNT"))
    val yearWorkLocation = getLocationFromMaxAppearance(wholeYearAppearanceCountWorkDF, workOutputLocation, FORCE_ONE_LOCATION)

    val homeWorkJoined = mergeHomeWork(yearHomeLocation, yearWorkLocation).select(
      col("SUBSCRIBER_ID"),
      col("HOME_INDEX_1KM"),
      col("HOME_APPEARANCE_COUNT"),
      col("WORK_INDEX_1KM"),
      col("WORK_APPEARANCE_COUNT")
    )

    val joinedHomeWorkPath = dataRoot + "/output/tc/" + version+ "/homeWorkJoined.csv"
    homeWorkJoined.coalesce(1).write.option("header", "true").csv(joinedHomeWorkPath)
  }
}
