package lk.uom.datasearch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object ParquetWeeklyHomeWorkClassifyModel {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("HomeWorkClassifyModel")
      .getOrCreate()

    //    val dataRoot = "/home/gayan/Downloads/Compressed/spark-2.4.6-bin-hadoop2.7/DATA";

    val dataRoot = "/SCDR"
    val dataDir = dataRoot + "/synv_20130601_20131201"

    val homeOutputLocation = dataRoot + "/output/HomeYearOutput.csv"
    val workOutputLocation = dataRoot + "/output/WorkYearOutput.csv"


    var cdrDF = spark.read.parquet(dataDir)
      .select(col("SUBSCRIBER_ID"), to_timestamp(col("CALL_TIME"), "yyyyMMddHHmmss").as("CALL_TIMESTAMP"), col("INDEX_1KM"))
      .withColumn("WEEK_OF_YEAR", date_format(col("CALL_TIMESTAMP"), "w").cast(IntegerType))
      .withColumn("DATE_OF_YEAR", date_format(col("CALL_TIMESTAMP"), "D").cast(IntegerType))
      .withColumn("HOUR_OF_DAY", date_format(col("CALL_TIMESTAMP"), "H").cast(IntegerType));


    val HWTimeFilter: (Integer) => String = (hourOfDat: Integer) => {

      if(hourOfDat == null){
        ""
      }
      else{
        if (hourOfDat < 5) {
          "HOME_HOUR"
        }
        else if (hourOfDat >= 21) {
          "HOME_HOUR"
        }
        else {
          "WORK_HOUR"
        }
      }

    }

    val HWTimeFilterUdf = udf(HWTimeFilter)

    var HWTimeFilterDF = cdrDF.withColumn("HW_TIME_FILTER", HWTimeFilterUdf(col("HOUR_OF_DAY").cast(IntegerType)))

    var distinctHWTimeFilterDF = HWTimeFilterDF.select(col("SUBSCRIBER_ID"), col("INDEX_1KM"), col("WEEK_OF_YEAR"), col("DATE_OF_YEAR"), col("HW_TIME_FILTER")).distinct()

    var wholeYearHomeDF = distinctHWTimeFilterDF.filter("HW_TIME_FILTER='HOME_HOUR'")
      .groupBy("SUBSCRIBER_ID", "INDEX_1KM").agg(count(lit(1)).as("APPEARENCE_COUNT"))
      .groupBy("SUBSCRIBER_ID", "INDEX_1KM").agg(max("APPEARENCE_COUNT").as("MAX_APEARENCE_COUNT"));

    var wholeYearWorkDF = distinctHWTimeFilterDF.filter("HW_TIME_FILTER='WORK_HOUR'")
      .groupBy("SUBSCRIBER_ID", "INDEX_1KM").agg(count(lit(1)).as("APPEARENCE_COUNT"))
      .groupBy("SUBSCRIBER_ID", "INDEX_1KM").agg(max("APPEARENCE_COUNT").as("MAX_APEARENCE_COUNT"));

    wholeYearHomeDF.coalesce(1).write.option("header", "true").csv(homeOutputLocation)
    wholeYearWorkDF.coalesce(1).write.option("header", "true").csv(workOutputLocation)

    var wholeYearCountHome = wholeYearHomeDF.groupBy("INDEX_1KM").agg(count(lit(1)).as("COUNT_IN_1KM_CELL"))
    var wholeYearCountWork = wholeYearHomeDF.groupBy("INDEX_1KM").agg(count(lit(1)).as("COUNT_IN_1KM_CELL"))

    var wholeYearCountHomeOutLocation = dataRoot + "/output/SumHome.csv"
    var wholeYearCountWorkOutLocation = dataRoot + "/output/SumWork.csv"

    wholeYearCountHome.coalesce(1).write.option("header", "true").csv(wholeYearCountHomeOutLocation)
    wholeYearCountWork.coalesce(1).write.option("header", "true").csv(wholeYearCountWorkOutLocation)


    var cell_centers = dataRoot + "/1KM_CELL_CENTRES.csv"
    var homeJoinedOutput = dataRoot + "/output/homeRecidentCount.csv"
    var workJoinedOutput = dataRoot + "/output/workRecidentCount.csv"

    var cellCenterDf = spark.read.option("header", "true").csv(cell_centers)

//    var homeLocationDf = spark.read.option("header", "true").csv(wholeYearCountHomeOutLocation)
//    var workLocationDf = spark.read.option("header", "true").csv(wholeYearCountWorkOutLocation)
      var homeLocationDf = wholeYearCountHome;
      var workLocationDf = wholeYearCountWork;

    var homeJoined = homeLocationDf.join(cellCenterDf, homeLocationDf("INDEX_1KM") === cellCenterDf("LOCATION_ID"), "inner").select("LOCATION_ID", "LATITUDE", "LONGITUDE", "COUNT_IN_1KM_CELL")
    var workJoined = workLocationDf.join(cellCenterDf, workLocationDf("INDEX_1KM") === cellCenterDf("LOCATION_ID"), "inner").select("LOCATION_ID", "LATITUDE", "LONGITUDE", "COUNT_IN_1KM_CELL")

    homeJoined.coalesce(1).write.option("header", "true").csv(homeJoinedOutput)
    workJoined.coalesce(1).write.option("header", "true").csv(workJoinedOutput)

    // var wholeYearHWDF = distinctHWTimeFilterDF.groupBy("SUBSCRIBER_ID","INDEX_1KM").agg(count().as("YEAR_TOWER_DAYS_COUNT")).groupBy("SUBSCRIBER_ID").agg(max("YEAR_TOWER_DAYS_COUNT").as("MAX_YTD_COUNT"));
  }
}
