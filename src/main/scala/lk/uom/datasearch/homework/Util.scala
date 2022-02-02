package lk.uom.datasearch.homework

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, count, date_format, lit, max, to_timestamp, udf}
import org.apache.spark.sql.types.IntegerType

object Util {

  def readParquetCDR(dataRoot: String, startWeek: Int, endWeek: Int, spark : SparkSession): DataFrame ={
    val dataDirs = List(
      //                      "/synv_20130601_20131201"
      "/generated_voice_records_20121201_20130601",
      "/generated_voice_records_20130601_20131201"
    ).map(path => dataRoot+path)


    // Reading parquet CDR Data
    val cdrDF = spark.read.option("mode", "DROPMALFORMED").format("parquet")
      .schema("SUBSCRIBER_ID String,CALL_TIME String, INDEX_1KM String ").parquet(dataDirs:_*)
      .select(col("SUBSCRIBER_ID"), to_timestamp(col("CALL_TIME"), "yyyyMMddHHmmss").as("CALL_TIMESTAMP"), col("INDEX_1KM"))
      .withColumn("WEEK_OF_YEAR", date_format(col("CALL_TIMESTAMP"), "w").cast(IntegerType))
      .withColumn("DATE_OF_YEAR", date_format(col("CALL_TIMESTAMP"), "D").cast(IntegerType))
      .withColumn("HOUR_OF_DAY", date_format(col("CALL_TIMESTAMP"), "H").cast(IntegerType));

    return cdrDF.filter(col("WEEK_OF_YEAR").between(startWeek, endWeek))
  }

  def calculateTemporalAppearance(cdrDF: DataFrame): DataFrame = {
    /**
     * Filter By time of the day to separate home and work location
     */
    val HWTimeFilter: (Integer) => String = (hourOfDay: Integer) => {

      if (hourOfDay == null) {
        ""
      }
      else {
        if (hourOfDay < 5) {
          "HOME_HOUR"
        }
        else if (hourOfDay >= 21) {
          "HOME_HOUR"
        }
        else {
          "WORK_HOUR"
        }
      }

    }

    // Creating user defined function from filter
    val HWTimeFilterUdf = udf(HWTimeFilter)

    // Applying user defined filter function to whole dataset
    var HWTimeFilterDF = cdrDF.withColumn("HW_TIME_FILTER", HWTimeFilterUdf(col("HOUR_OF_DAY").cast(IntegerType)))

    // Limiting to only one record per day for a certain cell
    var distinctHWTimeFilterDF = HWTimeFilterDF.select(col("SUBSCRIBER_ID"), col("INDEX_1KM"), col("WEEK_OF_YEAR"), col("DATE_OF_YEAR"), col("HW_TIME_FILTER")).distinct()

    return distinctHWTimeFilterDF
  }

  def getLocationFromMaxAppearance(appearanceCountDF: DataFrame, outputDirectory: String): DataFrame ={

    var wholeYearMAXSubsLocationDF = appearanceCountDF.groupBy("SUBSCRIBER_ID")
      .agg(max("APPEARANCE_COUNT").as("MAX_APPEARANCE_COUNT"));

    // Identifying the location which reported max appearance is weak. In a case where there are two locations having
    // same appearance count as max, there will be two locations remained in the joined table.
    // Distinct method is used to filter out such cases, still we  are not sure of which one would be filtered out
    var wholeYearLocationDF = wholeYearMAXSubsLocationDF.select(col("SUBSCRIBER_ID") as "MAX-SUBSCRIBER_ID", col("MAX_APPEARANCE_COUNT") as "MAX_APPEARANCE_COUNT")
      .join(appearanceCountDF, col("MAX-SUBSCRIBER_ID") === col("SUBSCRIBER_ID")
        && col("MAX_APPEARANCE_COUNT") === col("APPEARANCE_COUNT"),
        "inner").select("SUBSCRIBER_ID", "INDEX_1KM", "MAX_APPEARANCE_COUNT").distinct()


    val subscriberLocationByCellIdOutPath = outputDirectory + "/subscriberLocationByCellId.csv"

    wholeYearLocationDF.coalesce(1).write.option("header", "true").csv(subscriberLocationByCellIdOutPath)

    var wholeYearCountInLocation = wholeYearLocationDF.groupBy("INDEX_1KM").agg(count(lit(1)).as("COUNT_IN_1KM_CELL"))

    var wholeYearCountInLocationOutput = outputDirectory + "/locationSubscriberCount.csv"

    wholeYearCountInLocation.select(col("INDEX_1KM") as "LOCATION_ID", col("COUNT_IN_1KM_CELL"))
      .coalesce(1).write.option("header", "true").csv(wholeYearCountInLocationOutput)

    wholeYearLocationDF
  }

  def mergeHomeWork(yearHomeLocation: DataFrame, yearWorkLocation: DataFrame): DataFrame = {
    val homeWorkJoined = yearHomeLocation.select(
      col("SUBSCRIBER_ID") as "HOME_SUBSCRIBER_ID",
      col("INDEX_1KM") as "HOME_INDEX_1KM",
      col("MAX_APPEARANCE_COUNT") as "HOME_APPEARANCE_COUNT")
      .join(yearWorkLocation.select(
        col("SUBSCRIBER_ID") as "WORK_SUBSCRIBER_ID",
        col("INDEX_1KM") as "WORK_INDEX_1KM",
        col("MAX_APPEARANCE_COUNT") as "WORK_APPEARANCE_COUNT"), col("HOME_SUBSCRIBER_ID") === col("WORK_SUBSCRIBER_ID"), "full_outer")

    val SubscriberIDFilter: (Integer, Integer) => Integer = (home_subsciber_id: Integer, work_subscriber_id) => {

      if (home_subsciber_id != null) {
        home_subsciber_id
      }
      else {
        work_subscriber_id
      }

    }

    // Creating user defined function from filter
    val SubscriberIDFilterUdf = udf(SubscriberIDFilter)

    // Applying user defined filter function to whole dataset
    val homeWorkJoinedAndFiltered = homeWorkJoined.withColumn("SUBSCRIBER_ID", SubscriberIDFilterUdf(
      col("HOME_SUBSCRIBER_ID").cast(IntegerType),
      col("WORK_SUBSCRIBER_ID").cast(IntegerType)))
    return homeWorkJoinedAndFiltered
  }
}
