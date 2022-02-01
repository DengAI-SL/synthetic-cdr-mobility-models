package lk.uom.datasearch.sample

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.rand

object SampleUserExtract {

  /*
   * Sample run : ./spark-submit --class lk.uom.datasearch.sample.SampleUserExtract /media/education/0779713087/MSc/home-work-classify/target/scala-2.11/subscriber-home-work-classify-model_2.11-1.4.2.jar /media/education/0779713087/MSc/Data
   * Sample run on server: spark-submit --class lk.uom.datasearch.sample.SampleUserExtract /home/hadoop/data/jobs/subscriber-home-work-classify-model_2.11-1.4.2.jar /SCDR
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SampleUserExtract")
      .config("spark.sql.broadcastTimeout", "36000")
//      .config( vpn  )
      .getOrCreate();

    //    var dataRoot = "/SCDR"
    var dataRoot = "/media/education/0779713087/MSc/Data";

    if (args(0) != null) {
      dataRoot = args(0);
    }
    println("data root : ", dataRoot)

    val dataDirs = List(
//                      "/synv_20130601_20131201"
                    "/generated_voice_records_20121201_20130601",
                    "/generated_voice_records_20130601_20131201"
                    ).map(path => dataRoot+path)
    val locationsCsv = dataRoot + "/1K_CELL_CENTERS_ALL.CSV"
    val outputLocation = dataRoot + "/output/SampleUserForWestern.csv"
    val startDate = "013-11-01";
    val endDate = "2013-11-10";
    val userCount = 20000;

    // Reading parquet data
    var cdrDF = spark.read.parquet(dataDirs:_*)
      .select(
        col("SUBSCRIBER_ID"),
        to_timestamp(col("CALL_TIME"), "yyyyMMddHHmmss").as("CALL_TIMESTAMP"),
        col("INDEX_1KM"))
      .withColumn("WEEK_OF_YEAR", date_format(col("CALL_TIMESTAMP"), "w").cast(IntegerType))
      .withColumn("DATE_OF_YEAR", date_format(col("CALL_TIMESTAMP"), "D").cast(IntegerType))
      .withColumn("HOUR_OF_DAY", date_format(col("CALL_TIMESTAMP"), "H").cast(IntegerType));

    var locationsDF = spark.read.option("header", "true").csv(locationsCsv)
      .select(
      col("LONGITUDE"),
      col("LATITUDE"),
      col("DSD_CODE4"),
      col("PROVINCE_NAME"),
      col("DISTRICT_NAME"),
      col("DS_NAME"),
      col("LOCATION_ID"),
      col("BTS_AREA"));

    var selectedAreaDF = locationsDF.filter(col("PROVINCE_NAME").equalTo("Western"))

    var pickedTimeIntervalDF = cdrDF.filter(col("CALL_TIMESTAMP").between(startDate, endDate))

    var restrictedToArea = pickedTimeIntervalDF.join(selectedAreaDF, selectedAreaDF("LOCATION_ID") === pickedTimeIntervalDF("INDEX_1KM"))

    // Restricting to random n, users
    val allUsers = restrictedToArea.select(col("SUBSCRIBER_ID") as "FILT_SUBSCRIBER_ID").distinct()         // Renaming column to avoid duplication in next step
    val randomlyPickedUsers = allUsers.orderBy(rand()).limit(userCount)

    val cdrForRandomlyPickedUsers = restrictedToArea.join(randomlyPickedUsers,randomlyPickedUsers("FILT_SUBSCRIBER_ID")===restrictedToArea("SUBSCRIBER_ID"))
      .select(
        col("SUBSCRIBER_ID"),
        col("CALL_TIMESTAMP"),
        col("INDEX_1KM"),
        col("LONGITUDE"),
        col("LATITUDE"),
        col("DSD_CODE4"),
        col("PROVINCE_NAME"),
        col("DISTRICT_NAME"),
        col("DS_NAME"),
        col("LOCATION_ID"),
        col("BTS_AREA")
      )


    // Writing Output
    cdrForRandomlyPickedUsers.coalesce(1).write.option("header","true").csv(outputLocation)
  }
}
