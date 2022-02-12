package lk.uom.datasearch.mobility

import lk.uom.datasearch.homework.Util.readParquetCDR
import org.apache.spark.sql.functions.{col, count, lit, max, udf}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

object SimMobility {
  /*
 * Argument order is : dataRoot version year dateOfYear
 *  Sample run : ./bin/spark-submit --class lk.uom.datasearch.mobility.SimMobility /media/education/0779713087/MSc/synthetic-cdr-mobility-models/target/scala-2.12/synthetic-cdr-mobility-models_2.12-1.4.2.jar /media/education/0779713087/MSc/Data 1 2013 317
 *
 *  Sample run on server: /opt/spark-3.2.1/bin/spark-submit --class lk.uom.datasearch.mobility.SimMobility /home/hadoop/jobs/synthetic-cdr-mobility-models_2.12-1.4.2.jar /SCDR 1 2013 317
 */

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SimMobility")
      .getOrCreate();

    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

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

    var year = "2013"
    if (args(2) != null) {
      year = args(2)
    }
    var dateOfYear = "1";
    if (args(3) != null) {
      dateOfYear = args(3)
    }
    val outputPath = dataRoot + "/output/SimMobility/" + version +"/" + dateOfYear


    val cdrDF: DataFrame = readParquetCDR(dataRoot, spark)

    val subscriber_hourly_count_in_cell = cdrDF.filter(col("YEAR").equalTo(year) && col("DATE_OF_YEAR")
      .equalTo(dateOfYear)).groupBy("SUBSCRIBER_ID", "HOUR_OF_DAY", "INDEX_1KM")
      .agg(count(lit(1)).as("COUNT_IN_1KM_CELL"))
    val max_appeared_count_in_cell = subscriber_hourly_count_in_cell
      .groupBy("SUBSCRIBER_ID", "HOUR_OF_DAY")
      .agg(max("COUNT_IN_1KM_CELL").as("MAX_APPEARANCE_COUNT"))
      .select(col("SUBSCRIBER_ID") as "M_SUBSCRIBER_ID", col("HOUR_OF_DAY") as "M_HOUR_OF_DAY", col("MAX_APPEARANCE_COUNT"))

    val max_appeared_cell = max_appeared_count_in_cell.join(subscriber_hourly_count_in_cell,
      col("M_SUBSCRIBER_ID") === col("SUBSCRIBER_ID") && col("M_HOUR_OF_DAY") === col("HOUR_OF_DAY"), "inner")
      .select("SUBSCRIBER_ID", "INDEX_1KM", "HOUR_OF_DAY")
      .dropDuplicates("SUBSCRIBER_ID","HOUR_OF_DAY")

    val hours = Seq("0","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23")
    val pivoted_sub_hour =  max_appeared_cell.groupBy("SUBSCRIBER_ID").pivot("HOUR_OF_DAY", hours)
      .agg(max("INDEX_1KM"))

    pivoted_sub_hour.coalesce(1).write.option("header", "true").csv(outputPath)
  }
}
