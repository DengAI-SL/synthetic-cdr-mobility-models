package lk.uom.datasearch.homework

import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import lk.uom.datasearch.homework.Util.{calculateTemporalAppearance, getLocationFromMaxAppearance, mergeHomeWork, readParquetCDR}

object AnnualHomeWorkClassifyWithSpatialCertainty {
  /*
   * Sample run : ./bin/spark-submit --class lk.uom.datasearch.homework.AnnualHomeWorkClassifyWithSpatialCertainty /media/education/0779713087/MSc/synthetic-cdr-mobility-models/target/scala-2.12/synthetic-cdr-mobility-models_2.12-1.4.2.jar /media/education/0779713087/MSc/Data 1 0 54 false true38*10
   *
   * Sample run on server: /opt/spark-3.2.1/bin/spark-submit --class lk.uom.datasearch.homework.AnnualHomeWorkClassifyWithSpatialCertainty /home/hadoop/jobs/synthetic-cdr-mobility-models_2.12-1.4.2.jar /SCDR 1 0 54 false true
   */

  val adj_schema: StructType = StructType(Array(
    StructField("ADJ_INDEX_1KM", StringType, false),
    StructField("ADJ_1", StringType, true),
    StructField("ADJ_2", StringType, true),
    StructField("ADJ_3", StringType, true),
    StructField("ADJ_4", StringType, true)
  ))

  var DEBUG = false;

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AnnualHomeWorkClassifyModelWIthSpatialCertainty")
      .getOrCreate();
    spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

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

    val homeOutputLocation = dataRoot + "/output/sc/" + version + "/homeYear"
    val workOutputLocation = dataRoot + "/output/sc/" + version + "/workYear"

    val adjacent_map_file = dataRoot + "/4_adjacent_cells.txt"

    val  cdrDF = readParquetCDR(dataRoot, startWeek, endWeek, spark)

    val distinctHWTimeFilterDF = calculateTemporalAppearance(cdrDF)
    val adjacent_map = spark.read.format("csv").schema(adj_schema).csv(adjacent_map_file)


    val wholeYearAppearanceCountHomeDF = distinctHWTimeFilterDF.filter("HW_TIME_FILTER='HOME_HOUR'")
      .groupBy("SUBSCRIBER_ID", "INDEX_1KM").agg(count(lit(1)).as("APPEARANCE_COUNT"))

    val adjHomeAppearanceComputed = calculateSpatialAppearance(wholeYearAppearanceCountHomeDF, adjacent_map, homeOutputLocation)
    val yearHomeLocation = getLocationFromMaxAppearance(adjHomeAppearanceComputed, homeOutputLocation, FORCE_ONE_LOCATION)


    val wholeYearAppearanceCountWorkDF = distinctHWTimeFilterDF.filter("HW_TIME_FILTER='WORK_HOUR'")
      .groupBy("SUBSCRIBER_ID", "INDEX_1KM").agg(count(lit(1)).as("APPEARANCE_COUNT"))

    val adjWorkAppearanceComputed = calculateSpatialAppearance(wholeYearAppearanceCountWorkDF, adjacent_map, workOutputLocation)
    val yearWorkLocation = getLocationFromMaxAppearance(adjWorkAppearanceComputed, workOutputLocation, FORCE_ONE_LOCATION)

    val homeWorkJoined = mergeHomeWork(yearHomeLocation, yearWorkLocation).select(
      col("SUBSCRIBER_ID"),
      col("HOME_INDEX_1KM"),
      col("HOME_APPEARANCE_COUNT"),
      col("WORK_INDEX_1KM"),
      col("WORK_APPEARANCE_COUNT")
    )

    val joinedHomeWorkPath = dataRoot + "/output/sc/" + version+ "/homeWorkJoined.csv"
    homeWorkJoined.coalesce(1).write.option("header", "true").csv(joinedHomeWorkPath)
  }

  def calculateSpatialAppearance(wholeYearAppearanceCountDF: DataFrame, adjacent_map: DataFrame, outputLocation: String): DataFrame = {
    /**
     * Calculate the Spatial Adjacency appearance of subscribers in Cells
     */

    // Prepare the spatial adjacency joined subscriber table
    var appearanceJoinedAdjacentCell = wholeYearAppearanceCountDF.select(
      col("INDEX_1KM") as "INDEX_1KM",
      col("SUBSCRIBER_ID") as "SUBSCRIBER_ID",
      col("APPEARANCE_COUNT") as "APPEARANCE_COUNT")
      .join(adjacent_map, col("INDEX_1KM") === col("ADJ_INDEX_1KM"), "leftouter")
      .select(
        col("INDEX_1KM") as "INDEX_1KM",
        col("SUBSCRIBER_ID") as "SUBSCRIBER_ID",
        col("APPEARANCE_COUNT") as "APPEARANCE_COUNT",
        col("ADJ_1") as "ADJ_1",
        col("ADJ_2") as "ADJ_2",
        col("ADJ_3") as "ADJ_3",
        col("ADJ_4") as "ADJ_4"
      );
    // Debug
    if (DEBUG){
      appearanceJoinedAdjacentCell.write.option("header", "true").csv(outputLocation+"/appearanceJoinedAdjacentCell")
    }

    // Join the count table again with adj_1 table to get the appearance count in adj_1 area
    val adj_1 = appearanceJoinedAdjacentCell
      .join(
        wholeYearAppearanceCountDF.select(
          col("INDEX_1KM") as "ADJ_INDEX_1KM",
          col("SUBSCRIBER_ID") as "ADJ_SUBSCRIBER_ID",
          col("APPEARANCE_COUNT") as "ADJ_APPEARANCE_COUNT"
        ),
        col("ADJ_1") === col("ADJ_INDEX_1KM")
          &&
          col("SUBSCRIBER_ID") === col("ADJ_SUBSCRIBER_ID"), "leftouter"
      )
      .select(
        col("INDEX_1KM") as "INDEX_1KM",
        col("SUBSCRIBER_ID") as "SUBSCRIBER_ID",
        col("APPEARANCE_COUNT") as "APPEARANCE_COUNT",
        // appearance count in ADJ_1 area for subscriber is put to ADJ_1_APPEARANCE_COUNT
        col("ADJ_APPEARANCE_COUNT") as "ADJ_1_APPEARANCE_COUNT",
        col("ADJ_2") as "ADJ_2",
        col("ADJ_3") as "ADJ_3",
        col("ADJ_4") as "ADJ_4"
      )

    if (DEBUG) {
      adj_1.write.option("header", "true").csv(outputLocation + "/adj_1")
    }
    /// join with existing df 4 times for for adjescent columns
    val adj_2 = adj_1.join(
      wholeYearAppearanceCountDF.select(
        col("INDEX_1KM") as "ADJ_INDEX_1KM",
        col("SUBSCRIBER_ID") as "ADJ_SUBSCRIBER_ID",
        col("APPEARANCE_COUNT") as "ADJ_APPEARANCE_COUNT"
      ),
      col("ADJ_2") === col("ADJ_INDEX_1KM")
        &&
        col("SUBSCRIBER_ID") === col("ADJ_SUBSCRIBER_ID"), "leftouter"
    )
      .select(
        col("INDEX_1KM") as "INDEX_1KM",
        col("SUBSCRIBER_ID") as "SUBSCRIBER_ID",
        col("APPEARANCE_COUNT") as "APPEARANCE_COUNT",
        col("ADJ_1_APPEARANCE_COUNT") as "ADJ_1_APPEARANCE_COUNT",
        col("ADJ_APPEARANCE_COUNT") as "ADJ_2_APPEARANCE_COUNT",
        col("ADJ_3") as "ADJ_3",
        col("ADJ_4") as "ADJ_4"
      )
    // Debug
    if (DEBUG) {
      adj_2.write.option("header", "true").csv(outputLocation + "/adj_2")
    }
    val adj_3 = adj_2.join(
      wholeYearAppearanceCountDF.select(
        col("INDEX_1KM") as "ADJ_INDEX_1KM",
        col("SUBSCRIBER_ID") as "ADJ_SUBSCRIBER_ID",
        col("APPEARANCE_COUNT") as "ADJ_APPEARANCE_COUNT"
      ),
      col("ADJ_3") === col("ADJ_INDEX_1KM")
        &&
        col("SUBSCRIBER_ID") === col("ADJ_SUBSCRIBER_ID"), "leftouter"
    )
      .select(
        col("INDEX_1KM") as "INDEX_1KM",
        col("SUBSCRIBER_ID") as "SUBSCRIBER_ID",
        col("APPEARANCE_COUNT") as "APPEARANCE_COUNT",
        col("ADJ_1_APPEARANCE_COUNT") as "ADJ_1_APPEARANCE_COUNT",
        col("ADJ_2_APPEARANCE_COUNT") as "ADJ_2_APPEARANCE_COUNT",
        col("ADJ_APPEARANCE_COUNT") as "ADJ_3_APPEARANCE_COUNT",
        col("ADJ_4") as "ADJ_4"
      )

    // Debug
    if (DEBUG) {
      adj_3.write.option("header", "true").csv(outputLocation + "/adj_3")
    }
    val adj_4 = adj_3.join(
      wholeYearAppearanceCountDF.select(
        col("INDEX_1KM") as "ADJ_INDEX_1KM",
        col("SUBSCRIBER_ID") as "ADJ_SUBSCRIBER_ID",
        col("APPEARANCE_COUNT") as "ADJ_APPEARANCE_COUNT"
      ),
      col("ADJ_4") === col("ADJ_INDEX_1KM")
        &&
        col("SUBSCRIBER_ID") === col("ADJ_SUBSCRIBER_ID"), "leftouter"
    )
      .select(
        col("INDEX_1KM") as "INDEX_1KM",
        col("SUBSCRIBER_ID") as "SUBSCRIBER_ID",
        col("APPEARANCE_COUNT") as "APPEARANCE_COUNT",
        col("ADJ_1_APPEARANCE_COUNT") as "ADJ_1_APPEARANCE_COUNT",
        col("ADJ_2_APPEARANCE_COUNT") as "ADJ_2_APPEARANCE_COUNT",
        col("ADJ_3_APPEARANCE_COUNT") as "ADJ_3_APPEARANCE_COUNT",
        col("ADJ_APPEARANCE_COUNT") as "ADJ_4_APPEARANCE_COUNT"
      )

    // Debug
    if (DEBUG) {
      adj_4.write.option("header", "true").csv(outputLocation + "/adj_4")
    }
    val na_filled = adj_4.na.fill(0)

    // Debug
    if (DEBUG){
      na_filled.write.option("header", "true").csv(outputLocation+"/na_filled")
    }

    val adjAppearanceComputed = na_filled.withColumn("ADJ_APPEARANCE",
        col("ADJ_1_APPEARANCE_COUNT")
        + col("ADJ_2_APPEARANCE_COUNT")
        + col("ADJ_3_APPEARANCE_COUNT")
        + col("ADJ_4_APPEARANCE_COUNT"))

    // To avoid the appearance of adjacency nodes take over the appearance on center node, will compute the
    // TOTAL_APPEARANCE taking half of the ADJ_APPEARANCE into account. A better approach would have been to compute a inverse distance matrix,
    // unless the voronoi cells are mostly uniform in size around the same area
    val adjLocationAppearanceComputed = adjAppearanceComputed.withColumn("TOTAL_APPEARANCE",
      col("APPEARANCE_COUNT") + col("ADJ_APPEARANCE")/2).drop(col("APPEARANCE_COUNT"))
      .withColumnRenamed("TOTAL_APPEARANCE", "APPEARANCE_COUNT")
    return adjLocationAppearanceComputed
  }
}
