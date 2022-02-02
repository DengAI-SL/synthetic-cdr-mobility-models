package lk.uom.datasearch.homework

import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import lk.uom.datasearch.homework.Util.{readParquetCDR, getLocationFromMaxAppearance, calculateTemporalAppearance, mergeHomeWork}

object AnnualHomeWorkClassifyWithSpatialCertainty {
  /*
   * Sample run : ./bin/spark-submit --class lk.uom.datasearch.homework.AnnualHomeWorkClassifyWithSpatialCertainty /media/education/0779713087/MSc/home-work-classify/target/scala-2.11/subscriber-home-work-classify-model_2.11-1.4.2.jar /media/education/0779713087/MSc/Data 1 0 54
   * Sample run on server: spark-submit --class lk.uom.datasearch.homework.AnnualHomeWorkClassifyWithSpatialCertainty /home/hadoop/jobs/subscriber-home-work-classify-model_2.11-1.4.2.jar /SCDR 1 0 54
   */

  val adj_schema: StructType = StructType(Array(
    StructField("ADJ_INDEX_1KM", IntegerType, false),
    StructField("ADJ_1", IntegerType, true),
    StructField("ADJ_2", IntegerType, true),
    StructField("ADJ_3", IntegerType, true),
    StructField("ADJ_4", IntegerType, true)
  ))

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AnnualHomeWorkClassifyModelWIthSpatialCertainty")
      .getOrCreate();

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


    val homeOutputLocation = dataRoot + "/output/sc/" + version + "/homeYear"
    val workOutputLocation = dataRoot + "/output/sc/" + version + "/workYear"

    val adjacent_map_file = dataRoot + "/4_adjacent_cells.txt"

    val  cdrDF = readParquetCDR(dataRoot, startWeek, endWeek, spark)

    val distinctHWTimeFilterDF = calculateTemporalAppearance(cdrDF)

    val wholeYearAppearanceCountHomeDF = distinctHWTimeFilterDF.filter("HW_TIME_FILTER='HOME_HOUR'")
      .groupBy("SUBSCRIBER_ID", "INDEX_1KM").agg(count(lit(1)).as("APPEARANCE_COUNT"))


    val adjacent_map = spark.read.format("csv").schema(adj_schema).csv(adjacent_map_file)

    val adjHomeAppearanceComputed = calculateSpatialAppearance(wholeYearAppearanceCountHomeDF, adjacent_map)

    var wholeYearAppearanceCountWorkDF = distinctHWTimeFilterDF.filter("HW_TIME_FILTER='WORK_HOUR'")
      .groupBy("SUBSCRIBER_ID", "INDEX_1KM").agg(count(lit(1)).as("APPEARANCE_COUNT"))

    val adjWorkAppearanceComputed = calculateSpatialAppearance(wholeYearAppearanceCountWorkDF, adjacent_map)

    val yearHomeLocation = getLocationFromMaxAppearance(adjHomeAppearanceComputed, homeOutputLocation)
    val yearWorkLocation = getLocationFromMaxAppearance(adjWorkAppearanceComputed, workOutputLocation)

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

  def calculateSpatialAppearance(wholeYearAppearanceCountDF: DataFrame, adjacent_map: DataFrame): DataFrame = {
    /**
     * Calculate the Spatial Adjacency appearance of subscribers in Cells
     */

    // Prepare the spatial adjacency joined subscriber table
    var adjacentJoinedHomeCellCount = wholeYearAppearanceCountDF.select(
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

    // Join the count table again with adj_1 table to get the appearance count in adj_1 area
    val adj_1 = adjacentJoinedHomeCellCount
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
      ).na.fill(0)

    val adjAppearanceComputed = adj_4.withColumn("ADJ_APPEARANCE",
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
