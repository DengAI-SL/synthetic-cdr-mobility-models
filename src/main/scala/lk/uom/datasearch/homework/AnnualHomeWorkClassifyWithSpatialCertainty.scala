package lk.uom.datasearch.homework

import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object AnnualHomeWorkClassifyWithSpatialCertainty {
  /*
   * Sample run : ./spark-submit --class lk.uom.datasearch.homework.AnnualHomeWorkClassifyWithSpatialCertainty /media/education/0779713087/MSc/home-work-classify/target/scala-2.11/subscriber-home-work-classify-model_2.11-1.4.2.jar /media/education/0779713087/MSc/Data 1
   * Sample run on server: spark-submit --class lk.uom.datasearch.homework.AnnualHomeWorkClassifyWithSpatialCertainty /home/hadoop/data/jobs/subscriber-home-work-classify-model_2.11-1.4.2.jar /SCDR 1
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

    val homeOutputLocation = dataRoot + "/output/sc/" + version + "/homeYear"
    val workOutputLocation = dataRoot + "/output/sc/" + version + "/workYear"

    val adjacent_map_file = dataRoot + "/4_adjacent_cells.txt"

    val  cdrDF = readParquetCDR(dataRoot, spark)

    val distinctHWTimeFilterDF = calculateTemporalAppearance(cdrDF)

    val wholeYearAppearanceCountHomeDF = distinctHWTimeFilterDF.filter("HW_TIME_FILTER='HOME_HOUR'")
      .groupBy("SUBSCRIBER_ID", "INDEX_1KM").agg(count(lit(1)).as("APPEARANCE_COUNT"))


    val adjacent_map = spark.read.format("csv").schema(adj_schema).csv(adjacent_map_file)

    val adjHomeAppearanceComputed = calculateSpatialAppearance(wholeYearAppearanceCountHomeDF, adjacent_map)
//    adjHomeAppearanceComputed.write.option("header", "true").csv(homeOutputLocation)

    var wholeYearAppearanceCountWorkDF = distinctHWTimeFilterDF.filter("HW_TIME_FILTER='WORK_HOUR'")
      .groupBy("SUBSCRIBER_ID", "INDEX_1KM").agg(count(lit(1)).as("APPEARANCE_COUNT"))

    val adjWorkAppearanceComputed = calculateSpatialAppearance(wholeYearAppearanceCountWorkDF, adjacent_map)
//    adjWorkAppearanceComputed.write.option("header", "true").csv(workOutputLocation)

    val cell_centers = dataRoot + "/1KM_CELL_CENTRES.csv"
    val cellCentersDf = spark.read.option("header", "true").csv(cell_centers)

    val yearHomeLocation = getLocationFromMaxAppearance(adjHomeAppearanceComputed, cellCentersDf, homeOutputLocation)
    val yearWorkLocation = getLocationFromMaxAppearance(adjWorkAppearanceComputed, cellCentersDf, workOutputLocation)

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

  def readParquetCDR(dataRoot: String, spark : SparkSession): DataFrame ={
    val dataDirs = List(
      //                      "/synv_20130601_20131201"
      "/generated_voice_records_20121201_20130601",
      "/generated_voice_records_20130601_20131201"
    ).map(path => dataRoot+path)


    // Reading parquet CDR Data
    val cdrDF = spark.read.parquet(dataDirs:_*)
      .select(col("SUBSCRIBER_ID"), to_timestamp(col("CALL_TIME"), "yyyyMMddHHmmss").as("CALL_TIMESTAMP"), col("INDEX_1KM"))
      .withColumn("WEEK_OF_YEAR", date_format(col("CALL_TIMESTAMP"), "w").cast(IntegerType))
      .withColumn("DATE_OF_YEAR", date_format(col("CALL_TIMESTAMP"), "D").cast(IntegerType))
      .withColumn("HOUR_OF_DAY", date_format(col("CALL_TIMESTAMP"), "H").cast(IntegerType));

    return cdrDF
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
      col("APPEARANCE_COUNT") + col("ADJ_APPEARANCE")/2)
    return adjLocationAppearanceComputed
  }

  def getLocationFromMaxAppearance(appearanceCountDF: DataFrame, cellCentersDf: DataFrame, outputDirectory: String): DataFrame ={

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
