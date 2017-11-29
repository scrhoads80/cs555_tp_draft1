import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame

object WorkWithGraphAlgResutls {
  def main(args: Array[String]): Unit = {

    if(args.length != 3) {
      println("incorrect number of arguments")
      println("graphAlgoParquetDir outputDir <local | yarn>")
      return
    }

    val graphAlgoParquetDir = args(0)
    val outputDir = args(1)
    val localOrYarn = args(2)

    println(s"graphAlgoParquetDir: $graphAlgoParquetDir")
    println(s"outputDir: $outputDir")
    println(s"local | yarn: $localOrYarn")

    val spark = if(localOrYarn == "local") {
      SparkSession.builder().appName("TermProject").config("spark.master", "local[4]").getOrCreate()
    } else {
      SparkSession.builder().appName("TermProject").getOrCreate()
    }

    import  spark.implicits._

    // used to get user information mapped to results
    val subPostUserCommentVertDf = spark.read.parquet(s"$graphAlgoParquetDir/subPostUserCommentGraphVertices")

    /**
      *user commenting on user results
      */
    // strongly connected components
    /* df schema
      id | component
     */
    val userCommentUserSscDf = spark.read.parquet(s"$graphAlgoParquetDir/stronglyConnectedUsersViaComments")

    /* df schema
      component | count
     */
    val userCommentUserSscComponentCount = userCommentUserSscDf.groupBy("component").count().orderBy(desc("count")).filter($"count" > 2)

    writeStronglyConnectedComponentCountsToDisk(userCommentUserSscComponentCount, subPostUserCommentVertDf, s"$outputDir/stronglyConnectedComponentCountCommentsCsv")

    writeStronglyConnectedComponentToIdsToDisk(userCommentUserSscDf, userCommentUserSscComponentCount, subPostUserCommentVertDf,
      s"$outputDir/stronglyConnectedComponentComponentToNamesCommentsJson")

    val commentPreppedDf = spark.read.parquet(s"$graphAlgoParquetDir/commentPreppedDf")
    val iH8NormiesSubredditCounts = commentPreppedDf.filter($"author" === "Ih8Normies").groupBy("subreddit").count()
    iH8NormiesSubredditCounts.show(50)

    val joe4942SubredditCounts = commentPreppedDf.filter($"author" === "joe4942").groupBy("subreddit").count()
    joe4942SubredditCounts.show(50)



    // pagerank
    val userCommentUserPageRankVertDf = spark.read.parquet(s"$graphAlgoParquetDir/userCommentOnUsersPageRankVertices")
    userCommentUserPageRankVertDf.printSchema()

    val uCuPageRankVertSortedDf = userCommentUserPageRankVertDf.orderBy(desc("pagerank")).join(
      subPostUserCommentVertDf, "id")

    uCuPageRankVertSortedDf.write.format("com.databricks.spark.csv").option("header", "false").save(s"$outputDir/pageRankCommentsCsv")


    // pop piar
    val uCuPopularPair = spark.read.parquet(s"$graphAlgoParquetDir/userCommentOnUserPopularPair")

    popularPairWriteToDisk(uCuPopularPair, subPostUserCommentVertDf, s"$outputDir", "CommentsCsv", spark)


    // triangle
//    val uCuTriangleCntDf = spark.read.parquet(s"$graphAlgoParquetDir/userCommentOnUserTriangleCount")
//    val uCuTriangleCntOrderdDf = uCuTriangleCntDf.orderBy(desc("count"))
//    uCuTriangleCntOrderdDf.show(20)


//    val metaDataWithResultsDf = uCuTriangleCntOrderdDf.join(subPostUserCommentVertDf, "id")
//    metaDataWithResultsDf.show(20)

    // sub post user comment results

    // strongly connected
    val subPostUserCommentSscDf = spark.read.parquet(s"$graphAlgoParquetDir/stronglyConnectedUsersViaSubPostUserComment")

    val sPucSscComponentCountDf = subPostUserCommentSscDf.groupBy("component").count().orderBy(desc("count")).filter($"count" > 2)

    writeStronglyConnectedComponentCountsToDisk(sPucSscComponentCountDf, subPostUserCommentVertDf, s"$outputDir/stronglyConnectedComponentCountSubPostUserCommentCsv")

    subPostUserCommentSscDf.printSchema()
    sPucSscComponentCountDf.printSchema()
    subPostUserCommentVertDf.printSchema()

    writeStronglyConnectedComponentToIdsToDisk(subPostUserCommentSscDf.drop("name", "type"), sPucSscComponentCountDf,
      subPostUserCommentVertDf, s"$outputDir/stronglyConnectedComponentComponentToNamesSubPostUserCommentJson")


    // page rank
    val sPucsPageRankVertDf = spark.read.parquet(s"$graphAlgoParquetDir/subPostUserCommentPageRankVertices")

    val sPucsPageRankSortedDf = sPucsPageRankVertDf.orderBy(desc("pagerank"))
    sPucsPageRankSortedDf.write.format("com.databricks.spark.csv").option("header", "false").save(s"$outputDir/pageRankSubPostUserCommentCsv")


    //popular pair - focused on posting to subreddits b/c commenting was addressed in the graph results above
    val sPucsPopularPairDf = spark.read.parquet(s"$graphAlgoParquetDir/subPostUserCommentUserPostSubCount")

    popularPairWriteToDisk(sPucsPopularPairDf, subPostUserCommentVertDf, outputDir, "SubPostUserCommentCsv", spark)

    // triangle
//    val sPucsTriangleCntDf = spark.read.parquet(s"$graphAlgoParquetDir/subPostUserCommentTriangleCount")
//    val sPucsTriangleCntOrderedDf = sPucsTriangleCntDf.orderBy(desc("count"))
//    sPucsTriangleCntOrderedDf.show(20)


    spark.stop()
  }

  def popularPairWriteToDisk(popPairDf: DataFrame, idNameDf: DataFrame, outputFolder: String, graphType: String, spark: SparkSession): Unit = {
    import  spark.implicits._

    val uCuPopularPairSortedDf = popPairDf.filter($"count" > 1).filter($"src" =!= $"dst").sort($"count".desc).join(
      idNameDf.select("id", "name", "type").withColumnRenamed("id", "src").withColumnRenamed("name", "src_name").withColumnRenamed(
        "type", "src_type"), "src").join(
      idNameDf.select("id", "name", "type").withColumnRenamed("id", "dst").withColumnRenamed("name", "dst_name").
        withColumnRenamed("type", "dst_type"), "dst")

    uCuPopularPairSortedDf.write.format("com.databricks.spark.csv").option("header", "false").save(
      s"$outputFolder/popularPairAll$graphType"
    )

    val uCuPopulairPairMostCommonDst = popPairDf.groupBy("dst").count().orderBy(desc("count")).join(
      idNameDf.withColumnRenamed("id", "dst"), "dst")

    uCuPopulairPairMostCommonDst.write.format("com.databricks.spark.csv").option("header", "false").save(
      s"$outputFolder/popularPairMostCommonDst$graphType"
    )

  }

  def writeStronglyConnectedComponentCountsToDisk(sccDf: DataFrame, usersDf: DataFrame, saveLocation:String): DataFrame = {
    val userCommentUserSscComponentCountWithNames = sccDf.join(
      usersDf.withColumnRenamed("id", "component"), "component")

    userCommentUserSscComponentCountWithNames.write.format("com.databricks.spark.csv").option("header", "false").save(
      saveLocation)

    userCommentUserSscComponentCountWithNames
  }

  def writeStronglyConnectedComponentToIdsToDisk(sccDf: DataFrame, sccComponentCountDf: DataFrame, idToNameDf: DataFrame , outputPath: String): Unit = {
    val userCommentUserSccIdsNamedDf = sccDf.join(idToNameDf, "id")
    //    val userCommentUserSscNodeAggDf = userCommentUserSscDf.groupBy("component").agg(collect_set("id").alias("ids"))
    val userCommentUserSscNodeAggDf = userCommentUserSccIdsNamedDf.groupBy("component").agg(collect_set("name").alias("names"))

    val userCommentUserSscCountAndAggDf = sccComponentCountDf.join(userCommentUserSscNodeAggDf, "component").join(
      idToNameDf.withColumnRenamed("id", "component"), "component")

    userCommentUserSscCountAndAggDf.show()
    userCommentUserSscCountAndAggDf.write.json(outputPath)

  }
}
