import org.apache.spark.sql.SparkSession
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

    /**
      *user commenting on user results
      */
    // strongly connected components
    val userCommentUserSscDf = spark.read.parquet(s"$graphAlgoParquetDir/stronglyConnectedUsersViaComments")

    val userCommentUserSscComponentCount = userCommentUserSscDf.groupBy("component").count().orderBy(desc("count")).filter($"count" > 2)

    val userCommentUserSscNodeAggDf = userCommentUserSscDf.groupBy("component").agg(collect_set("id").alias("ids"))

    val userCommentUserSscCountAndAggDf = userCommentUserSscComponentCount.join(userCommentUserSscNodeAggDf, "component")
    userCommentUserSscCountAndAggDf.show()

    // pagerank
    val userCommentUserPageRankVertDf = spark.read.parquet(s"$graphAlgoParquetDir/userCommentOnUsersPageRankVertices")
    userCommentUserPageRankVertDf.printSchema()
    val userCommentUserPageRankEdgeDf = spark.read.parquet(s"$graphAlgoParquetDir/userCommentOnUsersPageRankEdges")

    val uCuPageRankVertSortedDf = userCommentUserPageRankVertDf.orderBy(desc("pagerank"))
    uCuPageRankVertSortedDf.show(20)

    // pop piar
    val uCuPopularPair = spark.read.parquet(s"$graphAlgoParquetDir/userCommentOnUserPopularPair")
//    uCuPopularPair.printSchema()
    val uCuPopularPairSortedDf = uCuPopularPair.filter($"count" > 1).sort($"count".desc)
    uCuPopularPairSortedDf.show(20)

    val uCuPopulairPairMostCommonDst = uCuPopularPair.groupBy("dst").count().orderBy(desc("count"))
    uCuPopulairPairMostCommonDst.show(20)

    // todo triangle count







    // sub post user comment results

  }
}
