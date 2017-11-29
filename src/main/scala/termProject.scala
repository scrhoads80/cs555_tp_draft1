
import java.io.{File, PrintWriter}

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame

import scala.io._

object TermProjectRhoadsMalenseck{

  val DELETED_TXT = """[deleted]"""

  def main(args: Array[String]): Unit = {

    if(args.length != 3) {
      println("Need correct number of arguments to run:")
      println("graphVertAndEdgesDir outputDir <local || yarn>")
      return
    }

    val graphVertEdgeDfParquetDir = args(0)
    val outputDirectory = args(1)
    val localOrYarn = args(2)

    println(s"combined dataframe parquet input dir: $graphVertEdgeDfParquetDir")
    println(s"output parquet dir: $outputDirectory")
    println(s"local or yarn: $localOrYarn")

    val spark = if(localOrYarn == "local") {
      SparkSession.builder().appName("TermProject").config("spark.master", "local[4]").getOrCreate()
    } else {
      SparkSession.builder().appName("TermProject").getOrCreate()
    }

    // allow filter opertations

    val vertDf = spark.read.parquet(s"$graphVertEdgeDfParquetDir/userCommentingOnUsersVertices")
    val edgeDf = spark.read.parquet(s"$graphVertEdgeDfParquetDir/userCommentingOnUsersEdges")

    val usersCommentingOnUsersGraphFrame = GraphFrame(vertDf, edgeDf)

    val sscUsersOnUsers = usersCommentingOnUsersGraphFrame.stronglyConnectedComponents.maxIter(10).run()
    sscUsersOnUsers.write.parquet(s"$outputDirectory/stronglyConnectedUsersViaComments")

    val pageRankOnUsersGraph = usersCommentingOnUsersGraphFrame.pageRank.maxIter(10).run()
    pageRankOnUsersGraph.vertices.write.parquet(s"$outputDirectory/userCommentOnUsersPageRankVertices")
    pageRankOnUsersGraph.edges.write.parquet(s"$outputDirectory/userCommentOnUsersPageRankEdges")

    val userCommentUserPopularUserPair = edgeDf.groupBy("src", "dst").count().orderBy(desc("count"))
    userCommentUserPopularUserPair.write.parquet(s"$outputDirectory/userCommentOnUserPopularPair")

//    val userCommentUserTriangleCountDf = usersCommentingOnUsersGraphFrame.triangleCount.run()
//    userCommentUserTriangleCountDf.write.parquet(s"$outputDirectory/userCommentOnUserTriangleCount")

    // load up subreddit - post - user - comment vert and edges

    val subPostUserCommentVertDf = spark.read.parquet(s"$graphVertEdgeDfParquetDir/subPostUserCommentGraphVertices")
    val subPostUserCommentEdgeDf = spark.read.parquet(s"$graphVertEdgeDfParquetDir/subPostUserCommentGraphEdges")

    val subPostUserCommentGraphFrame = GraphFrame(subPostUserCommentVertDf, subPostUserCommentEdgeDf)

    val sscSubPostUserCommentGraphFrame = subPostUserCommentGraphFrame.stronglyConnectedComponents.maxIter(10).run()
    sscSubPostUserCommentGraphFrame.write.parquet(s"$outputDirectory/stronglyConnectedUsersViaSubPostUserComment")

    val pageRankSubPostUserGraphFrame = subPostUserCommentGraphFrame.pageRank.maxIter(10).run()
    pageRankSubPostUserGraphFrame.vertices.write.parquet(s"$outputDirectory/subPostUserCommentPageRankVertices")
    pageRankSubPostUserGraphFrame.edges.write.parquet(s"$outputDirectory/subPostUserCommentPageRankEdges")

    import spark.implicits._

    val subPostUserCommentUserPostingSubredditPopularPair = subPostUserCommentEdgeDf.filter($"relationship" === "posted")
      .groupBy("src", "dst").count().orderBy(desc("count"))

    subPostUserCommentUserPostingSubredditPopularPair.write.parquet(s"$outputDirectory/subPostUserCommentUserPostSubCount")

//    val subPostUserCommentTriangleCountDf = subPostUserCommentGraphFrame.triangleCount.run()
//    subPostUserCommentTriangleCountDf.write.parquet(s"$outputDirectory/subPostUserCommentTriangleCount")

    spark.stop()

  }

}
