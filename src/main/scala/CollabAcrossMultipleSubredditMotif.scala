import org.apache.spark.sql.{DataFrame, SparkSession}
import org.graphframes.GraphFrame

object CollabAcrossMultipleSubredditMotif {
  def main(args: Array[String]): Unit = {

    if(args.length != 3) {
      println("incorrect number of arguments")
      println("<graphs dir> <output dir> <local | yarn>")
      return
    }

    val graphsDir = args(0)
    val outputDir = args(1)
    val localOrYarn = args(2)

    val spark = if (localOrYarn == "local") {
      SparkSession.builder().appName("CreateGraph").config("spark.master", "local[4]").getOrCreate()
    } else {
      SparkSession.builder().appName("CreateGraph").getOrCreate()
    }

    import spark.implicits._

    // load up subreddit - post - user - comment vert and edges

    val subPostUserCommentVertDf = spark.read.parquet(s"$graphsDir/subPostUserCommentGraphVertices")
    subPostUserCommentVertDf.printSchema()
    subPostUserCommentVertDf.select("type").distinct().show(10)
    val subPostUserCommentEdgeDf = spark.read.parquet(s"$graphsDir/subPostUserCommentGraphEdges")
    subPostUserCommentEdgeDf.printSchema()

    val subPostUserCommentGraphFrame = GraphFrame(subPostUserCommentVertDf, subPostUserCommentEdgeDf)

    // find user who posts in multiple subreddits and is commented on by the same people
    val postMultipleSubredditsAndCommentorFollowsDf = motifFind(subPostUserCommentGraphFrame)

//    val opSubsCommenterDf = postMultipleSubredditsAndCommentorFollowsDf.select($"u.name".alias("user_name1"), "s.name", "s2.name", "u2.name")
    val opSubsCommenterDf = postMultipleSubredditsAndCommentorFollowsDf.select($"u.name".alias("user1"),
      $"s.name".alias("subreddit1"), $"s2.name".alias("subreddit2"), $"u2.name".alias("user2"))

    opSubsCommenterDf.show(10)

    opSubsCommenterDf.write.json(s"$outputDir/collabAcrossMultipleSubredditsMotif")

    spark.stop()
  }

  def motifFind(fullGraph: GraphFrame): DataFrame = {

    val allMotifs = fullGraph.find("(u)-[e1]->(s);(u)-[e2]->(s2);(u2)-[e3]->(s);(u2)-[e4]->(s2);(u2)-[e5]->(u);(u)-[e6]->(u2)")

    /*val sAndS2SubredditMotif = allMotifs.filter("e.relationship = 'posted'").filter("e2.relationship = 'posted'").filter(
      "e3.relationship = 'posted'").filter("e4.relationship = 'posted'").filter("s.type = 'subreddit'").filter(
      "s2.type = 'subreddit'").filter("s.id != s2.id").filter("u.id != u2.id")
      */
    val sAndS2SubredditMotif = allMotifs.filter("s.type = 'subreddit'").filter("s2.type = 'subreddit'").filter(
      "s.id != s2.id").filter("u.id != u2.id").filter("e5.relationship = 'commented'").filter("e6.relationship = 'commented'")

    sAndS2SubredditMotif


  }
}
