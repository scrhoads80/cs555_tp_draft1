import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame

object CreateAndSaveInitialGraphs {
  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      println("Need correct number of arguments to run:")
      println("combinedDfDir outputDir <local || yarn>")
      return
    }

    val combinedDfParquetDir = args(0)
    val outputDirectory = args(1)
    val localOrYarn = args(2)

    println(s"combined dataframe parquet input dir: $combinedDfParquetDir")
    println(s"output parquet dir: $outputDirectory")
    println(s"local or yarn: $localOrYarn")

    val spark = if (localOrYarn == "local") {
      SparkSession.builder().appName("CreateGraph").config("spark.master", "local[4]").getOrCreate()
    } else {
      SparkSession.builder().appName("CreateGraph").getOrCreate()
    }

    // allow filter opertations
    import spark.implicits._

    val submissionDf = spark.read.parquet(s"$combinedDfParquetDir/submissionPreppedDf")
    val commentDf = spark.read.parquet(s"$combinedDfParquetDir/commentPreppedDf")

    val allSubmissionAuthId = submissionDf.select($"op_id").withColumnRenamed("op_id", "id").distinct()
    val allCommentAuthId = commentDf.select($"auth_id").withColumnRenamed("auth_id", "id").distinct()

    // each user will be a node
    val allUsersEitherPostedOrCommented = allSubmissionAuthId.union(allCommentAuthId).distinct()

    // need to add op_id to comment df by matching comments link id and submissions id

    val trimmedSubmissionDf = submissionDf.drop("disable_comments", "num_comments", "original_link",
      "promoted", "promoted_by", "subreddit_id", "third_party_tracking")

    val trimmedCommentDf = commentDf.drop("body", "created_utc", "id", "parent_id", "subreddit_id", "link_id_str")


    val submissionCommentDf = trimmedCommentDf.join(trimmedSubmissionDf, "link_hash")

    // create edge dataframe
    val edgeDf = submissionCommentDf.select("op_id", "auth_id").withColumnRenamed("op_id", "dst").withColumnRenamed("auth_id", "src")

    val usersCommentingOnUsersGraphFrame = GraphFrame(allUsersEitherPostedOrCommented, edgeDf)

    usersCommentingOnUsersGraphFrame.vertices.write.parquet(s"$outputDirectory/userCommentingOnUsersVertices")
    usersCommentingOnUsersGraphFrame.edges.write.parquet(s"$outputDirectory/userCommentingOnUsersEdges")

    // create 2nd graph type nodes are subreddit and user - edges are subreddit-posts-user user-comments-user

    // df with just subreddit_id (int hashval), subreddit (name), type
    import org.apache.spark.sql.functions.lit

    val subRedditVertDf = submissionDf.select($"sub_id", $"subreddit").distinct()
      .withColumnRenamed("sub_id", "id").withColumnRenamed("subreddit", "name").withColumn("type", lit("subreddit"))

    val subUserVertDf = submissionDf.select($"op_id", $"author").distinct()
      .withColumnRenamed("op_id", "id").withColumnRenamed("author", "name").withColumn("type", lit("user"))

    val commentUserVertDf = commentDf.select($"auth_id", $"author").distinct()
      .withColumnRenamed("auth_id", "id").withColumnRenamed("author", "name").withColumn("type", lit("user"))

    // df for the vertices in the graph
    val subPostUserCommentGraphNodeDf = subRedditVertDf.union(subUserVertDf).union(commentUserVertDf).distinct()

    // create posted edges
    val userPostsEdgeDf = submissionDf.select($"op_id", $"sub_id").withColumnRenamed("op_id", "src")
      .withColumnRenamed("sub_id", "dst").withColumn("relationship", lit("posted"))

    val userCommentUserEdgeDf = edgeDf.withColumn("relationship", lit("commented"))

    // df for the edges in the graph
    val subPostUserCommentGraphEdgeDf = userPostsEdgeDf.union(userCommentUserEdgeDf)

    // create graph for sub - post - user - comment graph
    val subPostUserCommentGraph = GraphFrame(subPostUserCommentGraphNodeDf, subPostUserCommentGraphEdgeDf)

    // save out graph
    subPostUserCommentGraph.vertices.write.parquet(s"$outputDirectory/subPostUserCommentGraphVertices")
    subPostUserCommentGraph.edges.write.parquet(s"$outputDirectory/subPostUserCommentGraphEdges")

    spark.stop()
  }
}
