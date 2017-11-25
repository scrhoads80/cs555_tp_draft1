
import java.io.{File, PrintWriter}

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame

import scala.io._

object TermProjectRhoadsMalenseck{

  val DELETED_TXT = """[deleted]"""

  def main(args: Array[String]): Unit = {

    if(args.length != 4) {
      println("Need correct number of arguments to run:")
      println("commentParquetDir submissionParquetDir outputDir <local || yarn>")
      return
    }

    val commentParquetDirectory = args(0)
    val submissionParquetDirectory = args(1)
    val outputDirectory = args(2)
    val localOrYarn = args(3)

    println(s"comment parquet input dir: $commentParquetDirectory")
    println(s"submission parquet input dir: $submissionParquetDirectory")
    println(s"output dir: $outputDirectory")
    println(s"local or yarn: $localOrYarn")

//    val spark = SparkSession.builder().appName("Simple App").config("spark.master", "local[4]").getOrCreate()
    val spark = if(localOrYarn == "local") {
      SparkSession.builder().appName("TermProject").config("spark.master", "local[4]").getOrCreate()
    } else {
      SparkSession.builder().appName("TermProject").getOrCreate()
    }

    // allow filter opertations
    import spark.implicits._

    // read bz2 file
//    val commentDf = spark.read.json("/home/scrhoads/workspaces/graphx_learning/rsrc/RS_2016-09.bz2")

    // write data frame to disk
    //    commentDf.write.parquet("/home/scrhoads/workspaces/graphx_learning/rsrc/submission_parquet2")

    val septSubmissionDf = spark.read.parquet(s"$submissionParquetDirectory/RS_2016-09")
    val septModSchemaSubmissionDf = trimSchemaForSubmissions(septSubmissionDf)

//    val septColsSet = septSubmissionDf.schema.fields.map(_.name).toSet

    val octSubmissionDf = spark.read.parquet(s"$submissionParquetDirectory/RS_2016-10")
    val octModSchemaSubmissionDf = trimSchemaForSubmissions(octSubmissionDf)


    val septCommentDf = spark.read.parquet(s"$commentParquetDirectory/RC_2016-09")
    val septModSchemaCommentdf = trimSchemaForComments(septCommentDf)
//    septModSchemaCommentdf.printSchema()

    val octCommentDf = spark.read.parquet(s"$commentParquetDirectory/RC_2016-10")
    val octModSchemaCommentdf = trimSchemaForComments(octCommentDf)

//    val diffColSet = octColsSet -- septColsSet
//    println(diffColSet)

//    val submissionDf = septModSchemaSubmissionDf.union(octModSchemaSubmissionDf)
//    val commentDf = septModSchemaCommentdf.union(octModSchemaCommentdf)
      val submissionDf = septModSchemaSubmissionDf
      val commentDf = septModSchemaCommentdf

    val nonDeletedSubmissionDf = removeDeletedAuthors(submissionDf, spark)
    val nonDeletedCommentsDf = removeDeletedAuthors(commentDf, spark)

    val zeroCommentsRemovedSubmissionDf = removePostsWithZeroComments(nonDeletedSubmissionDf, spark)
//    zeroCommentsRemovedSubmissionDf.show(10)
//    nonDeletedCommentsDf.show(10)

//    val foundPostId = nonDeletedCommentsDf.filter($"link_id" === "t3_53epsb")
//    foundPostId.show()

    // need to create ints for name in submission and link_id on comment - both will be called link_hash for joining later

    val intIdSubmissionDf = createColumnWithHashedValue(zeroCommentsRemovedSubmissionDf, "name", "link_hash")
    val intIdCommentDf = createColumnWithHashedValue(nonDeletedCommentsDf, "link_id", "link_hash")

    val addAuthIdSubmissionDf = createColumnWithHashedValue(intIdSubmissionDf, "author", "op_id").filter($"subreddit" === "The_Donald")
    val addAuthIdCommentDf = createColumnWithHashedValue(intIdCommentDf, "author", "auth_id").filter($"subreddit" === "The_Donald")

    val allSubmissionAuthId = addAuthIdSubmissionDf.select($"op_id").withColumnRenamed("op_id", "id")
    val allCommentAuthId = addAuthIdCommentDf.select($"auth_id").withColumnRenamed("auth_id", "id")

    // each user will be a node
    val allUsersEitherPostedOrCommented = allSubmissionAuthId.union(allCommentAuthId).distinct()

    // need to add op_id to comment df by matching comments link id and submissions id
//    val commentsWithLinkIdAuthId = addAuthIdCommentDf.jo

    val trimmedSubmissionDf = addAuthIdSubmissionDf.drop("created_utc", "disable_comments", "name",
      "num_comments", "original_link", "promoted", "promoted_by", "subreddit", "subreddit_id", "third_party_tracking")

    val trimmedCommentDf = addAuthIdCommentDf.drop("body", "created_utc", "id", "parent_id", "subreddit",
      "subreddit_id", "link_id_str")


    val submissionCommentDf = trimmedCommentDf.join(trimmedSubmissionDf, "link_hash")

    // create edge dataframe
    val edgeDf = submissionCommentDf.select("op_id", "auth_id").withColumnRenamed("op_id", "dst").withColumnRenamed("auth_id", "src")

    val usersCommentingOnUsersGraphFrame = GraphFrame(allUsersEitherPostedOrCommented, edgeDf)
//    val g1 = GraphFrame(commentDf, null)
//    println(usersCommentingOnUsersGraphFrame.vertices.count())
//    println(usersCommentingOnUsersGraphFrame.edges.count())
    val sscUsersOnUsers = usersCommentingOnUsersGraphFrame.stronglyConnectedComponents.maxIter(2).run()
    val orderedSscUsers = sscUsersOnUsers.orderBy("component")
    sscUsersOnUsers.show(10)

    new PrintWriter(new File(s"$outputDirectory/output")) {
      try {
        write(sscUsersOnUsers.show(100).toString)
      } finally {
        close()
      }
    }

    spark.stop()

  }

  def trimSchemaForSubmissions(df: DataFrame): DataFrame = {
    df.drop("author_flair_css_class", "author_flair_text", "clicked", "hidden", "is_self", "likes")
      .drop("link_flair_css_class", "link_flair_text", "locked", "media", "media_embed", "over_18", "permalink", "saved")
      .drop("score", "selftext", "selftext_html", "thumbnail", "title", "url", "edited", "distinguished", "stickied")
      .drop("adserver_click_url", "adserver_imp_pixel", "preview", "secure_media", "secure_media_embed" )
      .drop("archived", "contest_mode", "downs", "href_url", "domain", "gilded", "hide_score", "imp_pixel", "mobile_ad_url", "post_hint")
      .drop("quarantine", "retrieved_on", "third_pary_tracking", "third_party_tracking_2", "ups", "promoted_display_name", "promoted_url")
      .drop("spoiler")
  }

  def trimSchemaForComments(df: DataFrame): DataFrame = {
    df.drop("author_flair_css_class", "author_flair_text", "controversiality", "distinguished", "edited", "gilded")
      .drop("retrieved_on", "score", "stickied", "ups")
  }

  /*
  def changeIdToIntHashOfIdText(df: DataFrame): DataFrame = {



    // create hash instead of text id
    val idHashDf = df.withColumn("id", udfGetStrHash(df("id")))

    idHashDf
  }

  def addAuthorIdColumn(df: DataFrame, newColumnName: String): DataFrame = {
    val udfGetStrHash = udf { (id: String) =>
      id.hashCode
    }

    val addAuthIdDf = df.withColumn(newColumnName, udfGetStrHash(df("author")))
    addAuthIdDf
  }*/

  def createColumnWithHashedValue(df: DataFrame, oldColumnName: String, newColumnName: String): DataFrame = {
    val udfGetStrHash = udf { (id: String) =>
      id.hashCode
    }

    val idHashDf = df.withColumn(newColumnName, udfGetStrHash(df(oldColumnName)))
    idHashDf
  }

  def removeDeletedAuthors(df: DataFrame, sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    df.filter(!(isnull($"author") || $"author" === DELETED_TXT))
  }

  def removePostsWithZeroComments(df: DataFrame, sparkSession: SparkSession): DataFrame = {
    import  sparkSession.implicits._
    df.filter(!($"num_comments" === 0))
  }

}
