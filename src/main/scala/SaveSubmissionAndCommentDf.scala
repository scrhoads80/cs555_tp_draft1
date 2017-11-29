import org.apache.spark.sql.functions.{isnull, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SaveSubmissionAndCommentDf {

  val DELETED_TXT = """[deleted]"""
  val SUBREDDIT_FILTER_LIST = List("The_Donald", "HillaryClinton", "SandersForPresident", "politics",
    "Conservative", "Republican", "Democrats", "Libertarian", "Ask_Politics", "POLITIC", "PoliticalDiscussion",
    "NeutralPolitics", "ModeratePolitics", "Anarchism", "Anarcho_Capitalism")

  def main(args: Array[String]): Unit = {

    if(args.length != 6) {
      println("error with number of arguments")
      println("Need: commentParquetDir submissionParquetDir outputDir <local | yarn> <comma seperated list of 2digit months. No Spaces! e.g. 09,10,11> <true | false>-true to limit to political subreddits")
      return
    }

    val commentParquetDirectory = args(0)
    val submissionParquetDirectory = args(1)
    val outputDirectory = args(2)
    val localOrYarn = args(3)
    val commanStrOfMonths = args(4)
    val doLimitToPoliticalSubredditStr = args(5)

    val doLimitToPoliticalSubreddit = if(doLimitToPoliticalSubredditStr == "true") {
      true
    } else {
      false
    }

    println(s"comment parquet input dir: $commentParquetDirectory")
    println(s"submission parquet input dir: $submissionParquetDirectory")
    println(s"output parquet dir: $outputDirectory")
    println(s"yarn or local: $localOrYarn")
    println(s"months: $commanStrOfMonths")
    println(s"doLimitToPoliticalSubreddit: $doLimitToPoliticalSubreddit")

    val spark = if(localOrYarn == "local") {
      SparkSession.builder().appName("TermProject").config("spark.master", "local[4]").getOrCreate()
    } else {
      SparkSession.builder().appName("TermProject").getOrCreate()
    }

    // list of comments and submissions to open up
    val monthsList = commanStrOfMonths.split(',').toSeq

    // comprehension loading up next parquet, modifying schema, and unioning w/ initial
    val combinedSubmissionDf = (for (month <- monthsList) yield
      trimSchemaForSubmissions(retrieveParquetAndTrimContent(submissionParquetDirectory, "RS_2016",
        month, true, spark, doLimitToPoliticalSubreddit))) reduce (_ union _)

    combinedSubmissionDf.printSchema()

    val combinedCommentDf = (for (month <- monthsList) yield
      trimSchemaForComments(retrieveParquetAndTrimContent(submissionParquetDirectory, "RC_2016",
        month, false, spark, doLimitToPoliticalSubreddit))) reduce (_ union _)

    combinedCommentDf.printSchema()

    combinedSubmissionDf.write.parquet(s"$outputDirectory/submissionPreppedDf")
    combinedCommentDf.write.parquet(s"$outputDirectory/commentPreppedDf")

    spark.stop()


  }

  def retrieveParquetAndTrimContent(submissionParquetDirectory:String, typeYearPrefix:String, month:String, isSubmission: Boolean,
                                    sparkSession: SparkSession, doLimitToPoliticalSubreddit: Boolean): DataFrame = {
    import sparkSession.implicits._

    // remove deleted authors, filter on specific subreddits, then return to be unioned with others

    val commonOperationDf = removeDeletedAuthors(sparkSession.read.parquet(s"$submissionParquetDirectory/$typeYearPrefix-$month"), sparkSession)


    val filterOnSubredditDf = if(doLimitToPoliticalSubreddit) {
      commonOperationDf.filter(($"subreddit".isin(SUBREDDIT_FILTER_LIST:_*)))
    } else {
      commonOperationDf
    }

    val retDf = if(isSubmission) {
      val removedZeroCommentsDf = removePostsWithZeroComments(filterOnSubredditDf, sparkSession)
      val nameToLinkHashDf = createColumnWithHashedValue(removedZeroCommentsDf, "name", "link_hash")
      val authToOpIdDf = createColumnWithHashedValue(nameToLinkHashDf, "author", "op_id")
      val subredditIdDf = createColumnWithHashedValue(authToOpIdDf, "subreddit", "sub_id")
      subredditIdDf
    } else {
      val linkIdToLinkHasDf = createColumnWithHashedValue(filterOnSubredditDf, "link_id", "link_hash")
      val authToAuthId = createColumnWithHashedValue(linkIdToLinkHasDf, "author", "auth_id")
      val subredditIdDf = createColumnWithHashedValue(authToAuthId, "subreddit", "sub_id")
      subredditIdDf
    }

    retDf
  }

  def trimSchemaForSubmissions(df: DataFrame): DataFrame = {
    val keptColDf = df.drop("author_flair_css_class", "author_flair_text", "clicked", "hidden", "is_self", "likes")
      .drop("link_flair_css_class", "link_flair_text", "locked", "media", "media_embed", "over_18", "permalink", "saved")
      .drop("score", "selftext", "selftext_html", "thumbnail", "title", "url", "edited", "distinguished", "stickied")
      .drop("adserver_click_url", "adserver_imp_pixel", "preview", "secure_media", "secure_media_embed" )
      .drop("archived", "contest_mode", "downs", "href_url", "domain", "gilded", "hide_score", "imp_pixel", "mobile_ad_url", "post_hint")
      .drop("quarantine", "retrieved_on", "third_party_tracking", "third_party_tracking_2", "ups", "promoted_display_name", "promoted_url")
      .drop("spoiler", "original_link", "from", "from_id", "from_kind", "promoted", "promoted_by")

//    keptColDf.printSchema()

    keptColDf
  }

  def trimSchemaForComments(df: DataFrame): DataFrame = {
    /*df.drop("author_flair_css_class", "author_flair_text", "controversiality", "distinguished", "edited", "gilded")
      .drop("retrieved_on", "score", "stickied", "ups")
      */

    val keepColNames = Seq("author", "body", "created_utc", "id", "link_id", "parent_id", "subreddit", "subreddit_id",
      "link_hash", "auth_id", "sub_id")

    val keptColDf = df.select(keepColNames.head, keepColNames.tail: _*)
    keptColDf
  }

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
