import org.apache.spark.sql.functions.{isnull, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SaveSubmissionAndCommentDf {

  val DELETED_TXT = """[deleted]"""
  val SUBREDDIT_FILTER_LIST = List("The_Donald", "HillaryClinton", "SandersForPresident", "politics",
    "Conservative", "Republican", "Democrats", "Libertarian", "Ask_Politics", "POLITIC", "PoliticalDiscussion")

  def main(args: Array[String]): Unit = {

    if(args.length != 5) {
      println("error with number of arguments")
      println("Need: commentParquetDir submissionParquetDir outputDir <local | yarn> <comma seperated list of 2digit months. No Spaces! e.g. 09,10,11>")
      return
    }

    val commentParquetDirectory = args(0)
    val submissionParquetDirectory = args(1)
    val outputDirectory = args(2)
    val localOrYarn = args(3)
    val commanStrOfMonths = args(4)

    println(s"comment parquet input dir: $commentParquetDirectory")
    println(s"submission parquet input dir: $submissionParquetDirectory")
    println(s"output dir: $outputDirectory")
    println(s"yarn or local: $localOrYarn")
    println(s"months: $commanStrOfMonths")

    val spark = if(localOrYarn == "local") {
      SparkSession.builder().appName("TermProject").config("spark.master", "local[4]").getOrCreate()
    } else {
      SparkSession.builder().appName("TermProject").getOrCreate()
    }

    // list of comments and submissions to open up
    val monthsList = commanStrOfMonths.split(',').toSeq

    // comprehension loading up next parquet, modifying schema, and unioning w/ initial
    val combinedSubmissionDf = (for (month <- monthsList) yield
      trimSchemaForSubmissions(retrieveParquetAndTrimContent(submissionParquetDirectory, "RS_2016", month, true, spark))) reduce (_ union _)

    val combinedCommentDf = (for (month <- monthsList) yield
      trimSchemaForComments(retrieveParquetAndTrimContent(submissionParquetDirectory, "RC_2016", month, false, spark))) reduce (_ union _)

    combinedSubmissionDf.write.parquet(s"$submissionParquetDirectory/submissionPreppedDf")
    combinedCommentDf.write.parquet(s"$commentParquetDirectory/commentPreppedDf")

    spark.stop()


  }

  def retrieveParquetAndTrimContent(submissionParquetDirectory:String, typeYearPrefix:String, month:String, isSubmission: Boolean, sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    // remove deleted authors, filter on specific subreddits, then return to be unioned with others

    val commonOperationDf = removeDeletedAuthors(sparkSession.read.parquet(s"$submissionParquetDirectory/$typeYearPrefix-$month"), sparkSession)
      .filter(($"subreddit".isin(SUBREDDIT_FILTER_LIST:_*)))

    val retDf = if(isSubmission) {
      val removedZeroCommentsDf = removePostsWithZeroComments(commonOperationDf, sparkSession)
      val nameToLinkHashDf = createColumnWithHashedValue(removedZeroCommentsDf, "name", "link_hash")
      val authToOpIdDf = createColumnWithHashedValue(nameToLinkHashDf, "author", "op_id")
      authToOpIdDf
    } else {
      val linkIdToLinkHasDf = createColumnWithHashedValue(commonOperationDf, "link_id", "link_hash")
      val authToAuthId = createColumnWithHashedValue(linkIdToLinkHasDf, "author", "auth_id")
      authToAuthId
    }

    retDf
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
