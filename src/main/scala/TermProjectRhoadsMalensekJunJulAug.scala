import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame
import org.apache.spark.sql.functions.{concat, lit}

object TermProjectRhoadsMalensekJunJulAug {

  def hash = udf((id: String) => id.hashCode)

  def drop(df: DataFrame): DataFrame = {
    df.drop("author_flair_css_class", "author_flair_text", "clicked", "hidden", "is_self", "likes")
      .drop("link_flair_css_class", "link_flair_text", "locked", "media", "media_embed", "over_18", "permalink", "saved")
      .drop("score", "selftext", "selftext_html", "thumbnail", "title", "url", "edited", "distinguished", "stickied")
      .drop("adserver_click_url", "adserver_imp_pixel", "preview", "secure_media", "secure_media_embed" )
      .drop("archived", "contest_mode", "downs", "href_url", "domain", "gilded", "hide_score", "imp_pixel", "mobile_ad_url", "post_hint")
      .drop("quarantine", "retrieved_on", "third_party_tracking", "third_party_tracking_2", "ups", "promoted_display_name", "promoted_url")
      .drop("spoiler", "author_flair_css_class", "author_flair_text", "controversiality", "distinguished", "edited", "gilded")
      .drop("retrieved_on", "score", "stickied", "ups", "promoted", "from", "from_id")
  }

  def prepSubs(df: DataFrame, sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    drop(df)
      .filter(!($"num_comments" === 0))
      .filter(!(isnull($"author") || $"author" === "[deleted]"))
      .withColumn("link_hash", hash($"name"))
      .withColumn("auth_id", hash($"author"))
      .filter($"subreddit" === "Politics" || $"subreddit" === "The_Donald" || $"subreddit" === "HillaryClinton" || $"subreddit" === "SandersForPresident")
  }

  def prepComments(df: DataFrame, sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    drop(df)
      .filter(!(isnull($"author") || $"author" === "[deleted]"))
      .withColumn("link_hash", hash($"link_id"))
      .withColumn("auth_id", hash($"author"))
      .filter($"subreddit" === "Politics" || $"subreddit" === "The_Donald" || $"subreddit" === "HillaryClinton" || $"subreddit" === "SandersForPresident")
  }

  def main(args: Array[String]): Unit = {

    if(args.length != 1) {
      println("incorrect number of args")
      println("<local | yarn")
      return
    }

    val localOrYarn = args(0)

    val spark = if (localOrYarn == "local") {
      SparkSession.builder().appName("CreateGraph").config("spark.master", "local[4]").getOrCreate()
    } else {
      SparkSession.builder().appName("CreateGraph").getOrCreate()
    }

    val junSubmissionDf = spark.read.parquet("/555/tp/subs/parquet/2016-06")
    val julSubmissionDf = spark.read.parquet("/555/tp/subs/parquet/2016-07")
    val augSubmissionDf = spark.read.parquet("/555/tp/subs/parquet/2016-08")

    val junSubPrep = prepSubs(junSubmissionDf, spark)
    val julSubPrep = prepSubs(julSubmissionDf, spark)
    val augSubPrep = prepSubs(augSubmissionDf, spark)

    //Comments
    //------------------------------------------------------

    val junComments = spark.read.parquet("/555/tp/2016-06-parquet")
    val julComments = spark.read.parquet("/555/tp/2016-07-parquet")
    val augComments = spark.read.parquet("/555/tp/2016-08-parquet")

    val junComPrep = prepComments(junComments, spark)
    val julComPrep = prepComments(julComments, spark)
    val augComPrep = prepComments(augComments, spark)

    //Graph Prep per month
    //------------------------------------------------------
    import spark.implicits._

    val junComAuth = junComPrep.select($"auth_id")
    val julComAuth = julComPrep.select($"auth_id")
    val augComAuth = augComPrep.select($"auth_id")

    val junSubAuth = junSubPrep.select($"auth_id")
    val julSubAuth = julSubPrep.select($"auth_id")
    val augSubAuth = augSubPrep.select($"auth_id")

    val junAllAuth = junComAuth.union(junSubAuth).distinct()
    val julAllAuth = julComAuth.union(julSubAuth).distinct()
    val augAllAuth = augComAuth.union(augSubAuth).distinct()

    val junSubs = junSubPrep.withColumnRenamed("auth_id", "op_id")
    val julSubs = julSubPrep.withColumnRenamed("auth_id", "op_id")
    val augSubs = augSubPrep.withColumnRenamed("auth_id", "op_id")

    val junSubCom = junComPrep.join(junSubs, "link_hash")
    val julSubCom = julComPrep.join(julSubs, "link_hash")
    val augSubCom = augComPrep.join(augSubs, "link_hash")

    val junVertices = junAllAuth.withColumnRenamed("auth_id", "id")
    val julVertices = julAllAuth.withColumnRenamed("auth_id", "id")
    val augVertices = augAllAuth.withColumnRenamed("auth_id", "id")

    val junEdges = junSubCom.select("op_id", "auth_id").withColumnRenamed("op_id", "dst").withColumnRenamed("auth_id", "src")
    val julEdges = julSubCom.select("op_id", "auth_id").withColumnRenamed("op_id", "dst").withColumnRenamed("auth_id", "src")
    val augEdges = augSubCom.select("op_id", "auth_id").withColumnRenamed("op_id", "dst").withColumnRenamed("auth_id", "src")

    val junGraph = GraphFrame(junVertices, junEdges)
    val julGraph = GraphFrame(julVertices, julEdges)
    val augGraph = GraphFrame(augVertices, augEdges)

    //Graphing operations

    val junSCC = junGraph.stronglyConnectedComponents.maxIter(2).run()
    val julSCC = julGraph.stronglyConnectedComponents.maxIter(2).run()
    val augSCC = augGraph.stronglyConnectedComponents.maxIter(2).run()

    val junOSCC = junSCC.groupBy("component").count().orderBy(desc("count")).show(20)
    val julOSCC = julSCC.groupBy("component").count().orderBy(desc("count")).show(20)
    val augOSCC = augSCC.groupBy("component").count().orderBy(desc("count")).show(20)

    //get the most connected author per month based on the results
    junComPrep.filter($"auth_id" === -2147158609).show()
    julComPrep.filter($"auth_id" === -2146642491).show()
    augComPrep.filter($"auth_id" === -2146642491).show()

    val summerVertices = junVertices.union(julVertices).union(augVertices)
    val summerEdges = junEdges.union(julEdges).union(augEdges)

    val summerGraph = GraphFrame(summerVertices, summerEdges)

    val summerSCC = summerGraph.stronglyConnectedComponents.maxIter(2).run()
    val summerOSCC = summerSCC.groupBy("component").count().orderBy(desc("count")).show(100)

    val summerPR = summerGraph.pageRank.maxIter(10).run()
    summerPR.vertices.write.parquet("/555/tp/output/pageRankVertices")
    summerPR.edges.write.parquet("/555/tp/output/pageRankEdges")
    summerPR.vertices.orderBy(desc("pagerank")).show(20)

    val summerTriangles = summerGraph.triangleCount.run()
    summerTriangles.write.parquet("/555/tp/output/triangles")

    //Summer mapreduce prep for ((commenter,OP), count) to see who's commenting most often in a user's threads.

    val junSubsAuth = junSubPrep.withColumnRenamed("author", "op")
    val julSubsAuth = julSubPrep.withColumnRenamed("author", "op")
    val augSubsAuth = augSubPrep.withColumnRenamed("author", "op")

    val junSubComAuth = junComPrep.join(junSubsAuth, "link_hash")
    val julSubComAuth = julComPrep.join(julSubsAuth, "link_hash")
    val augSubComAuth = augComPrep.join(augSubsAuth, "link_hash")

    val summerSubsAuth = junSubsAuth.union(julSubsAuth).union(augSubsAuth)
    val summerComs = junComPrep.union(julComPrep).union(augComPrep)

    val summerSubsComsJoin = summerSubsAuth.join(summerComs, "link_hash")
    val summerAuthEdges = summerSubsComsJoin.withColumn("srcAuth_dstAuth", concat($"author", lit(","), $"op")).select($"srcAuth_dstAuth")

    val summerMR = summerAuthEdges.rdd.map(authors => (authors.getString(0), 1)).reduceByKey(_ + _)
      .map(pair => pair.swap).sortByKey(false).map(reversePair => reversePair.swap)

    summerMR.saveAsTextFile("/555/tp/output/summerMR.txt")
  }

}

