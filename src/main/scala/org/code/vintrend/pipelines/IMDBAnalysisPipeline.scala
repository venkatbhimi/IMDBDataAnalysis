package org.code.vintrend.pipelines

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.code.vintrend.sources.{SourceLocations, TSVFileReader}

/**
  * Created by "venkatbhimi" on 04/10/2019.
  */
class IMDBAnalysisPipeline(spark:SparkSession, var properties: Map[String, String]) extends SourceLocations {

  import spark.implicits._

  val tSVFileReader = TSVFileReader(spark)

  val windowSpecRating: WindowSpec = Window.orderBy($"rating".desc)
  val windowavg: WindowSpec = Window.partitionBy($"titleType").orderBy($"averageRating".asc)

  /**
    * movie is the title type used to filter from whole IMDB data set, this can be changed by passing a parameter.
    *
    *     example => titleType = "movie"
    *     exam[le => topXRanks = 20
    *
    * @param titleType
    * @param topXRanks
    */
  def runPipeline(titleType: String, topXRanks:Int) :DataFrame = {

    val movieTitlesWithNames      :DataFrame  =  getTitleNamesWithTitleTypes(titleType, tSVFileReader.readPath(titleNames))
    val titilesWithAvgRatings     :DataFrame  =  getTitlesWithAvgRatings(tSVFileReader.readPath(pathtitleratings))
    val titlesWithtopRanks        :DataFrame  =  getTitlesWithAvgRankings(movieTitlesWithNames,titilesWithAvgRatings, topXRanks).cache // caching here as its a small data set of 20 rows

    val titlesWithtopRanksWithCrew : DataFrame              = getCrewDetails(titlesWithtopRanks, tSVFileReader.readPath(pathcrew)).cache // caching here as its a small data set of 20 rows
    val titlesWithtopRanksWithCrewWithNames :DataFrame      = getTitlesWithCrewNames(titlesWithtopRanksWithCrew, tSVFileReader.readPath(pathnames)).drop("dirdeathYear", "endYear").cache()
    val titlesWithtopRanksWithCrewWithNamesWithPrevTitles   = getTitlesWithCrewNamesWithPrevTitlesNames(titlesWithtopRanksWithCrewWithNames, movieTitlesWithNames)
    val titlesWithtopRanksWithCrewWithNamesWithPrevTitlesAdult   = getCleanupNulls(titlesWithtopRanksWithCrewWithNamesWithPrevTitles)

    val finalDataSet = titlesWithtopRanksWithCrewWithNamesWithPrevTitlesAdult.select(selectOrderOfCols:_*)

    finalDataSet.orderBy($"rank".asc)
  }

  /**
    * Selecting order of the columns
    *
    * @return
    */
  def selectOrderOfCols = {
    val seq = Seq("rank", "primaryTitle" , "originalTitle" , "dirName" ,"writerName" , "isAdult" , "startYear" , "runtimeMinutes" , "genere" , "averageRating" , "numVotes" , "writerKownTitles" ,"directorKownTitles")
    seq.map(x => col(x))
    seq.map(name => {
      if( name == "genres")
        col(name).as(s"$name".mkString(","))
      else
        col(name).as(s"$name")
    })
  }

  def convertToString(col:Column) = concat(lit("["), concat_ws(",", col), lit("]"))
  /**
    * Adding Adult Desctiption
    *
    * @param titlesWithtopRanksWithCrewWithNamesWithPrevTitles
    * @return
    */
  def getCleanupNulls(titlesWithtopRanksWithCrewWithNamesWithPrevTitles:DataFrame): DataFrame = {
    titlesWithtopRanksWithCrewWithNamesWithPrevTitles.withColumnRenamed("isAdult", "adultFlg")
      .withColumn("isAdult", when($"adultFlg" === 1, "Yes").otherwise("No")).drop("adultFlg")
      .withColumn("genere", convertToString($"genres")).drop("genres")
      .withColumn("writerKownTitles",when($"writPrvKownTitles".isNull, "").otherwise($"writPrvKownTitles")).drop("writPrvKownTitles")
      .withColumn("directorKownTitles",when($"dirPrvKownTitles".isNull, "").otherwise($"dirPrvKownTitles")).drop("dirPrvKownTitles")
  }
  /**
    * This is to process known titles for writers and directors , as this codes comes in array of strings,
    * the results should be as collection of titles for both the fields.
    *
    * @param titlesWithtopRanksWithCrewWithNames
    * @param movieTitlesWithNames
    * @return
    */
  def getTitlesWithCrewNamesWithPrevTitlesNames(titlesWithtopRanksWithCrewWithNames:DataFrame, movieTitlesWithNames:DataFrame) = {

    val prevTitlesForWriter = {
      val writPrevTitles = titlesWithtopRanksWithCrewWithNames.select("tconst","writPrevTitles").filter($"writPrevTitles".isNotNull).filter($"writPrevTitles" !== Array("")).withColumnRenamed("tconst", "titleId")
      writPrevTitles.join(titlesWithtopRanksWithCrewWithNames.drop("writPrevTitles"), $"writPrevTitles".cast("String").contains($"tconst".cast("String")))
        .select("titleId", "primaryTitle")
        .withColumnRenamed("primaryTitle", "writPrvKownTitles").rdd
        .map(x => (x.getString(0), x.getString(1)))
        .reduceByKey(_+" , "+_).toDF("titleId","writPrvKownTitles")
    }

    val titlesWithtopRanksWithCrewWithNamesWithWritPrvTitles = titlesWithtopRanksWithCrewWithNames.join(prevTitlesForWriter, $"tconst" === $"titleId","left").drop("titleId", "writPrevTitles")

    val prevTitlesForDirector = {
      val writPrevTitles = titlesWithtopRanksWithCrewWithNames.select("tconst","dirPrevTitles").filter($"dirPrevTitles".isNotNull).filter($"dirPrevTitles" !== Array("")).withColumnRenamed("tconst", "titleId")
      writPrevTitles.join(titlesWithtopRanksWithCrewWithNames.drop("dirPrevTitles"), $"dirPrevTitles".cast("String").contains($"tconst".cast("String")))
        .select("titleId", "primaryTitle")
        .withColumnRenamed("primaryTitle", "dirPrvKownTitles").rdd
        .map(x => (x.getString(0), x.getString(1)))
        .reduceByKey(_+" , "+_).toDF("titleId","dirPrvKownTitles")
    }

    val titlesWithtopRanksWithCrewWithNamesWithWritDirPrvTitles = titlesWithtopRanksWithCrewWithNamesWithWritPrvTitles.join(prevTitlesForDirector, $"tconst" === $"titleId","left").drop("titleId", "dirPrevTitles")
    titlesWithtopRanksWithCrewWithNamesWithWritDirPrvTitles.drop("tconst", "titleType")
  }
  /**
    *
    * @param titlesWithtopRanksWithCrew
    * @return
    */
  def getTitlesWithCrewNames(titlesWithtopRanksWithCrew:DataFrame, df:DataFrame) :DataFrame = {
    val dfWithNames = tSVFileReader.readingCrewNames(df)

    val df_with_dirs = {
      dfWithNames.join(broadcast(titlesWithtopRanksWithCrew), $"dir" === $"nconst", "right")
        .withColumnRenamed("primaryName", "dirName")
        .withColumnRenamed("birthYear", "dirBYear")
        .withColumnRenamed("profession", "dirProf")
        .withColumnRenamed("knownTitles", "dirPrevTitles")
        .withColumnRenamed("deathYear", "dirdeathYear")
        .drop("nconst", "dir", "dirBYear", "dirProf")
    }
    val df_with_dirs_writers = {
      dfWithNames.join(broadcast(df_with_dirs), $"writ" === $"nconst", "right")
        .withColumnRenamed("primaryName", "writerName")
        .withColumnRenamed("birthYear", "writerBirthYear")
        .withColumnRenamed("profession", "writerProfession")
        .withColumnRenamed("knownTitles", "writPrevTitles")
        .drop("nconst","writ","writerBirthYear",  "deathYear","writerProfession")
    }
    df_with_dirs_writers
  }

  /**
    * joining the top ranked titles with crew dataset to get the creditors details.
    *
    * Here the resulting data set only gets the directors/writers with name codes.
    *
    * @param titlesWithtopRanks
    * @return
    */
  def getCrewDetails(titlesWithtopRanks:DataFrame, df:DataFrame) = {
    val df_withCrew    = tSVFileReader.readTitleCrewDataSet(df)
    val titlesWithCrew = df_withCrew.join(broadcast(titlesWithtopRanks), $"tconst" === $"tit").drop("tit")
    titlesWithCrew
  }
  /**
    * finding the top 20 ratings movies
    *
    * averageNumberOfVotes - calculated with a window of partitioning on titletype
    * for sytanx purpose, as we need average of total numVotes for all the titles
    *
    * rating - based on the formula (numVotes/averageNumberOfVotes) * averageRating
    *
    * rank - used row_number() function over a window with order of rating and averageNumberOfVotes
    *
    * @param movieTitlesWithNames
    * @param titilesWithAvgRatings
    * @return
    */
  def getTitlesWithAvgRankings(movieTitlesWithNames:DataFrame ,titilesWithAvgRatings:DataFrame, topRank:Int):DataFrame = {

    val joinedtitlesWithAvgRatings = movieTitlesWithNames.join(titilesWithAvgRatings, $"tconst" === $"titleID").drop("titleID")

    val df_WithRankings =
      joinedtitlesWithAvgRatings
        .withColumn("averageNumberOfVotes", bround(avg($"numVotes").over(windowavg),2))
        .withColumn("rating", bround(($"numVotes"/$"averageNumberOfVotes") * $"averageRating",2))
        .withColumn("rank", row_number().over(windowSpecRating))
    df_WithRankings.drop("averageNumberOfVotes", "rating").filter($"rank" <= topRank)
  }

  /**
    * this method reads and validates the data of tiles-ratings data set.
    * As the ranking only considring the titles with greater than 50 votes,
    * applying filter with numVotes > 50
    *
    * @return
    */
  def getTitlesWithAvgRatings(df:DataFrame) :DataFrame = {
    val df_WithTitleRatings       = tSVFileReader.readTitleRatings(df)
    val titlesWithMoreThan50Votes = df_WithTitleRatings.filter($"numVotes" > 50 )
    titlesWithMoreThan50Votes.drop("titleType")
  }

  /**
    *
    * @param titleType
    * @return
    */
  def getTitleNamesWithTitleTypes(titleType:String, df:DataFrame) = {
    val df_titlesnames        = tSVFileReader.readTitleNames(df)
    val titleNamesWithMovies  =  df_titlesnames.filter($"titleType" === titleType).withColumnRenamed("tconst", "titleID")
    titleNamesWithMovies.withColumnRenamed("tconst", "titleID")
  }

}

object IMDBAnalysisPipeline {
  def apply(spark: SparkSession, properties: Map[String, String] ): IMDBAnalysisPipeline = new IMDBAnalysisPipeline(spark, properties)
}
