package org.code.vintrend.sources

import org.apache.spark.sql.{DataFrame,SparkSession}
import org.code.vintrend.util.DataValidator

/**
  * Created by "venkatbhimi" on 04/10/2019.
  */
class TSVFileReader(spark: SparkSession) extends DataValidator with Serializable {

  import spark.implicits._

  def readPath(path:String): DataFrame = {
    spark.read.format("csv").option("header", "true").load(path)
  }

  /**
    * data from
    * title.crew.tsv.gz – Contains the director and writer information for all the titles in IMDb. Fields include
    * tconst (string) - alphanumeric unique identifier of the title
    * directors (array of nconsts) - director(s) of the given title
    * writers (array of nconsts) – writer(s) of the given title
    */
    def readTitleCrewDataSet(df_tit_crew:DataFrame): DataFrame = {
    df_tit_crew.map(x => {
      checkTitleCrewData(x.getString(0).split("\t"))
    }).toDF("tit", "dir", "writ")
  }

  /**
    * title.basics.tsv.gz - Contains the following information for titles:
    * tconst (string) - alphanumeric unique identifier of the title
    * titleType (string) – the type/format of the title (e.g. movie, short, tvseries, tvepisode, video, etc)
    * primaryTitle (string) – the more popular title / the title used by the filmmakers on promotional materials at the point of release
    * originalTitle (string) - original title, in the original language
    * isAdult (boolean) - 0: non-adult title; 1: adult title
    * startYear (YYYY) – represents the release year of a title. In the case of TV Series, it is the series start year
    * endYear (YYYY) – TV Series end year. ‘\N’ for all other title types
    * runtimeMinutes – primary runtime of the title, in minutes
    * genres (string array) – includes up to three genres associated with the title
    */
  def readTitleNames(df_titles:DataFrame) :DataFrame = {
     val df_titlesnames =  df_titles.mapPartitions( partition =>
      partition.map(row => {
        checkDataTitles(row.getString(0).split("\t"))
      } )
    ).toDF("tconst", "titleType", "primaryTitle", "originalTitle", "isAdult", "startYear", "endYear", "runtimeMinutes", "genres")

    df_titlesnames
  }

  /**
    * data from
    * title.ratings.tsv.gz – Contains the IMDb rating and votes information for titles
    * tconst (string) - alphanumeric unique identifier of the title
    * averageRating – weighted average of all the individual user ratings
    * numVotes - number of votes the title has received
    */

    def readTitleRatings(df_tit_ratings:DataFrame) : DataFrame = {
    val df_WithSchema = df_tit_ratings.map(x => checTitleRatings(x.getString(0).split("\t"))).toDF("tconst","averageRating", "numVotes" )
    df_WithSchema
  }

  /**
    * name.basics.tsv.gz – Contains the following information for names:
    * nconst (string) - alphanumeric unique identifier of the name/person
    * primaryName (string)– name by which the person is most often credited
    * birthYear – in YYYY forma
    * deathYear – in YYYY format if applicable, else '\N'
    * primaryProfession (array of strings)– the top-3 professions of the person
    * knownForTitles (array of tconsts) – titles the person is known for
    */
    def readingCrewNames(df_names:DataFrame): DataFrame = {
    val df_nameswithtitles = df_names.map(x => {
      checkDataNames(x.getString(0).split("\t"))
    }).toDF("nconst", "primaryName", "birthYear", "deathYear", "profession", "knownTitles")
    df_nameswithtitles
  }

}

object TSVFileReader {
  def apply(spark: SparkSession): TSVFileReader = new TSVFileReader(spark)
}