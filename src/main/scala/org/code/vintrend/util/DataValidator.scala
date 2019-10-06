package org.code.vintrend.util

/**
  * Created by "venkatbhimi" on 04/10/2019.
  *
  * this trait is to validated and split the data, as data is in single
  * string of each line with separation of columns with tab spaces.
  *
  */
trait DataValidator extends  Serializable {

  /**
    *
     * @param arrData
    * @return
    */
  def checTitleRatings(arrData: Array[_<:Any]) : (String, Double, Int) ={
    arrData match {
      case Array(tconst:String) => (tconst, 0.0 , 0)
      case Array(tconst:String, averageRating:String) => (tconst, averageRating.toDouble, 0)
      case Array(tconst:String, averageRating:String, numVotes:String) => (tconst, averageRating.toDouble, numVotes.toInt)
    }
  }

  /**
    *
    * @param arrData
    * @return
    */
  def checkTitleCrewData(arrData: Array[_<:AnyRef]): (String, String, String) = {
    arrData match {
      case Array(tit:String) => (tit, "", "")
      case Array(tit:String, dir:String) => (tit, dir, "")
      case Array(tit:String, dir:String, writ:String) => (tit, dir, writ)
    }
  }

  /**
    *
    * @param arrData
    * @return
    */
  def checkDataTitles(arrData: Array[_<:AnyRef]): (String, String, String,String, String, String, String,String, Array[String]) = {
    arrData match {
      case Array(tconst:String) => (tconst, "", "", "", "", "", "", "", Array(""))
      case Array(tconst:String, titleType:String) => (tconst, titleType, "", "", "", "", "", "", Array(""))
      case Array(tconst:String, titleType:String, primaryTitle:String) => (tconst, titleType, primaryTitle, "", "", "", "", "", Array(""))
      case Array(tconst:String, titleType:String, primaryTitle:String, originalTitle:String) => (tconst, titleType, primaryTitle, originalTitle, "", "", "", "", Array(""))
      case Array(tconst:String, titleType:String, primaryTitle:String, originalTitle:String, isAdult:String) => (tconst, titleType, primaryTitle, originalTitle, isAdult, "", "", "", Array(""))
      case Array(tconst:String, titleType:String, primaryTitle:String, originalTitle:String, isAdult:String, startYear:String) =>  (tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, "", "", Array(""))
      case Array(tconst:String, titleType:String, primaryTitle:String, originalTitle:String, isAdult:String, startYear:String, endYear:String) => (tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, "", Array(""))
      case Array(tconst:String, titleType:String, primaryTitle:String, originalTitle:String, isAdult:String, startYear:String, endYear:String, runtimeMinutes:String ) => (tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, Array(""))
      case Array(tconst:String, titleType:String, primaryTitle:String, originalTitle:String, isAdult:String, startYear:String, endYear:String, runtimeMinutes:String, genres:String) =>(tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, Array(genres))
      case Array(tconst:String, titleType:String, primaryTitle:String, originalTitle:String, isAdult:String, startYear:String, endYear:String, runtimeMinutes:String, genres:Array[String] ) => (tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres)
    }
  }

  /**
    *
    * @param arrData
    * @return
    */
  def checkDataNames(arrData: Array[_<:AnyRef]): (String, String, String,String, Array[String], Array[String]) = {
    arrData match {
      case Array(nconst:String ) => (nconst, "", "", "", Array(""),Array(""))
      case Array(nconst:String, pname:String ) => (nconst, pname, "", "", Array(""),Array(""))
      case Array(nconst:String, pname:String, byear:String) => (nconst, pname, byear, "", Array(""), Array(""))
      case Array(nconst:String, pname:String, byear:String, dyear:String) => (nconst, pname, byear, dyear, Array(""), Array(""))
      case Array(nconst:String, pname:String, byear:String, dyear:String, prof:String) => (nconst, pname, byear, dyear, Array(prof), Array(""))
      case Array(nconst:String, pname:String, byear:String, dyear:String,  prof:Array[String]) => (nconst, pname, byear, dyear, prof, Array(""))
      case Array(nconst:String, pname:String, byear:String, dyear:String, prof:String, titles:String) =>  (nconst, pname, byear, dyear, Array(prof), Array(titles))
      case Array(nconst:String, pname:String, byear:String, dyear:String,  prof:Array[String], titles:String) =>(nconst, pname, byear, dyear, prof, Array(titles))
      case Array(nconst:String, pname:String, byear:String, dyear:String,  prof:String, titles:Array[String]) =>(nconst, pname, byear, dyear, Array(prof), titles)
      case Array(nconst:String, pname:String, byear:String, dyear:String,  prof:Array[String], titles:Array[String]) =>(nconst, pname, byear, dyear, prof, titles)
    }
  }

}
