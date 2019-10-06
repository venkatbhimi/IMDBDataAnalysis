package org.code.vintrend.util


import org.apache.log4j.{Level, Logger}
import org.scalatest.{FunSuite, Matchers, WordSpec}

/**
  * Created by "venkatbhimi" on 05/10/2019.
  */
class DataValidatorTest extends FunSuite with Matchers with DataValidator {

 test ("checTitleRatings validations")  {
   assert(checTitleRatings(Array("tt0000001"))._1  === "tt0000001")
   assert(checTitleRatings(Array("tt0000001", "5.6"))._2  === 5.6)
   assert(checTitleRatings(Array("tt0000001", "5.6", "1539"))._3 === 1539)
   assert(checTitleRatings(Array("tt0000001", "1539", "1539")).productArity === 3)
  }

  test("checkTitleCrewData validations")    {
    assert(checkTitleCrewData(Array("tt0000009"))._1  === "tt0000009")
    assert(checkTitleCrewData(Array("", "nm0085156"))._2  === "nm0085156")
    assert(checkTitleCrewData(Array("", "", "nm0085156"))._3 === "nm0085156")
    assert(checkTitleCrewData(Array("", "", "nm0085156")).productArity === 3)
  }

  test("checkDataTitles validations")   {
    assert(checkDataTitles(Array("tconst"))._1  === "tconst")
    assert(checkDataTitles(Array("", "movie"))._2  === "movie")
    assert(checkDataTitles(Array("", "", "Joker"))._3 === "Joker")
    assert(checkDataTitles(Array("", "", "", "war"))._4 === "war")
    assert(checkDataTitles(Array("", "", "", "", "No"))._5 === "No")
    assert(checkDataTitles(Array("", "", "", "", "", "2018"))._6 === "2018")
    assert(checkDataTitles(Array("", "", "", "", "", "", "\\N"))._7 === "\\N" )
    assert(checkDataTitles(Array("", "", "", "", "", "", "", "150" ))._8 === "150" )
    assert(checkDataTitles(Array("", "", "", "", "", "", "", "", "Action"))._9 ===  Array("Action"))
    assert(checkDataTitles(Array("", "", "", "", "", "", "", "", Array("Drama")))._9  === Array("Drama"))
    assert(checkDataTitles(Array("", "", "", "Dark Night")).productArity === 9)
  }

  test("checkDataNames validations")   {
    assert(checkDataNames(Array("t1234"))._1  === "t1234")
    assert(checkDataNames(Array("", "Fred Astaire"))._2  === "Fred Astaire")
    assert(checkDataNames(Array("", "", "1924"))._3 === "1924")
    assert(checkDataNames(Array("", "", "", "\\N"))._4 === "\\N")
    assert(checkDataNames(Array("", "", "", "", "actress"))._5 === Array("actress"))
    assert(checkDataNames(Array("", "", "", "", "", Array("")))._6 === Array(""))
    assert(checkDataNames(Array("", "", "", "", "Actor", Array("")))._5 ===  Array("Actor"))
    assert(checkDataNames(Array("", "", "", "", "", Array("")))._6  === Array(""))
    assert(checkDataNames(Array("", "", "", "1980")).productArity === 6)
  }

}
