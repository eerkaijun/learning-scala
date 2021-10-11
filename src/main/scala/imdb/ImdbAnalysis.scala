package imdb

case class TitleBasics(tconst: String, titleType: Option[String], primaryTitle: Option[String],
                      originalTitle: Option[String], isAdult: Int, startYear: Option[Int], endYear: Option[Int],
                      runtimeMinutes: Option[Int], genres: Option[List[String]])
case class TitleRatings(tconst: String, averageRating: Float, numVotes: Int)
case class TitleCrew(tconst: String, directors: Option[List[String]], writers: Option[List[String]])
case class NameBasics(nconst: String, primaryName: Option[String], birthYear: Option[Int], deathYear: Option[Int],
                      primaryProfession: Option[List[String]], knownForTitles: Option[List[String]])

object ImdbAnalysis {

  // Hint: use a combination of `ImdbData.titleBasicsPath` and `ImdbData.parseTitleBasics`
  val titleBasicsList: List[TitleBasics] = ImdbData.readFile(ImdbData.titleBasicsPath, ImdbData.parseTitleBasics _);

  // Hint: use a combination of `ImdbData.titleRatingsPath` and `ImdbData.parseTitleRatings`
  val titleRatingsList: List[TitleRatings] = ImdbData.readFile(ImdbData.titleRatingsPath, ImdbData.parseTitleRatings _);

  // Hint: use a combination of `ImdbData.titleCrewPath` and `ImdbData.parseTitleCrew`
  val titleCrewList: List[TitleCrew] = ImdbData.readFile(ImdbData.titleCrewPath, ImdbData.parseTitleCrew);

  // Hint: use a combination of `ImdbData.nameBasicsPath` and `ImdbData.parseNameBasics`
  val nameBasicsList: List[NameBasics] = ImdbData.readFile(ImdbData.nameBasicsPath, ImdbData.parseNameBasics);

  def task1(list: List[TitleBasics]): List[(Float, Int, Int, String)] = {
    Nil;
  }

  def task2(l1: List[TitleBasics], l2: List[TitleRatings]): List[String] = {
    Nil;
  }

  def task3(l1: List[TitleBasics], l2: List[TitleRatings]): List[(Int, String, String)] = {
    Nil;
  }

  // Hint: There could be an input list that you do not really need in your implementation.
  def task4(l1: List[TitleBasics], l2: List[TitleCrew], l3: List[NameBasics]): List[(String, Int)] = {
    Nil;
  }

  def main(args: Array[String]) {
    println(titleBasicsList.lift(1))
    println(titleRatingsList.lift(1))
    println(titleCrewList.lift(1))
    println(nameBasicsList.lift(1))
    //val durations = timed("Task 1", task1(titleBasicsList))
    //val titles = timed("Task 2", task2(titleBasicsList, titleRatingsList))
    //val topRated = timed("Task 3", task3(titleBasicsList, titleRatingsList))
    //val crews = timed("Task 4", task4(titleBasicsList, titleCrewList, nameBasicsList))
    //println(durations)
    //println(titles)
    //println(topRated)
    //println(crews)
    //println(timing)
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}