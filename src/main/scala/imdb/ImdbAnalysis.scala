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
  //val titleRatingsList: List[TitleRatings] = ImdbData.readFile(ImdbData.titleRatingsPath, ImdbData.parseTitleRatings _);

  // Hint: use a combination of `ImdbData.titleCrewPath` and `ImdbData.parseTitleCrew`
  //val titleCrewList: List[TitleCrew] = ImdbData.readFile(ImdbData.titleCrewPath, ImdbData.parseTitleCrew _);

  // Hint: use a combination of `ImdbData.nameBasicsPath` and `ImdbData.parseNameBasics`
  //val nameBasicsList: List[NameBasics] = ImdbData.readFile(ImdbData.nameBasicsPath, ImdbData.parseNameBasics _);

  def task1(list: List[TitleBasics]): List[(Float, Int, Int, String)] = {
    //val temp = list.last();
    //val temp2 = temp.flatMap(x => x.genres)
    //val temp3 = temp2.groupBy(i => i)
    //println(temp3)
    // if genres / runtimeMinutes is None, then we skip that row
    // let's do a groupby according to genres
    // where we also store the count, max, min and total somewhere
    // at the end compute max, min, average
    // and return for each genre (key)
    // first a flatmap then a groupby
    // val temp = list.flatMap(x => x.lift(0))
    val result = list.flatMap(aFunction _)
    // println(result)
    val temp2 = result.groupBy(x => x._1).map { case (k,v) => (k,v.map(_._2))}
    // println(temp2)
    temp2.map({ case (k,v) => (v.sum.toFloat/v.size, v.min, v.max, k)}).toList
  }

  def task2(l1: List[TitleBasics], l2: List[TitleRatings]): List[String] = {
    Nil;
    // First join titlebasics and titleratings
    // then do a filter based on the conditions
    // return the original title
    
  }

  def task3(l1: List[TitleBasics], l2: List[TitleRatings]): List[(Int, String, String)] = {
    Nil;
  }

  // Hint: There could be an input list that you do not really need in your implementation.
  def task4(l1: List[TitleBasics], l2: List[TitleCrew], l3: List[NameBasics]): List[(String, Int)] = {
    Nil;
  }

  def aFunction(x: TitleBasics) = {
    // we need to somehow skip the None
    if (x.genres != None && x.runtimeMinutes != None) {
      val temp = x.genres.get
      temp.zip(List.fill(temp.length)(x.runtimeMinutes.get))
    } else {
      List()
    }
  }

  def main(args: Array[String]) {
    println(titleBasicsList.last)
    println(titleBasicsList.head)
    val temp = titleBasicsList.last;
    val copy = titleBasicsList.last;
    val dataset = List(temp,copy)
    val result = dataset.flatMap(aFunction _)
    println(result)
    val temp2 = result.groupBy(x => x._1).map { case (k,v) => (k,v.map(_._2))}
    println(temp2)
    val temp3 = temp2.map { case (k,v) => (v.sum.toFloat/v.size, v.min, v.max, k)}
    println(temp3)
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
