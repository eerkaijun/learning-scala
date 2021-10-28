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
  val titleCrewList: List[TitleCrew] = ImdbData.readFile(ImdbData.titleCrewPath, ImdbData.parseTitleCrew _);

  // Hint: use a combination of `ImdbData.nameBasicsPath` and `ImdbData.parseNameBasics`
  val nameBasicsList: List[NameBasics] = ImdbData.readFile(ImdbData.nameBasicsPath, ImdbData.parseNameBasics _);

  def task1(list: List[TitleBasics]): List[(Float, Int, Int, String)] = {
    list.filter{ x => x.genres != None && x.runtimeMinutes != None}
    .flatMap{ case x => x.genres.get.zip(List.fill(x.genres.get.length)(x.runtimeMinutes.get))}
    .groupBy(x => x._1).map { case (k,v) => (k,v.map(_._2))}
    .map({ case (k,v) => (v.sum.toFloat/v.size, v.min, v.max, k)}).toList
  }

  def task2(l1: List[TitleBasics], l2: List[TitleRatings]): List[String] = {
    // First filter based on the conditions
    // then join the 2 datasets together that share the same tconst
    // return the original title
    val l1_filtered = l1.filter({ case x => 
      x.titleType != None && x.titleType.get == "movie" && 
      x.primaryTitle != None && 
      x.startYear != None && x.startYear.get >= 1990 && 
      x.startYear.get <= 2018})
    val l2_filtered = l2.filter({ case y =>
      y.averageRating >= 7.5 && y.numVotes >= 500000})
      .map(y => y.tconst).toSet
    l1_filtered.filter{ case x =>  l2_filtered.contains(x.tconst)}
    .map{ case x => x.primaryTitle.get}
  }

  def task3(l1: List[TitleBasics], l2: List[TitleRatings]): List[(Int, String, String)] = {
    val l2_map = l2.map(x => (x.tconst -> x.averageRating)).toMap
    val l1_filtered = l1.filter({ case x => 
      x.titleType != None && x.titleType.get == "movie" && 
      x.primaryTitle != None && x.genres != None &&
      x.startYear != None && x.startYear.get >= 1900 && 
      x.startYear.get <= 1999})
    .filter{case x => l2_map.contains(x.tconst)}
    l1_filtered.map(x => (x.startYear.get, x.primaryTitle.get, x.genres.get, l2_map.get(x.tconst).get)).groupBy(x => x._1 / 10 % 10)
    .map{ case(k,v) => (k,v.flatMap(x => (x._3,(List.fill(x._3.length)(x._2)),(List.fill(x._3.length)(x._4))).zipped.toList))}
    .map{ case(k,v) => (k,v.groupBy(_._1).map{ case (k,v) => (k, v.sorted.maxBy(_._3))})}
    .flatMap{ case(k,v) => v.map{ case(genre, tuple) => (k,genre,tuple._2)}}.toList.sorted
  }

  // Hint: There could be an input list that you do not really need in your implementation.
  def task4(l1: List[TitleBasics], l2: List[TitleCrew], l3: List[NameBasics]): List[(String, Int)] = {
    val l3_filtered = l3.filter{ case x => 
      x.primaryName != None && x.knownForTitles != None &&
      x.knownForTitles.get.length >= 2
    }
    val l1_filtered = l1.filter{ case x => 
      x.startYear != None && x.startYear.get >= 2010 && x.startYear.get <= 2021
    }.flatMap(x => List(x.tconst)).toSet
    l3_filtered.flatMap(x => (x.knownForTitles.get,(List.fill(x.knownForTitles.get.length)(x.primaryName.get)),(List.fill(x.knownForTitles.get.length)(x.nconst))).zipped.toList)
    .filter{ case x => l1_filtered.contains(x._1)}
    .groupBy(x => x._3)
    .filter{case(k,v) => v.length >= 2}
    .map{case(k,v)=>(v.head._2,v.length)}.toList
  }

  def main(args: Array[String]) {
    val durations = timed("Task 1", task1(titleBasicsList))
    val titles = timed("Task 2", task2(titleBasicsList, titleRatingsList))
    val topRated = timed("Task 3", task3(titleBasicsList, titleRatingsList))
    val crews = timed("Task 4", task4(titleBasicsList, titleCrewList, nameBasicsList))
    println(durations)
    println(titles)
    println(topRated)
    println(crews)
    println(timing)
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
