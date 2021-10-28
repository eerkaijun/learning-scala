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
    // First join titlebasics and titleratings
    // then do a filter based on the conditions
    // return the original title
    val l1_filtered = l1.filter({ case x => 
      x.titleType != None && x.titleType.get == "movie" && 
      x.primaryTitle != None && 
      x.startYear != None && x.startYear.get >= 1990 && 
      x.startYear.get <= 2018})
    val l2_filtered = l2.filter({ case y =>
      y.averageRating >= 7.5 && y.numVotes >= 500000})
    // l1_filtered.map({ case x => 
    //   l2_filtered.filter({ case y => y.tconst == x.tconst})
    //   .map({ case x => x.primaryTitle.get})})
    l1_filtered.flatMap(x => l2_filtered.map(y => (x,y))).filter{ case (x,y) => x.tconst == y.tconst}
    .map{ case(x,y) => x.primaryTitle.get}

    
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
    .map{ case(k,v) => (k,v.flatMap(x => x._3.zip(List.fill(x._3.length)(x._2,x._4))))}
    .map{ case(k,v) => (k,v.groupBy(_._1).map{ case (k,v) => (k, v.map(_._2).maxBy(_._2))})}
    .flatMap{ case(k,v) => v.map{ case(genre, tuple) => (k,genre,tuple._1)}}.toList.sorted
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
    /*
    val l1_filtered = titleBasicsList.filter({ case x => 
      x.titleType != None && x.titleType.get == "movie" && 
      x.primaryTitle != None && x.genres != None &&
      x.startYear != None && x.startYear.get >= 1900 && 
      x.startYear.get <= 1909})
    val temp = l1_filtered.flatMap(x => titleRatingsList.map(y => (x,y))).filter{ case (x,y) => x.tconst == y.tconst}
    .map{ case(x,y) => (x.primaryTitle.get, x.genres.get, y.averageRating)}
    //val temp = l1_filtered.filter(x => x.startYear.get == 1950)
    //val temp2 = temp.flatMap(x => x.genres.get.zip(List.fill(temp.length)(x.primaryTitle.get, x.tconst)))
    //println(temp2)
    //val temp3 = temp2.groupBy(x => x._1).map { case (k,v) => (k,v.map(_._2))}
    //println(temp)
    // try to have one entry for each genre ie flatmap for genre
    val temp2 = temp.flatMap(x => x._2.zip(List.fill(x._2.length)(x._1, x._3)))
    //println(temp2)
    val temp3 = temp2.groupBy(x => x._1).map{ case (k,v) => (k,v.map(_._2))}
    println(temp3)
    val temp4 = temp3.map{ case(k,v) => (k,v.sorted.maxBy(_._2))}
    println(temp4)
    val temp5 = temp4.map{ case(k,v) => (0, k, v._1)}
    println(temp5)
    */

    /*
    val l1_filtered = titleBasicsList.filter({ case x => 
      x.titleType != None && x.titleType.get == "movie" && 
      x.primaryTitle != None && x.genres != None &&
      x.startYear != None && x.startYear.get >= 1900 && 
      x.startYear.get <= 1999})
    val temp2 = titleRatingsList.map(x => (x.tconst -> x.averageRating)).toMap
    val temp3 = l1_filtered.filter{case x => temp2.contains(x.tconst)}
    println(l1_filtered.length)
    println(temp3.length)
    val temp4 = temp3.map(x => (x.startYear.get, x.primaryTitle.get, x.genres.get, temp2.get(x.tconst).get)).groupBy(x => x._1 / 10 % 10)
    .map{ case(k,v) => (k,v.flatMap(x => x._3.zip(List.fill(x._3.length)(x._2,x._4))))}
    .map{ case(k,v) => (k,v.groupBy(_._1).map{ case (k,v) => (k, v.map(_._2).maxBy(_._2))})}
    .flatMap{ case(k,v) => v.map{ case(genre, tuple) => (k,genre,tuple._1)}}.toList.sorted
    println(temp4)
    //val temp = l1_filtered.flatMap(x => titleRatingsList.map(y => (x,y))).filter{ case (x,y) => x.tconst == y.tconst}
    //  .map{ case(x,y) => (x.startYear.get, x.primaryTitle.get, x.genres.get, y.averageRating)}
    //  .groupBy(x => x._1 / 10 % 10)
    //println(temp)
    */  


    
    // Task 4
    val temp = nameBasicsList.filter{ case x => 
      x.primaryName != None && x.knownForTitles != None &&
      x.knownForTitles.get.length >= 2
    }
    val filtered = titleBasicsList.filter{ case x => 
      x.startYear != None && x.startYear.get >= 2010 && x.startYear.get <= 2021
    }.flatMap(x => List(x.tconst)).toSet
    //println(filtered)
    //val temp2 = temp.flatMap(x => x.knownForTitles.get.zip(List.fill(x.knownForTitles.get.length)(x.primaryName.get, x.nconst)))
    val temp2 = temp.flatMap(x => (x.knownForTitles.get,(List.fill(x.knownForTitles.get.length)(x.primaryName.get)),(List.fill(x.knownForTitles.get.length)(x.nconst))).zipped.toList)
    //println(temp2)
    val temp3 = temp2.filter{case x => filtered.contains(x._1)}
    //println(temp3)
    println(temp3.groupBy(x => x._3).filter{case(k,v) => v.length >= 2}.map{case(k,v)=>(v.head._2,v.length)}.toList)

    //println(l1_filtered.flatMap(x => titleRatingsList.map(y => (x,y))).filter{ case (x,y) => x.tconst == y.tconst})
    //dataset1.flatMap(x => dataset2.map(y => (x,y))).filter({ case ((a, _, _, _, _), (b, _)) => a == b })
    

    /*
    val dataset = List(temp,copy)
    val result = dataset.flatMap(aFunction _)
    println(result)
    val temp2 = result.groupBy(x => x._1).map { case (k,v) => (k,v.map(_._2))}
    println(temp2)
    val temp3 = temp2.map { case (k,v) => (v.sum.toFloat/v.size, v.min, v.max, k)}
    println(temp3)*/

    // val durations = timed("Task 1", task1(titleBasicsList))
    // val titles = timed("Task 2", task2(titleBasicsList, titleRatingsList))
    // val topRated = timed("Task 3", task3(titleBasicsList, titleRatingsList))
    // val crews = timed("Task 4", task4(titleBasicsList, titleCrewList, nameBasicsList))
    // println(durations)
    // println(titles)
    // println(topRated)
    // println(crews)
    // println(timing)
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
