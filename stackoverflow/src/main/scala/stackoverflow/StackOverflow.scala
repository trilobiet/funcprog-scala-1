package stackoverflow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import annotation.tailrec
import scala.reflect.ClassTag

/** A raw stackoverflow posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[QID], score: Int, tags: Option[String]) extends Serializable


/** The main class */
object StackOverflow extends StackOverflow {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines   = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
    val raw     = rawPostings(lines)
    val grouped = groupedPostings(raw)
    val scored  = scoredPostings(grouped)
    val vectors = vectorPostings(scored)
    assert(vectors.count() == 2121822, "Incorrect number of vectors: " + vectors.count())

    val means   = kmeans(sampleVectors(vectors), vectors, debug = true)
    val results = clusterResults(means, vectors)
    printResults(results)
  }
}

/** The parsing and kmeans methods */
class StackOverflow extends StackOverflowInterface with Serializable {

  /** Languages */
  val langs =
    List(
      "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
      "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def langSpread = 50000
  assert(langSpread > 0, "If langSpread is zero we can't recover the language from the input data!")

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120


  //
  //
  // Parsing utilities:
  //
  //

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType =    arr(0).toInt,
        id =             arr(1).toInt,
        acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
        parentId =       if (arr(3) == "") None else Some(arr(3).toInt),
        score =          arr(4).toInt,
        tags =           if (arr.length >= 6) Some(arr(5).intern()) else None)
    })


  /** Group the questions and answers together */
  def groupedPostings(postings: RDD[Posting]): RDD[(QID, Iterable[(Question, Answer)])] = {

    // Get the questions
    val questions: RDD[(QID, Posting)] =
      postings.filter(p => p.postingType == 1).map(p => (p.id, p))
    // Get the answers
    val answers: RDD[(QID, Posting)] =
      postings.filter(p => p.postingType == 2).map(p => (p.parentId.get, p))

    // Join questions & answers
    val j: RDD[(QID, (Question, Answer))] = questions.join(answers)
    val k: RDD[(QID, Iterable[(Question, Answer)])] = j.groupByKey()
    k
  }


  /** Compute the maximum score for each posting */
  def scoredPostings(grouped: RDD[(QID, Iterable[(Question, Answer)])]): RDD[(Question, HighScore)] = {

    def answerHighScore(as: Array[Answer]): HighScore = {
      var highScore = 0
      var i = 0
      while (i < as.length) {
        val score = as(i).score
        if (score > highScore)
          highScore = score
        i += 1
      }
      highScore
    }

    // Step by step explanation of what happens below:
    // 1: remove QID, keep only 2nd part of tuple
    // val q1: RDD[Iterable[(Question, Answer)]] = grouped.map(g => g._2)
    // 2: flatten (RDDs have no flatten method, use identity)
    // val q2: RDD[(Question, Answer)] = q1.flatMap(i=>i)
    // 3: group Answers by key
    // val q3: RDD[(Question, Iterable[Answer])] = q2.groupByKey()
    // 4: replace list of answers with highest score
    // val q4: RDD[(Question, HighScore)] = q3.mapValues(v => answerHighScore(v.toArray))

    /*
    grouped
      .map(g => g._2)
      .flatMap(i=>i)
      .groupByKey()
      .mapValues(v => answerHighScore(v.toArray))
    */

    grouped
      .map(g => g._2)
      .flatMap(i=>i)
      .map{ case(q,a) => (q, a.score) }
      .reduceByKey( _+_ )

  }


  /** Compute the vectors for the kmeans */
  def vectorPostings(scored: RDD[(Question, HighScore)]): RDD[(LangIndex, HighScore)] = {
    /** Return optional index of first language that occurs in `tags`. */
    def firstLangInTag(tag: Option[String], ls: List[String]): Option[Int] = {
      if (tag.isEmpty) None
      else if (ls.isEmpty) None
      else if (tag.get == ls.head) Some(0) // index: 0
      else {
        val tmp = firstLangInTag(tag, ls.tail)
        tmp match {
          case None => None
          case Some(i) => Some(i + 1) // index i in ls.tail => index i+1
        }
      }
    }

    scored.map{
      // map the question to the language tag value which is an Option[Int]
      case(question,highScore) => ( firstLangInTag(question.tags,langs), highScore)
    }.filter{
      // remove rows for which there is no value in optInt
      case(optInt,highScore: HighScore) => optInt.nonEmpty
    }.map{
      // map to the value and apply langSpread
      case(optInt,highScore: HighScore) => (optInt.get * langSpread, highScore)
    }

  }


  /** Sample the vectors */
  def sampleVectors(vectors: RDD[(LangIndex, HighScore)]): Array[(Int, Int)] = {

    assert(kmeansKernels % langs.length == 0, "kmeansKernels should be a multiple of the number of languages studied.")
    val perLang = kmeansKernels / langs.length

    // http://en.wikipedia.org/wiki/Reservoir_sampling
    def reservoirSampling(lang: Int, iter: Iterator[Int], size: Int): Array[Int] = {
      val res = new Array[Int](size)
      val rnd = new util.Random(lang)

      for (i <- 0 until size) {
        assert(iter.hasNext, s"iterator must have at least $size elements")
        res(i) = iter.next
      }

      var i = size.toLong
      while (iter.hasNext) {
        val elt = iter.next
        val j = math.abs(rnd.nextLong) % i
        if (j < size)
          res(j.toInt) = elt
        i += 1
      }

      res
    }

    val res =
      if (langSpread < 500)
      // sample the space regardless of the language
        vectors.takeSample(false, kmeansKernels, 42)
      else
      // sample the space uniformly from each language partition
        vectors.groupByKey.flatMap({
          case (lang, vectors) => reservoirSampling(lang, vectors.toIterator, perLang).map((lang, _))
        }).collect()

    assert(res.length == kmeansKernels, res.length)
    res
  }


  //
  //
  //  Kmeans method:
  //
  //

  /** Main kmeans computation */
  @tailrec final def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] = {

    val newMeans: Array[(LangIndex, LangIndex)] = means.clone() // you need to compute newMeans

    /* ----------------------------
              .  .      .
            .   .     .      .
       .   .   X   .  .  .      .
         .     .   .   .  .  .
        .   .   .     .   X   .
         .    .   .   .   .  .   .
       .   .    .      . .  .
          .  X     .       .  .
            .    .   .    . .
                 .     .

       vector: .
       mean:   X
     ---------------------------- */

    // 1: pair each vector with the index of the closest mean (its cluster);
    val pairs: RDD[(Int, (Int, Int))] = vectors.map(v => (findClosest(v,means), v ) )

    // 2: compute the new means by averaging the values of each cluster.
    val clusters: RDD[(Int, Iterable[(Int, Int)])] = pairs.groupByKey()
    val clusterMeans: RDD[(Int, (Int, Int))] = clusters.mapValues( vectors => averageVectors(vectors) )

    // 3: fill in the newMeans array (keep the old array - not all values may be overwritten)
    clusterMeans.collect().foreach{ case(i,tpl) => newMeans.update(i,tpl) }

    val distance = euclideanDistance(means, newMeans)

    if (debug) {
      println(s"""Iteration: $iter
                 |  * current distance: $distance
                 |  * desired distance: $kmeansEta
                 |  * means:""".stripMargin)
      for (idx <- 0 until kmeansKernels)
        println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
          f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
    }

    if (converged(distance))
      newMeans
    else if (iter < kmeansMaxIterations)
      kmeans(newMeans, vectors, iter + 1, debug)
    else {
      if (debug) {
        println("Reached max iterations!")
      }
      newMeans
    }
  }


  //
  //
  //  Kmeans utilities:
  //
  //

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) =
    distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while(idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }


  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }


  //
  //
  //  Displaying results:
  //
  //
  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(LangIndex, HighScore)]): Array[(String, Double, Int, Int)] = {
    val closest = vectors.map(p => (findClosest(p, means), p))
    val closestGrouped: RDD[(Int, Iterable[(LangIndex, HighScore)])] = closest.groupByKey()

    val median = closestGrouped.mapValues { vs: Iterable[(LangIndex, HighScore)] => {

        // group points by their language
        val langGroups: Map[LangIndex, Iterable[(LangIndex, HighScore)]] = vs.groupBy{ case(langIndex, score) => langIndex }
        // count number of points per language
        val langGroupsSize: Map[LangIndex, Int] = langGroups.mapValues(list => list.size)
        // get langIndex for largest number of points
        val langMaxGroupsIndex = langGroupsSize.maxBy{ case(langIndex, size) => size }._1
        // get count for most common language
        val mostCommonCount = vs.filter{ case(langIndex, score) => langIndex == langMaxGroupsIndex }.size

        // (a) the dominant programming language in the cluster;
        val langLabel: String = langs(langMaxGroupsIndex / langSpread) // correct for langSpread applied earlier!
        // (b) the percent of answers that belong to the dominant language;
        val langPercent: Double = (mostCommonCount * 100) / vs.size
        // (c) the size of the cluster (the number of questions it contains);
        val clusterSize: Int = vs.size

        // (d) the median of the highest answer scores.
        val lstScores: Seq[HighScore] = vs.map(_._2).toSeq
        val orderedScores: Seq[HighScore] = lstScores.sorted

        /*   How to get the median: (maybe put this in a function)

                |            | |
              | |          | | |
            | | | | |    | | | | | |
                a            a b
              odd: a        even: (a+b)/2
        */
        val isOdd = orderedScores.size % 2 == 1
        val center = orderedScores.size / 2
        val medianScore: Int =
          if (isOdd) orderedScores(center) // pick the center value
          else (orderedScores(center-1) + orderedScores(center))/2 // average the 2 center values

        (langLabel, langPercent, clusterSize, medianScore)
      }
    }

    median.collect().map(_._2).sortBy(_._4)
  }


  def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
    println("Resulting clusters:")
    println("  Score  Dominant language (%percent)  Questions")
    println("================================================")
    for ((lang, percent, size, score) <- results)
      println(f"${score}%7d  ${lang}%-17s (${percent}%-5.1f%%)      ${size}%7d")
  }
}
