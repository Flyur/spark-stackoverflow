package stackoverflow

import org.apache.spark.rdd.RDD
import org.junit.{Assert, Test}
import stackoverflow.StackOverflow._
import stackoverflow.StackOverflowSuite.sc

class ApiTest {

  @Test def newMeansTest(): Unit = {
    val lines: RDD[String] = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
    val raw: RDD[Answer] = rawPostings(lines)
    val grouped: RDD[(QID, Iterable[(Question, Answer)])] = groupedPostings(raw)
    val scored: RDD[(Question, HighScore)] = scoredPostings(grouped)
    val vectors: RDD[(LangIndex, HighScore)] = vectorPostings(scored).persist()

    val means = sampleVectors(vectors)
    val newMeans = means.clone()

    val clusters = vectors
      .map(vector => (findClosest(vector, means), vector))
      .groupByKey()
      .map(item => (item._1, averageVectors(item._2)))
      .collect()

    for (value <- clusters) {
      newMeans.update(value._1, value._2)
    }

    Assert.assertFalse(newMeans.isEmpty)
    Assert.assertEquals(means.length, newMeans.length)
  }

  @Test def calculateMedianTest(): Unit = {
    Assert.assertEquals(4, calculateMedian(Array(1, 2, 3, 4, 5, 6)))
    Assert.assertEquals(4, calculateMedian(Array(1, 2, 3, 4, 5, 6, 7)))
    Assert.assertEquals(4, calculateMedian(Array(9, 4, 10, 2, 7, 3, 1)))
  }
}
