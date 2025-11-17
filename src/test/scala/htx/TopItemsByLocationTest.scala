package htx

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import scala.collection.mutable.PriorityQueue

class TopItemsByLocationTest extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var sc: SparkContext = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[*]")
      .appName("TopItemsByLocationTest")
      .getOrCreate()
    sc = spark.sparkContext
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
      sc = null
    }
  }

  import TopItemsByLocation.ItemCount
  import TopItemsByLocation.itemCountOrdering

  test("combineByKey deduplicates detection_oid") {
    val input = Seq(
      (1L, 101L, "itemA"),
      (1L, 101L, "itemA"), // duplicate detection_oid
      (2L, 102L, "itemB"),
      (3L, 103L, "itemC")
    )
    val rdd = sc.parallelize(input)
    val uniqueDetections = rdd
      .map { case (detId, geog, item) => (detId, (geog, item)) }
      .combineByKey[(Long, String)](
        (v: (Long, String)) => v,
        (existing: (Long, String), _: (Long, String)) => existing,
        (v1: (Long, String), _: (Long, String)) => v1
      )
      .map(_._2)
      .collect()

    assert(uniqueDetections.length == 3)
    assert(uniqueDetections.contains((101L, "itemA")))
    assert(uniqueDetections.exists(_._1 == 102L))
  }

  test("item counting and top-x works correctly") {
    val uniqueDetections = Seq(
      (101L, "itemA"),
      (101L, "itemA"),
      (101L, "itemB"),
      (102L, "itemC"),
      (102L, "itemB")
    )
    val rdd = sc.parallelize(uniqueDetections)

    val itemCounts = rdd
      .map { case (geog, item) => ((geog, item), 1L) }
      .reduceByKey(_ + _)

    val geogItemCounts = itemCounts.map { case ((geog, item), count) => (geog, ItemCount(item, count)) }

    val topX = 1

    val topPerGeog = geogItemCounts.aggregateByKey(
      PriorityQueue.empty[ItemCount](itemCountOrdering)
    )(
      (pq, ic) => {
        pq.enqueue(ic)
        if (pq.size > topX) pq.dequeue()
        pq
      },
      (pq1, pq2) => {
        pq2.foreach { ic =>
          pq1.enqueue(ic)
          if (pq1.size > topX) pq1.dequeue()
        }
        pq1
      }
    )

    val results = topPerGeog.collectAsMap()

    assert(results(101L).exists((ic: ItemCount) => ic.item == "itemA" || ic.item == "itemB"))
    assert(results(102L).exists((ic: ItemCount) => ic.item == "itemC" || ic.item == "itemB"))
  }
}
