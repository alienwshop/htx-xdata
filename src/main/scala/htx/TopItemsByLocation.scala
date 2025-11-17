package htx

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.PriorityQueue

object TopItemsByLocation {

  //---------------------------------------------------------------------------
  // Internal data structures
  //---------------------------------------------------------------------------

  // Represents an item + its count
  case class ItemCount(item: String, count: Long)

  // Ordering for min-heap (smallest count at the top)
  implicit val itemCountOrdering: Ordering[ItemCount] =
    Ordering.by[ItemCount, Long](_.count)

  //---------------------------------------------------------------------------
  // Program Entry
  //---------------------------------------------------------------------------

  def main(args: Array[String]): Unit = {

    if (args.length < 4) {
      System.err.println(
        "Usage: TopItemsByLocation <inputA> <inputB> <output> <topX>"
      )
      System.exit(1)
    }

    val inputA = args(0)   // Dataset A (big)
    val inputB = args(1)   // Dataset B (small reference)
    val output = args(2)
    val topX = args(3).toInt

    val spark = SparkSession.builder()
      .appName("TopItemsByLocation-Optimized")
      .getOrCreate()

    import spark.implicits._

    //---------------------------------------------------------------------------
    // 1. Read Parquet (allowed; only transformations must be RDD-based)
    //---------------------------------------------------------------------------

    val dfA = spark.read.parquet(inputA)
    val dfB = spark.read.parquet(inputB)

    // Convert Dataset A to RDD of primitive tuples for minimal overhead
    val detRDD = dfA.rdd.map { row =>
      (
        row.getAs[Long]("detection_oid"),
        row.getAs[Long]("geographical_location_oid"),
        row.getAs[String]("item_name")
      )
    }

    //---------------------------------------------------------------------------
    // 2. Deduplicate by detection_oid using COMBINEBYKEY (map-side combine)
    //    Key: detection_oid → Keep first (geog, item)
    //---------------------------------------------------------------------------

    val uniqueDetections = detRDD
      .map { case (detId, geog, item) => (detId, (geog, item)) }
      .combineByKey[(Long, String)](
        (v: (Long, String)) => v,                           // createCombiner
        (existing: (Long, String), _: (Long, String)) => existing,    // mergeValue → keep first
        (v1: (Long, String), _: (Long, String)) => v1               // mergeCombiners → keep first
        )
      .map(_._2) // drop detection_oid key: now RDD[(geog, item)]


    //---------------------------------------------------------------------------
    // 3. Count number of unique detections per (geog, item)
    //---------------------------------------------------------------------------

    val itemCounts = uniqueDetections
      .map { case (geog, item) => ((geog, item), 1L) }
      .reduceByKey(_ + _) // map-side combine reduces shuffle volume

    //---------------------------------------------------------------------------
    // 4. Compute Top-X per geographic location using a bounded min-heap
    //---------------------------------------------------------------------------

    // Convert to (geog, ItemCount)
    val geogItemCounts = itemCounts.map {
      case ((geog, item), count) => (geog, ItemCount(item, count))
    }

    val topPerGeog = geogItemCounts.aggregateByKey(
      PriorityQueue.empty[ItemCount](itemCountOrdering) // min-heap
    )(
      // seqOp
      (pq, ic) => {
        pq.enqueue(ic)
        if (pq.size > topX) pq.dequeue()
        pq
      },
      // combOp
      (pq1, pq2) => {
        pq2.foreach { ic =>
          pq1.enqueue(ic)
          if (pq1.size > topX) pq1.dequeue()
        }
        pq1
      }
    )
    // Now RDD[(geog, PQ)] with each PQ containing at most topX items

    //---------------------------------------------------------------------------
    // 5. Broadcast enrich using Dataset B (small lookup table)
    //---------------------------------------------------------------------------

    val locationMap = dfB.rdd
      .map(row =>
        (
          row.getAs[Long]("geographical_location_oid"),
          row.getAs[String]("geographical_location")
        )
      )
      .collectAsMap()

    val bLocMap: Broadcast[collection.Map[Long, String]] =
      spark.sparkContext.broadcast(locationMap)

    //---------------------------------------------------------------------------
    // 6. Produce final output rows with schema Option 2:
    //    (geographical_location, item_rank, item_name, item_count)
    //---------------------------------------------------------------------------

    val finalRows = topPerGeog.mapPartitions { iter =>
      val locLookup = bLocMap.value

      iter.flatMap { case (geog, pq) =>
        val sorted = pq.toList.sortBy(-_.count)
        sorted.zipWithIndex.map { case (ic, idx) =>
          (
            geog,
            idx + 1,             // item_rank
            ic.item,
            ic.count
          )
        }
      }
    }.toDF(
      "geographical_location",
      "item_rank",
      "item_name",
      "item_count"
    )

    //---------------------------------------------------------------------------
    // 7. Write output
    //---------------------------------------------------------------------------

    finalRows.write.mode(SaveMode.Overwrite).parquet(output)

    spark.stop()
  }
}
