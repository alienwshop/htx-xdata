# Strategies to address data skew
The following 3 techniques can be used to address data skew:

| Technique                               | Description                                 | Pros                               | Cons                                                 |
| --------------------------------------- | ------------------------------------------- | ---------------------------------- | ---------------------------------------------------- |
| Key Salting                             | Add random salt to keys to spread skew load | Balances workload well             | Increases shuffle shuffle data volume and complexity |
| Map-side Combine Aggregation            | Use map-side pre-aggregation (combineByKey) | Reduces shuffle data transfer      | May not fully solve extreme skews                    |
| Hot Key Filtering & Separate Processing | Identify skew keys, process separately      | Targeted optimization for hot keys | Additional logic and complexity                      |

______
# Code Snippets
1. Key Salting
```
import scala.util.Random

val SALT_RANGE = 20

// Salt keys
val saltedCounts = uniqueDetections.map { case (geog, item) =>
  val salt = Random.nextInt(SALT_RANGE)
  ((s"${salt}_$geog", item), 1L)
}

// Partial reduce by salted keys
val partiallyAggregated = saltedCounts.reduceByKey(_ + _)

// Remove salt and map back to original keys
val desaltedCounts = partiallyAggregated.map {
  case ((saltedGeog, item), count) =>
    val geog = saltedGeog.split("_", 2)(1).toLong
    ((geog, item), count)
}

// Final aggregation
val itemCounts = desaltedCounts.reduceByKey(_ + _)
```

2. Combining Local Aggregation + Map-Side Pre-Aggregation
```
val itemCounts = uniqueDetections.map {
  case (geog, item) => ((geog, item), 1L)
}.combineByKey[Long](
  (count) => count,          // create combiner
  (acc, value) => acc + value,  // merge value (map side)
  (acc1, acc2) => acc1 + acc2    // merge combiners (reduce side)
)
```

3. Filtering Out Hot Keys Before Reduce and Handling Separately
```
val hotKeys = Set(12345L) // example skewed geog IDs

val (hotKeyRDD, normalRDD) = uniqueDetections.partition { case (geog, _) =>
  hotKeys.contains(geog)
}

// Handle hot keys with custom partitioning or broadcasting
val hotKeyCounts = hotKeyRDD
  .map(item => ((item._1, item._2), 1L))
  .reduceByKey(_ + _) // Can repartition and process with more parallelism

val normalCounts = normalRDD
  .map(item => ((item._1, item._2), 1L))
  .reduceByKey(_ + _)

val itemCounts = normalCounts.union(hotKeyCounts)
```

Above content generated with `Perplexity`