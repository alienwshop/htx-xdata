# Design Considerations
1. Data Model & Deduplication
    * Deduplucation is done using `combineByKey` on `detection_oid`
        * **Rationale**: Map-side combining ensures minimal shuffle by keeping only the first value per detection.
        * **Efficiency**: Since each detection has only one `(geog, item)` combination, this avoids unnecessary duplication.
        * **Consideration**: Ensure `detection_oid` is indeed unique per detection. If collisions exist, current logic keeps the first arbitrarily.

2. Counting Unique Detections per `(geog, item)`
    * `reduceByKey(_ + _)` is used to count unique occurrences:
        * **Rationale**: Reduces shuffle because `reduceByKey` performs a map-side combine before shuffling data across partitions.
        * **Efficiency**: Optimal for 1M rows; `1M` rows is moderate for Spark but still benefits from reducing unnecessary shuffles.

3. Top-X Computation Using Min-Heap
    * `aggregateByKey` with `PriorityQueue` ensures:
        * Only top-X items are kept per geographic location.
        * Memory usage is bounded per key.
    * **Consideration**: Using `PriorityQueue` is efficient, but:
        * Ensure `topX` is small (tens or hundreds). Very large `topX` per geog may increase driver memory pressure.
        * The min-heap ensures only the largest counts survive.

4. Broadcast Enrichment
    * Dataset B (10,000 rows) is broadcast to avoid shuffling for a join.
        * **Rationale**: Small reference tables should always be broadcasted to minimize network overhead.
        * **Consideration**: Ensure the broadcast size does not exceed driver memory (10k rows is fine).

5. RDD vs DataFrame
    * Transformations are RDD-based, but input/output is via DataFrame/Parquet:
        * **Rationale**: DataFrame for I/O is optimized for Parquet; RDD transformations are controlled and flexible.
        * **Consideration**: For 1M rows, RDD is acceptable, but for very large datasets (>100M), DataFrame API with built-in aggregation may be faster.

6. Output
    * Output schema is:
    ```
    (geographical_location, item_rank, item_name, item_count)
    ```
    * Ensures compatibility with downstream systems and easy consumption.

# Spark Configuration Recommendations

| Configuration                                        | Recommended Value                            | Rationale                                                                |
| ---------------------------------------------------- | -------------------------------------------- | ------------------------------------------------------------------------ |
| `spark.executor.memory`                              | 4G                                           | Enough memory to hold RDD partitions, PriorityQueues, and broadcast map. |
| `spark.driver.memory`                                | 4G                                           | To accommodate broadcast variable and driver-side operations.            |
| `spark.executor.cores`                               | 2                                            | Balance parallelism with memory usage for small dataset.                 |
| `spark.sql.shuffle.partitions`                       | 8–16                                         | Reduce tiny shuffle files for small dataset (~1M rows).                  |
| `spark.serializer`                                   | `org.apache.spark.serializer.KryoSerializer` | Efficient RDD serialization and broadcast.                               |
| `spark.driver.maxResultSize`                         | 1G                                           | Ensure broadcast variable fits in driver memory.                         |
| `spark.sql.autoBroadcastJoinThreshold`               | default (10MB)                               | Dataset B (~10k rows) fits, enables broadcast join automatically.        |
| GC options (optional)                                | `-XX:+UseG1GC`                               | For larger datasets; reduces GC overhead.                                |
| `numPartitions` for `reduceByKey` / `aggregateByKey` | 8                                            | Tuned to match shuffle partitions, avoids too many small tasks.          |


All content above generated with `Perplexity`