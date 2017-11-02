package org.opencypher.caps.impl.spark.io.neo4j.external

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

class Neo4jRDD(@transient sc: SparkContext,
               val query: String,
               val neo4jConfig: Neo4jConfig,
               val parameters: Map[String, Any] = Map.empty,
               partitions: Partitions = Partitions())
  extends RDD[Row](sc, Nil) {

  override def compute(partition: Partition, context: TaskContext): Iterator[Row] = {

    val neo4jPartition: Neo4jPartition = partition.asInstanceOf[Neo4jPartition]

    Executor.execute(neo4jConfig, query, parameters ++ neo4jPartition.window).sparkRows
  }

  override protected def getPartitions: Array[Partition] = {
    val p = partitions.effective()
    Range(0, p.partitions.toInt).map(idx => new Neo4jPartition(idx, p.skip(idx), p.limit(idx))).toArray
  }

  override def toString(): String = s"Neo4jRDD partitions $partitions $query using $parameters"
}
