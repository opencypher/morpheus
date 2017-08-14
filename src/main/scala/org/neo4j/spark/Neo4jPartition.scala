package org.neo4j.spark

import org.apache.spark.Partition

/**
  * @author mh
  * @since 02.03.16
  */
// , val lower: Long = 0, val upper: Long = 0 -> paging for cypher queries with skip / limit
class Neo4jPartition(idx: Long = 0, skip : Long = 0, limit : Long = Long.MaxValue) extends Partition {
  override def index: Int = idx.toInt
  val window : Map[String,Any] = Map("_limit" -> limit, "_skip" -> skip)

  override def toString: String = s"Neo4jRDD index $index skip $skip limit: $limit"
}
