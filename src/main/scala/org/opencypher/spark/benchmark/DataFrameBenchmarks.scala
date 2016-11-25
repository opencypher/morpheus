package org.opencypher.spark.benchmark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.opencypher.spark.api.value.CypherNode
import org.opencypher.spark.benchmark.AccessControlSchema.labelIndex
import org.opencypher.spark.impl.{SimplePatternIds, StdPropertyGraph, SupportedQuery}

object DataFrameBenchmarks extends SupportedQueryBenchmarks[SimpleDataFrameGraph] {

  def apply(query: SupportedQuery): Benchmark[SimpleDataFrameGraph] = query match {
    case SimplePatternIds(startLabels, types, endLabels) =>
      simplePatternIds(query.toString, labelIndex(startLabels.head), types.head, labelIndex(endLabels.head))
    case _ =>
      throw new IllegalArgumentException(s"No DataFrame implementation of $query")
  }

//  def nodeScanIdsSorted(label: String, sparkSession: SparkSession) = (graph: StdPropertyGraph) => {
//    import sparkSession.implicits._
//
//    val ids = graph.nodes.filter(CypherNode.labels(_).exists(_.contains(label))).map(CypherNode.id(_).map(_.v).get)
//    ids.sort(desc(ids.columns(0)))
//  }

  //MATCH (:Group)-[r:ALLOWED]->(:Company) RETURN id(r)
  def simplePatternIds(query: String, startLabel: Int, relType: String, endLabel: Int) = new DataFrameBenchmark(query) {

    override def innerRun(graph: SimpleDataFrameGraph) = {
      val nodes = graph.nodes.as("nodes")
      val rels = graph.relationships.filter(col("typ").equalTo(relType)).as("rels")

      val startLabeled = nodes.filter(row => row.getBoolean(startLabel)).alias("startLabeled")
      val endLabeled = nodes.filter(_.getBoolean(endLabel)).alias("endLabeled")

      val startJoined = startLabeled.join(rels, col("startLabeled.id") === col("rels.startId"))

      val endJoined = startJoined.join(endLabeled, col("endId") === col("endLabeled.id"))

      val result = endJoined.select(col("rels.id"))

      val (count, checksum) = result.rdd.treeAggregate((0, 0))({
        case ((c, cs), rel) => (c + 1, cs ^ rel.get(0).hashCode())
      }, {
        case ((lCount, lChecksum), (rCount, rChecksum)) => (lCount + rCount, lChecksum ^ rChecksum)
      })


      (result, count, checksum)
    }
  }
}

abstract class DataFrameBenchmark(query: String) extends Benchmark[SimpleDataFrameGraph] with Serializable {
  override def name: String = "DataFrame "

  override def run(graph: SimpleDataFrameGraph): Outcome = {
    val (frame, count,  checksum) = innerRun(graph)

    new Outcome {

      override lazy val plan = frame.queryExecution.toString()
      override lazy val computeCount = count
      override lazy val computeChecksum = checksum
    }
  }

  def innerRun(graph: SimpleDataFrameGraph): (DataFrame, Long, Int)
}


/*

 n

 id(n), n.prop, n.prop2, n:X, properties(n), cast(n.prop, INTEGER)


 MATCH (:Group)-[r:ALLOWED]->(:Company) RETURN id(r)

 SELECT id FROM nodes
 SELECT id FROM nodes
 SELECT id, startId, endId FROM relationships

 join(all three)


 MATCH (:Group)-[r:ALLOWED]->(:Company) RETURN id(r)

 SELECT id FROM nodes_partitioned_by_id
 SELECT left_rel_ids, startId FROM relationships_partitioned_by_start
 SELECT right_rel_ids, endId FROM relationships_partitioned_by_end
 join(left_rel_ids, right_rel_ids)


 id -> :X, :Y, .prop1, .prop2

 1) One big flat table

 2) Multiple tables but with rules

    id (:X) -> .prop1 .prop2
    id (:Y) -> .prop4 .prop4
    id   -> .prop1

 */


case class SimpleDataFrameGraph(nodes: DataFrame, relationships: DataFrame)
