package org.opencypher.spark.benchmark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame
import org.opencypher.spark.impl.{SimplePatternIds, SupportedQuery}

object GraphFramesBenchmarks extends SupportedQueryBenchmarks[GraphFrame] {

  override def apply(query: SupportedQuery): Benchmark[GraphFrame] = query match {
    case SimplePatternIds(startLabels, types, endLabels) =>
      simplePatternIds(startLabels.head, types.head, endLabels.head)
    case _ =>
      throw new IllegalArgumentException(s"No GraphFrame implementation of $query")
  }

  def simplePatternIds(startLabel: String, relType: String, endLabel: String) = new GraphFrameBenchmark {
    override def innerRun(graphFrame: GraphFrame): DataFrame = {
      val dataFrame = graphFrame.find("(a)-[r]->(b)")
      // filter label and relationship type
      val filtered = dataFrame.filter(col(s"a.${startLabel.toLowerCase}"))
        .filter(col(s"b.${endLabel.toLowerCase}"))
        .filter(col("r.type") === relType)

      val result = filtered.select("r.id")

      result
    }
  }
}

abstract class GraphFrameBenchmark extends Benchmark[GraphFrame] with Serializable {
  override def name: String = "GraphFrame"

  override def numNodes(graph: GraphFrame): Long = graph.vertices.count()

  override def numRelationships(graph: GraphFrame): Long = graph.edges.count()

  override def plan(graph: GraphFrame): String = innerRun(graph).queryExecution.toString

  override def run(graph: GraphFrame): Outcome = {
    val frame = innerRun(graph)
    val (count, checksum) = frame.rdd.treeAggregate((0l, 0))({
      case ((c, cs), rel) => (c + 1l, cs ^ rel.get(0).hashCode())
    }, {
      case ((lCount, lChecksum), (rCount, rChecksum)) => (lCount + rCount, lChecksum ^ rChecksum)
    })

    new Outcome {
      override lazy val computeCount: Long = count
      override lazy val computeChecksum: Int = checksum
      override val usedCachedPlan: Boolean = false
    }
  }

  def innerRun(graphFrame: GraphFrame): DataFrame
}

