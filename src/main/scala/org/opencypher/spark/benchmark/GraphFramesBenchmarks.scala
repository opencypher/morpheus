package org.opencypher.spark.benchmark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame
import org.opencypher.spark.impl.{MidPattern, SimplePatternIds, SupportedQuery}

object GraphFramesBenchmarks extends SupportedQueryBenchmarks[GraphFrame] {

  override def apply(query: SupportedQuery): Benchmark[GraphFrame] = query match {
    case SimplePatternIds(startLabels, types, endLabels) =>
      simplePatternIds(startLabels.head.toLowerCase, types.head, endLabels.head.toLowerCase)
    case MidPattern(startLabel, type1, midLabel, type2, endLabel) =>
      midPattern(startLabel.toLowerCase, type1, midLabel.toLowerCase, type2, endLabel.toLowerCase)
    case _ =>
      throw new IllegalArgumentException(s"No GraphFrame implementation of $query")
  }

  def simplePatternIds(startLabel: String, relType: String, endLabel: String) = new GraphFrameBenchmark {
    override def innerRun(graphFrame: GraphFrame): DataFrame = {
      val dataFrame = graphFrame.find("(a)-[r]->(b)")
      // filter label and relationship type
      val filtered = dataFrame
        .filter(col(s"a.$startLabel"))
        .filter(col("r.type") === relType)
        .filter(col(s"b.$endLabel"))

      val result = filtered.select("r.id")

      result
    }
  }

  def midPattern(startLabel: String, type1: String, midLabel: String, type2: String, endLabel: String) = new GraphFrameBenchmark {
    override def innerRun(graphFrame: GraphFrame): DataFrame = {
      val df = graphFrame.find("(a)-[r1]->(b); (b)-[r2]->(c)")
      val filtered = df
        .filter(col(s"a.$startLabel"))
        .filter(col(s"r1.type") === type1)
        .filter(col(s"b.$midLabel"))
        .filter(col(s"r2.type") === type2)
        .filter(col(s"c.$endLabel"))

      val result = filtered.select("r1.id", "r2.id")

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
    val (count, checksum) = countAndChecksum(frame)

    new Outcome {
      override lazy val computeCount: Long = count
      override lazy val computeChecksum: Int = checksum
      override val usedCachedPlan: Boolean = false
    }
  }

  def innerRun(graphFrame: GraphFrame): DataFrame
}

