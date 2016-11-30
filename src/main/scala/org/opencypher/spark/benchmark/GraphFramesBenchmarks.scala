package org.opencypher.spark.benchmark

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame
import org.opencypher.spark.impl._

object GraphFramesBenchmarks extends SupportedQueryBenchmarks[GraphFrame] {

  override def apply(query: SupportedQuery): Benchmark[GraphFrame] = query match {
    case SimplePatternIds(startLabels, types, endLabels) =>
      simplePatternIds(startLabels.head.toLowerCase, types.head, endLabels.head.toLowerCase)
    case MidPattern(startLabel, type1, midLabel, type2, endLabel) =>
      midPattern(startLabel.toLowerCase, type1, midLabel.toLowerCase, type2, endLabel.toLowerCase)
    case FixedLengthPattern(start, steps) =>
      fixedLengthPattern(start.toLowerCase, steps.map(p => p._1 -> p._2.toLowerCase))
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

  def fixedLengthPattern(startLabel: String, steps: Seq[(Rel, String)]) = new GraphFrameBenchmark {
    override def innerRun(graphFrame: GraphFrame): DataFrame = {
      var step = 0

      val motif = steps.foldLeft(new StringBuilder(s"(n$step)")) {
        case (builder, (Out(typ), label)) =>
          if (step > 0) builder.append(s"; (n$step)")
          val nextStep = step + 1
          val acc = builder.append(s"-[r$step]->(n$nextStep)")
          step = nextStep
          acc
        case (builder, (In(typ), label)) =>
          if (step > 0) builder.append(s"; (n$step)")
          val nextStep = step + 1
          val acc = builder.append(s"<-[r$step]-(n$nextStep)")
          step = nextStep
          acc
      }

      step = 0
      var cols = Seq[Column]()

      val df = graphFrame.find(motif.toString)
      val filtered = steps.foldLeft(df.filter(col(s"n$step.$startLabel"))) {
        case (frame, (rel, label)) =>
          cols = cols :+ col(s"r$step.id")
          step = step + 1
          frame
          .filter(col(s"r${step - 1}.type") === rel.relType)
          .filter(col(s"n$step.$label"))
      }

      val result = filtered.select(cols.reduce(_ + _))

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

