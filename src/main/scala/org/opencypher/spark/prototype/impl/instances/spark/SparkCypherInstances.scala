package org.opencypher.spark.prototype.impl.instances.spark

import org.apache.spark.sql.DataFrame
import org.opencypher.spark.benchmark.Converters
import org.opencypher.spark.prototype.api.spark.{SparkCypherGraph, SparkCypherRecords, SparkCypherView, SparkGraphSpace}
import org.opencypher.spark.prototype.api.value.CypherValue
import org.opencypher.spark.prototype.impl.classy.Cypher
import org.opencypher.spark.prototype.impl.convert.{CypherParser, CypherQueryBuilder, GlobalsExtractor}
import org.opencypher.spark.prototype.impl.physical.{RecordsViewProducer, RuntimeContext}
import org.opencypher.spark.prototype.impl.planner.{LogicalPlanner, LogicalPlannerContext, ViewPlanner, ViewPlannerContext}

trait SparkCypherInstances {

  implicit val sparkCypherEngineInstance = new Cypher {

    override type Graph = SparkCypherGraph
    override type Space = SparkGraphSpace
    override type View = SparkCypherView
    override type Records = SparkCypherRecords
    override type Data = DataFrame

    private val logicalPlanner = new LogicalPlanner()
    private val parser = CypherParser

    override def cypher(graph: Graph, query: String, parameters: Map[String, CypherValue]): View = {
      val (stmt, extractedLiterals) = parser.parseAndExtract(query)

      val globals = GlobalsExtractor(stmt, graph.space.globals)

      val converted = extractedLiterals.mapValues(v => Converters.cypherValue(v))
      val constants = (parameters ++ converted).map {
        case (k, v) => globals.constant(k) -> v
      }

      val physicalPlanner = new ViewPlanner(new RecordsViewProducer(RuntimeContext(constants)))

      val ir = CypherQueryBuilder.from(stmt, query, globals)

      println("IR constructed")

      val logicalPlan = logicalPlanner.plan(ir)(LogicalPlannerContext(graph.schema, globals))

      println("Logical plan constructed")
      println(logicalPlan.solved)

      val physicalPlan = physicalPlanner.plan(logicalPlan)(ViewPlannerContext(graph, globals))
      println("Physical plan constructed")
      physicalPlan
    }
  }
}
