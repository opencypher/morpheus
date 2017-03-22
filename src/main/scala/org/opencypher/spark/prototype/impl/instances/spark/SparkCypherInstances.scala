package org.opencypher.spark.prototype.impl.instances.spark

import org.apache.spark.sql.DataFrame
import org.opencypher.spark.benchmark.Converters
import org.opencypher.spark.prototype.api.spark.{SparkCypherGraph, SparkCypherRecords, SparkGraphSpace}
import org.opencypher.spark.prototype.api.value.CypherValue
import org.opencypher.spark.prototype.impl.classy.Cypher
import org.opencypher.spark.prototype.impl.convert.{CypherParser, CypherQueryBuilder, GlobalsExtractor}
import org.opencypher.spark.prototype.impl.physical.{GraphProducer, RuntimeContext}
import org.opencypher.spark.prototype.impl.planner._

trait SparkCypherInstances {

  implicit val sparkCypherEngineInstance = new Cypher {

    override type Graph = SparkCypherGraph
    override type Space = SparkGraphSpace
    override type Records = SparkCypherRecords
    override type Data = DataFrame

    private val logicalPlanner = new LogicalPlanner()
    private val physicalPlanner = new PhysicalPlanner()
    private val graphPlanner = new GraphPlanner()
    private val parser = CypherParser

    override def cypher(graph: Graph, query: String, parameters: Map[String, CypherValue]): Graph = {
      val (stmt, extractedLiterals) = parser.parseAndExtract(query)

      val globals = GlobalsExtractor(stmt, graph.space.globals)

      val converted = extractedLiterals.mapValues(v => Converters.cypherValue(v))
      val constants = (parameters ++ converted).map {
        case (k, v) => globals.constant(k) -> v
      }

      print("IR ... ")
      val ir = CypherQueryBuilder.from(stmt, query, globals)
      println("Done!")

      print("Logical plan ... ")
      val logicalPlan = logicalPlanner.plan(ir)(LogicalPlannerContext(graph.schema, globals))
      println("Done!")

//      print("Physical plan ...")
//      val physicalPlan = physicalPlanner.plan(logicalPlan)(PhysicalPlannerContext(graph.schema, globals))
//      println("Done!")

      print("Graph plan ...")
      val graphPlan = graphPlanner.plan(logicalPlan)(GraphPlannerContext(graph, globals, constants))
      println("Done!")

      graphPlan
    }
  }
}
