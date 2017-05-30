package org.opencypher.spark.impl.instances.spark

import org.apache.spark.sql.DataFrame
import org.opencypher.spark_legacy.benchmark.Converters
import org.opencypher.spark.api.spark.{SparkCypherGraph, SparkCypherRecords, SparkCypherResult, SparkGraphSpace}
import org.opencypher.spark.api.value.CypherValue
import org.opencypher.spark.impl.classes.Cypher
import org.opencypher.spark.impl.flat.{FlatPlanner, FlatPlannerContext}
import org.opencypher.spark.impl.ir.{BlockRegistry, CypherQueryBuilder, GlobalsExtractor, IRBuilderContext}
import org.opencypher.spark.impl.logical.{LogicalPlanner, LogicalPlannerContext}
import org.opencypher.spark.impl.parse.CypherParser
import org.opencypher.spark.impl.physical.{PhysicalPlanner, PhysicalPlannerContext}

trait SparkCypherInstances {

  implicit val sparkCypherEngineInstance = new Cypher {

    override type Graph = SparkCypherGraph
    override type Space = SparkGraphSpace
    override type Records = SparkCypherRecords
    override type Result = SparkCypherResult
    override type Data = DataFrame

    private val logicalPlanner = new LogicalPlanner()
    private val flatPlanner = new FlatPlanner()
    private val physicalPlanner = new PhysicalPlanner()
    private val parser = CypherParser

    override def cypher(graph: Graph, query: String, parameters: Map[String, CypherValue]): Result = {
      val (stmt, extractedLiterals) = parser.process(query)(CypherParser.defaultContext)

      val globals = GlobalsExtractor(stmt, graph.space.globals)

      val converted = extractedLiterals.mapValues(v => Converters.cypherValue(v))
      val constants = (parameters ++ converted).map {
        case (k, v) => globals.constant(k) -> v
      }

      val paramsAndTypes = GlobalsExtractor.paramWithTypes(stmt)

      print("IR ... ")
      val ir = CypherQueryBuilder(stmt)(IRBuilderContext.initial(query, globals, graph.schema, paramsAndTypes))
      println("Done!")

      print("Logical plan ... ")
      val logicalPlan = logicalPlanner(ir)(LogicalPlannerContext(graph.schema, globals))
      println("Done!")

      print("Flat plan ... ")
      val flatPlan = flatPlanner(logicalPlan)(FlatPlannerContext(graph.schema, globals))
      println("Done!")

      print("Physical plan ... ")
      val physicalPlan = physicalPlanner(flatPlan)(PhysicalPlannerContext(graph, globals, constants, graph.space.session))
      println("Done!")

      physicalPlan
    }
  }
}

