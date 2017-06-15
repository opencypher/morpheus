package org.opencypher.spark.impl.instances.spark

import org.apache.spark.sql.DataFrame
import org.opencypher.spark.api.classes.Cypher
import org.opencypher.spark.api.spark._
import org.opencypher.spark.api.value.CypherValue
import org.opencypher.spark.impl.flat.{FlatPlanner, FlatPlannerContext}
import org.opencypher.spark.impl.ir.global.GlobalsExtractor
import org.opencypher.spark.impl.ir.{CypherQueryBuilder, IRBuilderContext}
import org.opencypher.spark.impl.logical.{LogicalPlanner, LogicalPlannerContext}
import org.opencypher.spark.impl.parse.CypherParser
import org.opencypher.spark.impl.physical.{PhysicalPlanner, PhysicalPlannerContext}
import org.opencypher.spark_legacy.benchmark.Converters

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

      val globals = GlobalsExtractor(stmt, graph.space.tokens.globals)

      val converted = extractedLiterals.mapValues(v => Converters.cypherValue(v))
      val constants = (parameters ++ converted).map {
        case (k, v) => globals.constantRefByName(k) -> v
      }

      val paramsAndTypes = GlobalsExtractor.paramWithTypes(stmt)

      print("IR ... ")
      val ir = CypherQueryBuilder(stmt)(IRBuilderContext.initial(query, globals, graph.schema, paramsAndTypes))
      println("Done!")

      print("Logical plan ... ")
      val logicalPlan = logicalPlanner(ir)(LogicalPlannerContext(graph.schema))
      println("Done!")

      // TODO: Remove dependency on globals (?) Only needed to enforce everything is known, that could be done
      //       differently
      print("Flat plan ... ")
      val flatPlan = flatPlanner(logicalPlan)(FlatPlannerContext(graph.schema, globals))
      println("Done!")

      // TODO: It may be better to pass tokens around in the physical planner explicitly (via the records)
      //       instead of just using a single global tokens instance derived from the graph space
      //
      print("Physical plan ... ")
      val physicalPlan = physicalPlanner(flatPlan)(PhysicalPlannerContext(graph, globals, constants))
      println("Done!")

      physicalPlan
    }
  }
}

