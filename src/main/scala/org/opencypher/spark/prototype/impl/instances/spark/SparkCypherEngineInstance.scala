package org.opencypher.spark.prototype.impl.instances.spark

import org.apache.spark.sql.DataFrame
import org.opencypher.spark.prototype.api.graph.{SparkCypherGraph, SparkCypherView, SparkGraphSpace}
import org.opencypher.spark.prototype.api.record.SparkCypherRecords
import org.opencypher.spark.prototype.api.value.CypherValue
import org.opencypher.spark.prototype.impl.classy.CypherEngine
import org.opencypher.spark.prototype.impl.convert.{CypherParser, CypherQueryBuilder, GlobalsExtractor}
import org.opencypher.spark.prototype.impl.planner.{LogicalPlanner, PhysicalPlanner, PhysicalPlanningContext}

trait SparkCypherEngineInstance {
  implicit val sparkCypherEngineInstance = new CypherEngine {
    override type EGraph = SparkCypherGraph
    override type ESpace = SparkGraphSpace
    override type EView = SparkCypherView
    override type ERecords = SparkCypherRecords
    override type EData = DataFrame

    override def cypher(graph: SparkCypherGraph, query: String, parameters: Map[String, CypherValue]): SparkCypherView = {
      val (stmt, params) = parser.parseAndExtract(query)

      val globals = GlobalsExtractor(stmt, graph.space.globals)

      val ir = CypherQueryBuilder.from(stmt, query, globals)

      //    val cvs = params.mapValues {
      //      case s: String => CypherString(s)
      //      case x => throw new UnsupportedOperationException(s"Can't convert $x to CypherValue yet")
      //    }

      val plan = new LogicalPlanner().plan(ir)

      println(plan)

      physicalPlanner.plan(plan)(PhysicalPlanningContext(graph, globals))
    }

    val physicalPlanner = new PhysicalPlanner
    val parser = CypherParser
  }
}
