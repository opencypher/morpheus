package org.opencypher.spark.prototype.api.graph

import org.opencypher.spark.prototype.api.schema.Schema
import org.opencypher.spark.prototype.api.value.CypherString
import org.opencypher.spark.prototype.impl.convert.{CypherParser, CypherQueryBuilder, GlobalsExtractor}
import org.opencypher.spark.prototype.impl.planner.{LogicalPlanner, PhysicalPlanner, PhysicalPlanningContext}

trait CypherGraph {
  type Space <: GraphSpace
  type View <: CypherView

  def space: Space

  def nodes: View
  def relationships: View

  def constituents: Set[View]

  def schema: Schema

  def cypher(query: String): View

  // identity
  // properties
  // labels
}

trait SparkCypherGraph extends CypherGraph {
  override type Space = SparkGraphSpace
  override type View = SparkCypherView

  override def cypher(query: String) = CypherGraphWithQuery.cypher(query, this)
}

object CypherGraphWithQuery {
  def cypher(query: String, graph: SparkCypherGraph): SparkCypherView = {
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
