package org.opencypher.spark.impl.spark

import org.apache.spark.sql.SparkSession
import org.opencypher.spark.api.expr.Var
import org.opencypher.spark.api.ir.global.GlobalsRegistry
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.spark.{SparkCypherGraph, SparkCypherRecords, SparkGraphSpace}
import org.opencypher.spark.api.types.{CTNode, CTRelationship}

import scala.collection.mutable

class SparkGraphSpaceImpl(val session: SparkSession) extends SparkGraphSpace {

  graphSpace =>

  // TODO: Make this thread safe

  private var _globals: GlobalsRegistry = GlobalsRegistry.none
  private var _graphs: mutable.Map[String, SparkCypherGraph] = new mutable.HashMap()

  def globals = _globals

  override def base: SparkCypherGraph = ???

  override def importGraph(name: String, externalGraph: VerifiedExternalGraph): SparkCypherGraph = {

    val graph = new SparkCypherGraph {
      override def space: SparkGraphSpace = graphSpace

      override def relationships(v: Var): SparkCypherRecords = ???

      override def nodes(v: Var): SparkCypherRecords = ???

      override def schema: Schema = ???

      override def _nodes(name: String, typ: CTNode): SparkCypherRecords = externalGraph.verified.nodeFrame match {
        case Some(input) =>
          ???

        case None =>
          SparkCypherGraph.empty(graphSpace)._nodes(name, typ)
      }

      override def _relationships(name: String, typ: CTRelationship): SparkCypherRecords = externalGraph.verified.relFrame match {
        case Some(input) =>
          ???

        case None =>
          SparkCypherGraph.empty(graphSpace)._relationships(name, typ)
      }
    }
    _graphs(name) = graph
    graph
  }

  override def graph(name: String): Option[SparkCypherGraph] = _graphs.get(name)
}
