package org.opencypher.spark.impl.spark

import org.apache.spark.sql.SparkSession
import org.opencypher.spark.api.expr.Var
import org.opencypher.spark.api.ir.global.GlobalsRegistry
import org.opencypher.spark.api.record.{OpaqueField, RecordHeader}
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.spark.{SparkCypherGraph, SparkCypherRecords, SparkCypherSession, SparkGraphSpace}
import org.opencypher.spark.api.types.{CTNode, CTRelationship}

import scala.collection.mutable

class SparkGraphSpaceImpl(implicit val session: SparkCypherSession) extends SparkGraphSpace {

  graphSpace =>

  // TODO: Make this thread safe

  private var _globals: GlobalsRegistry = GlobalsRegistry.none
  private var _graphs: mutable.Map[String, SparkCypherGraph] = new mutable.HashMap()

  def globals = _globals

  override def base: SparkCypherGraph = ???

  override def importGraph(name: String, dfGraph: VerifiedDataFrameGraph) = register(name, new SparkCypherGraph {

    override def space: SparkGraphSpace = graphSpace

    override def relationships(v: Var): SparkCypherRecords = ???
    override def nodes(v: Var): SparkCypherRecords = ???

    // TODO: This should come from the input or be computed here
    override def schema: Schema = Schema.empty

    override def _nodes(name: String, typ: CTNode): SparkCypherRecords = dfGraph.verified.nodeDf match {
      case Some(input) =>
        ???

      case None =>
        SparkCypherRecords.empty(RecordHeader.from(OpaqueField(Var(name)(typ))))
    }

    override def _relationships(name: String, typ: CTRelationship): SparkCypherRecords = dfGraph.verified.relDf match {
      case Some(input) =>
        ???

      case None =>
        SparkCypherRecords.empty(RecordHeader.from(OpaqueField(Var(name)(typ))))
    }
  })

  private def register(name: String, graph: SparkCypherGraph): SparkCypherGraph = {
    if (_graphs.contains(name))
      throw new IllegalArgumentException(s"Already contains a graph with name $name")
    else {
      _graphs(name) = graph
      graph
    }
  }

  override def graph(name: String): Option[SparkCypherGraph] = _graphs.get(name)
}
