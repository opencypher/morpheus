package org.opencypher.spark.prototype.impl.physical

import org.opencypher.spark.api.types.CTNode
import org.opencypher.spark.prototype.api.expr._
import org.opencypher.spark.prototype.api.ir.QueryModel
import org.opencypher.spark.prototype.api.ir.global.{ConstantRef, LabelRef, RelTypeRef}
import org.opencypher.spark.prototype.api.ir.pattern.{AllGiven, AnyGiven}
import org.opencypher.spark.prototype.api.record.{ProjectedSlotContent, RecordSlot}
import org.opencypher.spark.prototype.api.schema.Schema
import org.opencypher.spark.prototype.api.spark.{SparkCypherGraph, SparkCypherRecords, SparkCypherView, SparkGraphSpace}
import org.opencypher.spark.prototype.api.value.CypherValue
import org.opencypher.spark.prototype.impl.instances.spark.records._
import org.opencypher.spark.prototype.impl.syntax.transform._

object RuntimeContext {
  val empty = RuntimeContext(Map.empty)
}

case class RuntimeContext(constants: Map[ConstantRef, CypherValue])

class GraphProducer(context: RuntimeContext) {

  implicit val c = context

  implicit final class RichCypherGraph(val graph: SparkCypherGraph) {
    def allNodes(v: Var): SparkCypherGraph = graph.nodes(v)

    def allRelationships(v: Var): SparkCypherGraph = graph.relationships(v)

    def select(fields: Map[Expr, String]): SparkCypherGraph =
      InternalCypherGraph(graph.records.select(fields))

    def project(slot: ProjectedSlotContent): SparkCypherGraph =
      InternalCypherGraph(graph.records.project(slot))

    def filter(expr: Expr): SparkCypherGraph =
      InternalCypherGraph(graph.records.filter(expr))

    def labelFilter(node: Var, labels: AllGiven[LabelRef]): SparkCypherGraph = {
      val labelExprs: Set[Expr] = labels.elts.map { ref => HasLabel(node, ref) }
      InternalCypherGraph(graph.records.filter(Ands(labelExprs)))
    }

    def typeFilter(rel: Var, types: AnyGiven[RelTypeRef]): SparkCypherGraph = {
      if (types.elts.isEmpty) graph
      else {
        val typeExprs: Set[Expr] = types.elts.map { ref => HasType(rel, ref) }
        InternalCypherGraph(graph.records.filter(Ands(typeExprs)))
      }
    }

    def expandSource(relView: SparkCypherGraph) = new JoinBuilder {
      override def on(node: Var)(rel: Var) = {
        val lhsSlot = graph.records.header.slotFor(node)
        val rhsSlot = relView.records.header.sourceNode(rel)

        assertIsNode(lhsSlot)
        assertIsNode(rhsSlot)

        val records = graph.records.join(relView.records)(lhsSlot, rhsSlot)
        InternalCypherGraph(records)
      }
    }

    def joinTarget(nodeView: SparkCypherGraph) = new JoinBuilder {
      override def on(rel: Var)(node: Var) = {
        val lhsSlot = graph.records.header.targetNode(rel)
        val rhsSlot = nodeView.records.header.slotFor(node)

        assertIsNode(lhsSlot)
        assertIsNode(rhsSlot)

        val records = graph.records.join(nodeView.records)(lhsSlot, rhsSlot)
        InternalCypherGraph(records)
      }
    }

    sealed trait JoinBuilder {
      def on(lhsKey: Var)(rhsKey: Var): SparkCypherGraph
    }

    private def assertIsNode(slot: RecordSlot): Unit = {
      slot.content.cypherType match {
        case CTNode =>
        case x => throw new IllegalArgumentException(s"Expected $slot to contain a node, but was $x")
      }
    }

    final case class InternalCypherGraph(records: SparkCypherRecords) extends SparkCypherGraph {
      override def domain: SparkCypherGraph = graph.domain
      override def model: QueryModel[Expr] = ???

      override def space: SparkGraphSpace = ???
      override def schema: Schema = ???
      override def nodes(v: Var): SparkCypherGraph = ???
      override def relationships(v: Var): SparkCypherGraph = ???
    }
  }
}
