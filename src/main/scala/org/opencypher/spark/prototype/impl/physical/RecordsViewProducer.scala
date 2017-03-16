package org.opencypher.spark.prototype.impl.physical

import org.opencypher.spark.api.types.CTNode
import org.opencypher.spark.prototype.api.expr._
import org.opencypher.spark.prototype.api.ir.QueryModel
import org.opencypher.spark.prototype.api.ir.global.{ConstantRef, LabelRef, RelTypeRef}
import org.opencypher.spark.prototype.api.ir.pattern.{AllGiven, AnyGiven}
import org.opencypher.spark.prototype.api.record.{ProjectedSlotContent, RecordSlot}
import org.opencypher.spark.prototype.api.spark.{SparkCypherGraph, SparkCypherRecords, SparkCypherView}
import org.opencypher.spark.prototype.api.value.CypherValue
import org.opencypher.spark.prototype.impl.instances.spark.records._
import org.opencypher.spark.prototype.impl.syntax.transform._

object RuntimeContext {
  val empty = RuntimeContext(Map.empty)
}

case class RuntimeContext(constants: Map[ConstantRef, CypherValue])

class RecordsViewProducer(context: RuntimeContext) {

  implicit val c = context

  implicit final class RichCypherGraph(val graph: SparkCypherGraph) {
    def allNodes(v: Var): SparkCypherView = graph.nodes(v)
  }

  implicit final class RichCypherView(val view: SparkCypherView) {

    def select(fields: Map[Expr, String]): SparkCypherView =
      InternalCypherView(view.records.select(fields))

    def project(slot: ProjectedSlotContent): SparkCypherView =
      InternalCypherView(view.records.project(slot))

    def filter(expr: Expr): SparkCypherView =
      InternalCypherView(view.records.filter(expr))

    def labelFilter(node: Var, labels: AllGiven[LabelRef]): SparkCypherView = {
      val labelExprs: Set[Expr] = labels.elts.map { ref => HasLabel(node, ref) }
      InternalCypherView(view.records.filter(Ands(labelExprs)))
    }

    def typeFilter(rel: Var, types: AnyGiven[RelTypeRef]): SparkCypherView = {
      if (types.elts.isEmpty) view
      else {
        val typeExprs: Set[Expr] = types.elts.map { ref => HasType(rel, ref) }
        InternalCypherView(view.records.filter(Ands(typeExprs)))
      }
    }

    def expandSource(relView: SparkCypherView) = new JoinBuilder {
      override def on(node: Var)(rel: Var) = {
        val lhsSlot = view.records.header.slotFor(node)
        val rhsSlot = relView.records.header.sourceNode(rel)

        assertIsNode(lhsSlot)
        assertIsNode(rhsSlot)

        val records = view.records.join(relView.records)(lhsSlot, rhsSlot)
        InternalCypherView(records)
      }
    }

    def joinTarget(nodeView: SparkCypherView) = new JoinBuilder {
      override def on(rel: Var)(node: Var) = {
        val lhsSlot = view.records.header.targetNode(rel)
        val rhsSlot = nodeView.records.header.slotFor(node)

        assertIsNode(lhsSlot)
        assertIsNode(rhsSlot)

        val records = view.records.join(nodeView.records)(lhsSlot, rhsSlot)
        InternalCypherView(records)
      }
    }

    sealed trait JoinBuilder {
      def on(lhsKey: Var)(rhsKey: Var): SparkCypherView
    }

    private def assertIsNode(slot: RecordSlot): Unit = {
      slot.content.cypherType match {
        case CTNode =>
        case x => throw new IllegalArgumentException(s"Expected $slot to contain a node, but was $x")
      }
    }

    final case class InternalCypherView(records: SparkCypherRecords) extends SparkCypherView {
      override def domain: SparkCypherGraph = view.domain
      override def graph: SparkCypherGraph = ???
      override def model: QueryModel[Expr] = ???
    }
  }
}
