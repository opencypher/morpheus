package org.opencypher.spark.prototype.impl.physical

import org.opencypher.spark.api.types.{CTNode, CTRelationship}
import org.opencypher.spark.prototype.api.expr.{Ands, Expr, HasLabel, Var}
import org.opencypher.spark.prototype.api.ir.QueryModel
import org.opencypher.spark.prototype.api.ir.global.LabelRef
import org.opencypher.spark.prototype.api.ir.pattern.AllGiven
import org.opencypher.spark.prototype.api.record.{ProjectedSlotContent, RecordSlot}
import org.opencypher.spark.prototype.api.spark.{SparkCypherGraph, SparkCypherRecords, SparkCypherView}
import org.opencypher.spark.prototype.impl.instances.spark.records._
import org.opencypher.spark.prototype.impl.syntax.transform._

object RecordsViewProducer {

  implicit final class RichCypherGraph(val graph: SparkCypherGraph) {
    def allNodes(v: Var): SparkCypherView = graph.nodes(v)
  }

  implicit final class RichCypherView(val view: SparkCypherView) {

    def select(fields: Map[Expr, String]): SparkCypherView =
      SparkCypherRecordsView(view.records.select(fields))

    def project(slot: ProjectedSlotContent): SparkCypherView =
      SparkCypherRecordsView(view.records.project(slot))

    def labelFilter(node: Var, labels: AllGiven[LabelRef]): SparkCypherView = {
      val labelExprs: Set[Expr] = labels.elts.map { ref => HasLabel(node, ref) }
      SparkCypherRecordsView(view.records.filter(Ands(labelExprs)))
    }

    def expandSource(relView: SparkCypherView) = new JoinBuilder {
      override def on(node: Var)(rel: Var) = {
        val lhsSlot = view.records.header.slotFor(node)
        val rhsSlot = relView.records.header.sourceNode(rel)

        assertIsNode(lhsSlot)
        assertIsNode(rhsSlot)

        val records = view.records.join(relView.records)(lhsSlot, rhsSlot)
        SparkCypherRecordsView(records)
      }
    }

    def joinTarget(nodeView: SparkCypherView) = new JoinBuilder {
      override def on(rel: Var)(node: Var) = {
        val lhsSlot = view.records.header.targetNode(rel)
        val rhsSlot = nodeView.records.header.slotFor(node)

        assertIsNode(lhsSlot)
        assertIsNode(rhsSlot)

        val records = view.records.join(nodeView.records)(lhsSlot, rhsSlot)
        SparkCypherRecordsView(records)
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

    final case class SparkCypherRecordsView(records: SparkCypherRecords) extends SparkCypherView {
      override def domain: SparkCypherGraph = view.domain
      override def graph: SparkCypherGraph = ???
      override def model: QueryModel[Expr] = ???
    }
  }
}
