package org.opencypher.spark.impl.physical

import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir.global.{ConstantRef, GlobalsRegistry, RelTypeRef}
import org.opencypher.spark.api.ir.pattern.{AnyGiven, EveryNode}
import org.opencypher.spark.api.record._
import org.opencypher.spark.api.types.{CTBoolean, CTNode}
import org.opencypher.spark.api.value.CypherValue
import org.opencypher.spark.impl.instances.spark.records._
import org.opencypher.spark.impl.logical.NamedLogicalGraph
import org.opencypher.spark.impl.spark.SparkColumnName
import org.opencypher.spark.impl.syntax.transform._

object RuntimeContext {
  val empty = RuntimeContext(Map.empty, GlobalsRegistry.empty)
}

case class RuntimeContext(constants: Map[ConstantRef, CypherValue], globals: GlobalsRegistry) {
  def columnName(slot: RecordSlot): String = SparkColumnName.of(slot)
  def columnName(content: SlotContent): String = SparkColumnName.of(content)
}

class PhysicalProducer(context: RuntimeContext) {

  implicit val c = context

  implicit final class RichCypherResult(val prev: InternalResult) {
    def nodeScan(inGraph: NamedLogicalGraph, v: Var, labels: EveryNode, header: RecordHeader): InternalResult = {
      val graph = prev.graphs(inGraph.name)

      val records = graph.nodes(v).reorder(header)

      // TODO: Should not discard prev records here
      prev.mapRecords(_ => records)
    }

    def relationshipScan(inGraph: NamedLogicalGraph, v: Var, header: RecordHeader): InternalResult = {
      val graph = prev.graphs(inGraph.name)

      val records = graph.relationships(v).reorder(header)

      // TODO: Should not discard prev records here
      prev.mapRecords(_ => records)
    }

    def filter(expr: Expr, header: RecordHeader): InternalResult =
      prev.mapRecords(_.filter(expr, header))

    def alias(expr: Expr, v: Var, header: RecordHeader): InternalResult =
      prev.mapRecords(_.alias2(expr, v, header))

    def project(expr: Expr, header: RecordHeader): InternalResult =
      prev.mapRecords(_.project(expr, header))

    def select(fields: IndexedSeq[Var], header: RecordHeader): InternalResult =
      prev.mapRecords(_.select(fields, header))

    def typeFilter(rel: Var, types: AnyGiven[RelTypeRef], header: RecordHeader): InternalResult = {
      if (types.elts.isEmpty) prev
      else {
        val typeExprs: Set[Expr] = types.elts.map { ref => HasType(rel, context.globals.relType(ref))(CTBoolean) }
        prev.mapRecords(_.filter(Ands(typeExprs), header))
      }
    }

    def joinTarget(nodeView: InternalResult) = new JoinBuilder {
      override def on(rel: Var)(node: Var) = {
        val lhsSlot = prev.records.header.targetNode(rel)
        val rhsSlot = nodeView.records.header.slotFor(node)

        assertIsNode(lhsSlot)
        assertIsNode(rhsSlot)

        prev.mapRecords(_.join(nodeView.records)(lhsSlot, rhsSlot))
      }
    }

    def expandSource(relView: InternalResult, header: RecordHeader) = new JoinBuilder {
      override def on(node: Var)(rel: Var) = {
        val lhsSlot = prev.records.header.slotFor(node)
        val rhsSlot = relView.records.header.sourceNode(rel)

        assertIsNode(lhsSlot)
        assertIsNode(rhsSlot)

        prev.mapRecords(_.join(relView.records, header)(lhsSlot, rhsSlot))
      }
    }

    sealed trait JoinBuilder {
      def on(lhsKey: Var)(rhsKey: Var): InternalResult
    }

    private def assertIsNode(slot: RecordSlot): Unit = {
      slot.content.cypherType match {
        case CTNode =>
        case x => throw new IllegalArgumentException(s"Expected $slot to contain a node, but was $x")
      }
    }
  }
}
