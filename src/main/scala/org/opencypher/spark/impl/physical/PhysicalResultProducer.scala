package org.opencypher.spark.impl.physical

import org.apache.spark.sql.Row
import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir.global._
import org.opencypher.spark.api.ir.pattern.{AnyGiven, EveryNode}
import org.opencypher.spark.api.record._
import org.opencypher.spark.api.spark.SparkCypherRecords
import org.opencypher.spark.api.types.{CTBoolean, CTNode}
import org.opencypher.spark.api.value.CypherValue
import org.opencypher.spark.impl.exception.Raise
import org.opencypher.spark.impl.instances.spark.SparkSQLExprMapper.asSparkSQLExpr
import org.opencypher.spark.impl.instances.spark.records._
import org.opencypher.spark.impl.logical.NamedLogicalGraph
import org.opencypher.spark.impl.spark.SparkColumnName
import org.opencypher.spark.impl.syntax.transform._
import org.opencypher.spark.impl.syntax.expr._

object RuntimeContext {
  val empty = RuntimeContext(Map.empty, TokenRegistry.empty, ConstantRegistry.empty)
}

case class RuntimeContext(parameters: Map[ConstantRef, CypherValue], tokens: TokenRegistry, constants: ConstantRegistry) {
  def columnName(slot: RecordSlot): String = SparkColumnName.of(slot)
  def columnName(content: SlotContent): String = SparkColumnName.of(content)
}

class PhysicalResultProducer(context: RuntimeContext) {

  implicit val c = context

  implicit final class RichCypherResult(val prev: PhysicalResult) {
    def nodeScan(inGraph: NamedLogicalGraph, v: Var, labels: EveryNode, header: RecordHeader): PhysicalResult = {
      val graph = prev.graphs(inGraph.name)

      val records = graph.nodes(v.name)

      // TODO: Should not discard prev records here
      prev.mapRecordsWithDetails(_ => records)
    }

    def relationshipScan(inGraph: NamedLogicalGraph, v: Var, header: RecordHeader): PhysicalResult = {
      val graph = prev.graphs(inGraph.name)

      val records = graph.relationships(v.name)

      // TODO: Should not discard prev records here
      prev.mapRecordsWithDetails(_ => records)
    }

    def filter(expr: Expr, header: RecordHeader): PhysicalResult = {
      prev.mapRecordsWithDetails { subject =>
        val filteredRows = asSparkSQLExpr(subject.header, expr, subject.data) match {
          case Some(sqlExpr) =>
            subject.data.where(sqlExpr)
          case None =>
            val predicate = cypherFilter(header, expr)
            subject.data.filter(liftTernary(predicate))
        }

        val selectedColumns = header.slots.map { c =>
          val name = context.columnName(c)
          filteredRows.col(name)
        }

        val newData = filteredRows.select(selectedColumns: _*)

        SparkCypherRecords.create(header, newData)(subject.space)
      }
    }

    def alias(expr: Expr, v: Var, header: RecordHeader): PhysicalResult =
      prev.mapRecordsWithDetails { subject =>
        val oldSlot = subject.header.slotsFor(expr).head

        val newSlot = header.slotsFor(v).head

        val oldColumnName = context.columnName(oldSlot)
        val newColumnName = context.columnName(newSlot)

        val newData = if (subject.data.columns.contains(oldColumnName)) {
          subject.data.withColumnRenamed(oldColumnName, newColumnName)
        } else {
          Raise.columnNotFound(oldColumnName)
        }

        SparkCypherRecords.create(header, newData)(subject.space)
      }

    def project(expr: Expr, header: RecordHeader): PhysicalResult =
      prev.mapRecordsWithDetails { subject =>
        val newData = asSparkSQLExpr(header, expr, subject.data) match {
          case None => Raise.notYetImplemented(s"projecting $expr")

          case Some(sparkSqlExpr) =>
            val headerNames = header.slotsFor(expr).map(context.columnName)
            val dataNames = subject.data.columns.toSeq

            // TODO: Can optimise for var AS var2 case -- avoid duplicating data
            headerNames.diff(dataNames) match {
              case Seq(one) =>
                // align the name of the column to what the header expects
                val newCol = sparkSqlExpr.as(one)
                val columnsToSelect = subject.data.columns.map(subject.data.col) :+ newCol

                subject.data.select(columnsToSelect: _*)
              case _ => Raise.multipleSlotsForExpression()
            }
        }

        SparkCypherRecords.create(header, newData)(subject.space)
      }

    def select(fields: IndexedSeq[Var], header: RecordHeader): PhysicalResult =
      prev.mapRecordsWithDetails { subject =>
        val fieldIndices = fields.zipWithIndex.toMap

        val groupedSlots = header.slots.sortBy {
          _.content match {
            case content: FieldSlotContent =>
              fieldIndices.getOrElse(content.field, Int.MaxValue)
            case content@ProjectedExpr(expr) =>
              val deps = expr.dependencies
              deps.headOption.filter(_ => deps.size == 1).flatMap(fieldIndices.get).getOrElse(Int.MaxValue)
          }
        }

        val data = subject.data
        val columns = groupedSlots.map { s => data.col(context.columnName(s)) }
        val newData = subject.data.select(columns: _*)

        SparkCypherRecords.create(header, newData)(subject.space)
      }

    def typeFilter(rel: Var, types: AnyGiven[RelTypeRef], header: RecordHeader): PhysicalResult = {
      if (types.elts.isEmpty) prev
      else {
        val typeExprs: Set[Expr] = types.elts.map { ref => HasType(rel, context.tokens.relType(ref))(CTBoolean) }
        prev.filter(Ors(typeExprs), header)
      }
    }

    def joinTarget(nodeView: PhysicalResult, header: RecordHeader) = new JoinBuilder {
      override def on(rel: Var)(node: Var) = {
        val lhsSlot = prev.records.withDetails.header.targetNode(rel)
        val rhsSlot = nodeView.records.withDetails.header.slotFor(node)

        assertIsNode(lhsSlot)
        assertIsNode(rhsSlot)

        prev.mapRecordsWithDetails(join(nodeView.records, lhsSlot, rhsSlot, header))
      }
    }

    def joinNode(nodeView: PhysicalResult, header: RecordHeader) = new JoinBuilder {
      override def on(endNode: Var)(node: Var) = {
        val lhsSlot = prev.records.header.slotFor(endNode)
        val rhsSlot = nodeView.records.header.slotFor(node)

        assertIsNode(lhsSlot)
        assertIsNode(rhsSlot)

        prev.mapRecordsWithDetails(join(nodeView.records, lhsSlot, rhsSlot, header))
      }
    }

    def expandSource(relView: PhysicalResult, header: RecordHeader) = new JoinBuilder {
      override def on(node: Var)(rel: Var) = {
        val lhsSlot = prev.records.withDetails.header.slotFor(node)
        val rhsSlot = relView.records.withDetails.header.sourceNode(rel)

        assertIsNode(lhsSlot)
        assertIsNode(rhsSlot)

        prev.mapRecordsWithDetails(join(relView.records, lhsSlot, rhsSlot, header))
      }
    }

    private def join(rhs: SparkCypherRecords, lhsSlot: RecordSlot, rhsSlot: RecordSlot, header: RecordHeader)
    : SparkCypherRecords => SparkCypherRecords = {
      def f(lhs: SparkCypherRecords) = {
        if (lhs.space == rhs.space) {
          val lhsData = lhs.withDetails.data
          val rhsData = rhs.withDetails.data
          val lhsColumn = lhsData.col(context.columnName(lhsSlot))
          val rhsColumn = rhsData.col(context.columnName(rhsSlot))

          val joinExpr = lhsColumn === rhsColumn
          val jointData = lhsData.join(rhsData, joinExpr, "inner")

          SparkCypherRecords.create(header, jointData)(lhs.space)
        } else {
          Raise.graphSpaceMismatch()
        }
      }
      f
    }

    def initVarExpand(source: Var, edgeList: Var, endNode: Var, header: RecordHeader): InternalResult = {
      val sourceSlot = header.slotFor(source)
      val edgeListSlot = header.slotFor(edgeList)
      val endNodeSlot = header.slotFor(endNode)

      assertIsNode(endNodeSlot)

      prev.mapRecordsWithDetails(_.initVarExpand(sourceSlot, edgeListSlot, endNodeSlot, header))
    }

    def varExpand(rels: PhysicalResult, edgeList: Var, endNode: Var, rel: Var, lower: Int, upper: Int, header: RecordHeader) = {
        val startSlot = rels.records.header.sourceNode(rel)
        val endNodeSlot = prev.records.header.slotFor(endNode)

        prev.mapRecordsWithDetails(_.varExpand(rels.records, lower, upper, header)(edgeList, endNodeSlot, rel, startSlot))
      }

    sealed trait JoinBuilder {
      def on(lhsKey: Var)(rhsKey: Var): PhysicalResult
    }

    private def assertIsNode(slot: RecordSlot): Unit = {
      slot.content.cypherType match {
        case CTNode =>
        case x => throw new IllegalArgumentException(s"Expected $slot to contain a node, but was $x")
      }
    }
  }

  private def liftTernary(f: Row => Option[Boolean]): (Row => Boolean) = {
    (r: Row) =>
      f(r) match {
        case None => false
        case Some(x) => x
      }
  }
}

