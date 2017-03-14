package org.opencypher.spark.prototype.impl.physical

import org.apache.spark.sql.Column
import org.opencypher.spark.prototype.api.expr.{Ands, Expr, HasLabel, Var}
import org.opencypher.spark.prototype.api.ir.QueryModel
import org.opencypher.spark.prototype.api.ir.global.LabelRef
import org.opencypher.spark.prototype.api.ir.pattern.AllGiven
import org.opencypher.spark.prototype.api.record.ProjectedSlotContent
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
        val lhsHeader = view.records.header
        val rhsHeader = relView.records.header

        val lhsSlot = lhsHeader.slotFor(node)
        val rhsSlot = rhsHeader.sourceNode(rel)

        val lhsDF = view.records.data
        val rhsDF = relView.records.data

        val lhsColumn = lhsDF.col(lhsDF.columns(lhsSlot.index))
        val rhsColumn = rhsDF.col(rhsDF.columns(rhsSlot.index))

        val joinExpr: Column = lhsColumn === rhsColumn
        val joined = lhsDF.join(rhsDF, joinExpr)

        val records = new SparkCypherRecords {
          override def data = joined
          override def header = lhsHeader ++ rhsHeader
        }

        SparkCypherRecordsView(records)
      }
    }

    /*
     * This only makes sense if `rel` exists and is a relationship
     */
    def joinTarget(nodeView: SparkCypherView) = new JoinBuilder {
      override def on(rel: Var)(node: Var) = {
        val lhsHeader = view.records.header
        val rhsHeader = nodeView.records.header

        val lhsSlot = lhsHeader.targetNode(rel)
        val rhsSlot = rhsHeader.slotFor(node)

        val lhsDF = view.records.data
        val rhsDF = nodeView.records.data

        val lhsColumn = lhsDF.col(lhsDF.columns(lhsSlot.index))
        val rhsColumn = rhsDF.col(rhsDF.columns(rhsSlot.index))

        val joinExpr: Column = lhsColumn === rhsColumn
        val joined = lhsDF.join(rhsDF, joinExpr)

        val records = new SparkCypherRecords {
          override def data = joined
          override def header = lhsHeader ++ rhsHeader
        }

        SparkCypherRecordsView(records)
      }
    }

    sealed trait JoinBuilder {
      def on(lhsKey: Var)(rhsKey: Var): SparkCypherView
    }

    final case class SparkCypherRecordsView(records: SparkCypherRecords) extends SparkCypherView {
      override def domain: SparkCypherGraph = view.domain
      override def graph: SparkCypherGraph = ???
      override def model: QueryModel[Expr] = ???
    }
  }
}
