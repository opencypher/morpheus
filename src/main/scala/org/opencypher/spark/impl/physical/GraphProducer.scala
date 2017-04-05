package org.opencypher.spark.impl.physical

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Literal
import org.opencypher.spark.api.types.CTNode
import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir.global.{ConstantRef, LabelRef, RelTypeRef}
import org.opencypher.spark.api.ir.pattern.{AllGiven, AnyGiven}
import org.opencypher.spark.api.ir.{Field, QueryModel}
import org.opencypher.spark.api.record._
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.spark.{SparkCypherGraph, SparkCypherRecords, SparkGraphSpace}
import org.opencypher.spark.api.value.CypherValue
import org.opencypher.spark.impl.instances.spark.records._
import org.opencypher.spark.impl.spark.{SparkColumnName, toSparkType}
import org.opencypher.spark.impl.syntax.header._
import org.opencypher.spark.impl.syntax.transform._

object RuntimeContext {
  val empty = RuntimeContext(Map.empty)
}

case class RuntimeContext(constants: Map[ConstantRef, CypherValue]) {
  def columnName(slot: RecordSlot): String = SparkColumnName.of(slot)
  def columnName(content: SlotContent): String = SparkColumnName.of(content)
}

class GraphProducer(context: RuntimeContext) {

  implicit val c = context

  implicit final class RichCypherGraph(val graph: SparkCypherGraph) {
    def allNodes(v: Var): SparkCypherGraph = graph.nodes(v)

    def allRelationships(v: Var): SparkCypherGraph = graph.relationships(v)

    def select(fields: Set[Var]): SparkCypherGraph =
      InternalCypherGraph(
        graph.details.select(fields),
        graph.model.select(fields.map { case Var(n, _) => Field(n)() })
      )

    def project(slot: ProjectedSlotContent): SparkCypherGraph =
      InternalCypherGraph(graph.details.project(slot), graph.model)

    def filter(expr: Expr): SparkCypherGraph =
      InternalCypherGraph(graph.details.filter(expr), graph.model)

    def labelFilter(node: Var, labels: AllGiven[LabelRef]): SparkCypherGraph = {
      val labelExprs: Set[Expr] = labels.elts.map { ref => HasLabel(node, ref) }
      InternalCypherGraph(graph.details.filter(Ands(labelExprs)), graph.model)
    }

    def typeFilter(rel: Var, types: AnyGiven[RelTypeRef]): SparkCypherGraph = {
      if (types.elts.isEmpty) graph
      else {
        val typeExprs: Set[Expr] = types.elts.map { ref => HasType(rel, ref) }
        InternalCypherGraph(graph.details.filter(Ands(typeExprs)), graph.model)
      }
    }

    def expandSource(relView: SparkCypherGraph) = new JoinBuilder {
      override def on(node: Var)(rel: Var) = {
        val lhsSlot = graph.details.header.slotFor(node)
        val rhsSlot = relView.details.header.sourceNode(rel)

        assertIsNode(lhsSlot)
        assertIsNode(rhsSlot)

        val records = graph.details.join(relView.details)(lhsSlot, rhsSlot)
        InternalCypherGraph(records, graph.model)
      }
    }

    def joinTarget(nodeView: SparkCypherGraph) = new JoinBuilder {
      override def on(rel: Var)(node: Var) = {
        val lhsSlot = graph.details.header.targetNode(rel)
        val rhsSlot = nodeView.details.header.slotFor(node)

        assertIsNode(lhsSlot)
        assertIsNode(rhsSlot)

        val records = graph.details.join(nodeView.details)(lhsSlot, rhsSlot)
        InternalCypherGraph(records, graph.model)
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

    final case class InternalCypherGraph(graphRecords: SparkCypherRecords, graphModel: QueryModel[Expr]) extends SparkCypherGraph {
      override def nodes(v: Var): SparkCypherGraph = new SparkCypherGraph {
        override def nodes(v: Var) = ???
        override def relationships(v: Var) = SparkCypherGraph.empty(graph.space)

        override def model = graphModel
        override def space = graph.space
        override def details = {
          val nodes = graphModel.result.nodes
          // TODO: Assert no duplicate entries
          val oldIndices: Map[SlotContent, Int] = graphRecords.header.slots.flatMap { slot: RecordSlot =>
              slot.content match {
                case p: ProjectedSlotContent =>
                  p.expr match {
                    case HasLabel(Var(name, _), label, _) if nodes(Field(name)()) => Some(ProjectedExpr(HasLabel(v, label), p.cypherType) -> slot.index)
                    case Property(Var(name, _), key, _) if nodes(Field(name)()) => Some(ProjectedExpr(Property(v, key), p.cypherType)-> slot.index)
                    case _ => None
                  }

                case o@OpaqueField(Var(name, _), _) if nodes(Field(name)()) => Some(OpaqueField(v, CTNode) -> slot.index)
                case _ => None
              }
          }.toMap

          // TODO: Check result for failure to add
          val (newHeader, _) = RecordHeader.empty.update(addContents(oldIndices.keySet.toSeq))
          val newIndices = newHeader.slots.map(slot => slot.content -> slot.index).toMap
          val mappingTable = oldIndices.map {
            case (content, oldIndex) => oldIndex -> newIndices(content)
          }

          val frames = nodes.map { nodeField =>
            val nodeVar = Var(nodeField.name)
            val nodeIndices: Seq[(Int, Option[Int])] = oldIndices.map {
              case (_, oldIndex) =>
                if (graphRecords.header.slots(oldIndex).content.owner.contains(nodeVar))
                  mappingTable(oldIndex) -> Some(oldIndex)
                else
                  mappingTable(oldIndex) -> None
            }.toSeq.sortBy(_._1)

            val columns = nodeIndices.map {
              case (newIndex, Some(oldIndex)) =>
                new Column(graphRecords.data.columns(oldIndex)).as(SparkColumnName.of(newHeader.slots(newIndex)))
              case (newIndex, None) =>
                val slot = newHeader.slots(newIndex)
                new Column(Literal(null, toSparkType(slot.content.cypherType))).as(SparkColumnName.of(slot.content))
            }
            graphRecords.data.select(columns: _*)

          }

          val finalDf = frames.reduce(_ union _)

          new SparkCypherRecords {
            override def data = finalDf
            override def header = newHeader
          }
        }

        override def schema = ???
      }
      override def relationships(v: Var): SparkCypherGraph = ???

      override def space: SparkGraphSpace = graph.space
      override def schema: Schema = graph.schema

      override def model: QueryModel[Expr] = graphModel
      override def details: SparkCypherRecords = graphRecords
    }
  }
}
