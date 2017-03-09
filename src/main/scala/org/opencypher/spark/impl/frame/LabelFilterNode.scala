package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.prototype.api.value.{CypherNode, CypherValue}
import org.opencypher.spark.impl.{ProductFrame, StdCypherFrame, StdField, StdRuntimeContext}
import org.opencypher.spark.prototype.api.expr.Const

object LabelFilterNode extends FrameCompanion {

  def apply(input: StdCypherFrame[CypherNode])(labels: Seq[String]): StdCypherFrame[CypherNode] = {
    LabelFilterNode(input)(labels)
  }

  private final case class LabelFilterNode(input: StdCypherFrame[CypherNode])(labels: Seq[String])
    extends StdCypherFrame[CypherNode](input.signature) {

    override def execute(implicit context: StdRuntimeContext): Dataset[CypherNode] = {
      val in = input.run
      val out = in.filter(labelFilter(labels))
      out
    }
  }

  private final case class labelFilter(labels: Seq[String]) extends (CypherNode => Boolean) {

    override def apply(node: CypherNode): Boolean = {
      labels.forall(l => CypherNode.labels(node).exists(_.contains(l)))
    }
  }
}

object FilterProduct extends FrameCompanion {

  def labelFilter(input: StdCypherFrame[Product])(node: Symbol, labels: Seq[String]): StdCypherFrame[Product] = {
    LabelFilterProduct(input)(input.signature.slot(node).get.ordinal, labels)
  }

  def paramEqFilter(input: StdCypherFrame[Product])(lhs: Symbol, param: Const): StdCypherFrame[Product] = {
    val lhsIdx = input.signature.slot(lhs).get.ordinal
    ParamEqFilter(input)(lhsIdx, param)
  }

  private final case class ParamEqFilter(input: StdCypherFrame[Product])(lhs: Int, param: Const) extends ProductFrame(input.signature) {
    override protected def execute(implicit context: StdRuntimeContext): Dataset[Product] = {
      val in = input.run
      val out = in.filter(paramEq(lhs, context.paramValue(param)))
      out
    }
  }

  private final case class paramEq(lhs: Int, rhs: CypherValue) extends (Product => Boolean) {
    import org.opencypher.spark.impl.util._

    override def apply(record: Product): Boolean = {
      val value = record.get(lhs)
      inner(value, rhs)
    }

    private def inner(l: Any, r: Any) = {
      l == r
    }
  }

  private final case class LabelFilterProduct(input: StdCypherFrame[Product])(index: Int, labels: Seq[String])
    extends StdCypherFrame[Product](input.signature) {

    override def execute(implicit context: StdRuntimeContext): Dataset[Product] = {
      val in = input.run
      val out = in.filter(labelFilter(index, labels))
      out
    }
  }

  private final case class labelFilter(index: Int, labels: Seq[String]) extends (Product => Boolean) {
    import org.opencypher.spark.impl.util._

    override def apply(record: Product): Boolean = {
      val node = record.getAs[CypherNode](index)
      labels.forall(l => CypherNode.labels(node).exists(_.contains(l)))
    }
  }
}
