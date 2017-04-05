package org.opencypher.spark_legacy.impl.frame

import org.apache.spark.sql.{Dataset, Row, functions}
import org.opencypher.spark_legacy.impl._

object Join extends FrameCompanion {

  def apply(lhs: StdCypherFrame[Row], rhs: StdCypherFrame[Row])
           (lhsKey: Symbol, rhsKey: Symbol)(optional: Boolean): StdCypherFrame[Row] = {
    val lhsField = obtain(lhs.signature.field)(lhsKey)
    val lhsType = lhsField.cypherType
    val lhsSlot = obtain(lhs.signature.fieldSlot)(lhsField)
    val rhsField = obtain(rhs.signature.field)(rhsKey)
    val rhsType = rhsField.cypherType
    val rhsSlot = obtain(rhs.signature.fieldSlot)(rhsField)

    requireInhabitedMeetType(lhsType, rhsType)
    requireEmbeddedRepresentation(lhsKey, lhsSlot)
    requireEmbeddedRepresentation(rhsKey, rhsSlot)

    // TODO: Should the join slots have the same representation?
    if (optional)
      LeftOuter(lhs, rhs)(lhsField, rhsField)
    else
      Inner(lhs, rhs)(lhsField, rhsField)
  }

  private final case class Inner(lhs: StdCypherFrame[Row], rhs: StdCypherFrame[Row])
                                    (lhsKey: StdField, rhsKey: StdField)
    extends StdCypherFrame[Row](lhs.signature ++ rhs.signature) {

    override def execute(implicit context: StdRuntimeContext): Dataset[Row] = {
      val lhsIn = lhs.run
      val rhsIn = rhs.run

      val lhsSlot = obtain(signature.fieldSlot)(lhsKey)
      val rhsSlot = obtain(signature.fieldSlot)(rhsKey)

      val joinExpr = functions.expr(s"${lhsSlot.sym.name} = ${rhsSlot.sym.name}")

      lhsIn.join(rhsIn, joinExpr, "inner")
    }
  }

  private final case class LeftOuter(lhs: StdCypherFrame[Row], rhs: StdCypherFrame[Row])
                                     (lhsKey: StdField, rhsKey: StdField)
    extends StdCypherFrame[Row](lhs.signature ++ rhs.signature) {

    override def execute(implicit context: StdRuntimeContext): Dataset[Row] = {
      val lhsIn = lhs.run
      val rhsIn = rhs.run

      val lhsSlot = obtain(signature.fieldSlot)(lhsKey)
      val rhsSlot = obtain(signature.fieldSlot)(rhsKey)

      val joinExpr = functions.expr(s"${lhsSlot.sym.name} = ${rhsSlot.sym.name}")

      lhsIn.join(rhsIn, joinExpr, "left_outer")
    }
  }
}
