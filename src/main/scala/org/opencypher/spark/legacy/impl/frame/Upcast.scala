package org.opencypher.spark.legacy.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.legacy.impl._
import org.opencypher.spark.prototype.api.types.CypherType

import scala.language.postfixOps

object Upcast extends FrameCompanion {

  def apply[Out](input: StdCypherFrame[Out])(fieldSym: Symbol)(widen: CypherType => CypherType)
                (implicit context: PlanningContext): StdCypherFrame[Out] = {

    val field = obtain(input.signature.field)(fieldSym)
    val oldType = field.cypherType
    val newType = widen(oldType)

    requireIsSuperTypeOf(newType, oldType)

    val (_, sig) = input.signature.upcastField(field.sym -> newType)
    CypherUpcast[Out](input)(sig)
  }

  private final case class CypherUpcast[Out](input: StdCypherFrame[Out])(sig: StdFrameSignature)
    extends StdCypherFrame[Out](sig) {

    override def execute(implicit context: RuntimeContext): Dataset[Out] = {
      val out = input.run
      out
    }
  }
}
