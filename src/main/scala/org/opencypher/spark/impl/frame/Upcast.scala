package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.api.CypherType
import org.opencypher.spark.impl.{FrameVerificationError, PlanningContext, StdCypherFrame, StdFrameSignature}

object Upcast {

  import org.opencypher.spark.impl.FrameVerification._

  def apply[Out](input: StdCypherFrame[Out])(fieldSym: Symbol)(widen: CypherType => CypherType)
                (implicit context: PlanningContext): StdCypherFrame[Out] = {

    val field = input.signature.field(fieldSym)
    val oldType = field.cypherType
    val newType = widen(oldType)

    unless(newType `superTypeOf` oldType isTrue) failWith TypeError(oldType, newType)

    val (_, sig) = input.signature.upcastField(field.sym, newType)
    new CypherUpcast[Out](input)(sig)
  }

  private final case class CypherUpcast[Out](input: StdCypherFrame[Out])(sig: StdFrameSignature)
    extends StdCypherFrame[Out](sig) {

    override def execute(implicit context: RuntimeContext): Dataset[Out] = {
      val out = input.run
      out
    }
  }

  protected[frame] final case class TypeError(oldType: CypherType, newType: CypherType)
    extends FrameVerificationError(
      s"Expected $oldType to be upcast to a wider type, but $newType is not a super type of $oldType"
    )
}
