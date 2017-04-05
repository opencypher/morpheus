package org.opencypher.spark_legacy.impl.frame

import org.opencypher.spark_legacy.impl.StdSlot
import org.opencypher.spark_legacy.impl.error.StdErrorInfo
import org.opencypher.spark_legacy.impl.verify.Verification
import org.opencypher.spark.api.types._

import scala.language.postfixOps

object FrameVerification {

  abstract class Error(detail: String)(implicit private val info: StdErrorInfo)
    extends Verification.Error(detail) {
    self: Product with Serializable =>
  }

  abstract class TypeError(msg: String)(implicit info: StdErrorInfo) extends Error(msg) {
    self: Product with Serializable =>
  }

  final case class IsNoSuperTypeOf(actualType: CypherType, baseType: CypherType)(implicit info: StdErrorInfo)
    extends TypeError(
      s"Supertype expected, but $actualType is not a supertype of $baseType"
    )(info)

  final case class IsNoSubTypeOf(actualType: CypherType, baseType: CypherType)(implicit info: StdErrorInfo)
    extends TypeError(
      s"Subtype expected, but $actualType is not a subtype of $baseType"
    )(info)

  final case class UnInhabitedMeetType(lhsType: CypherType, rhsType: CypherType)(implicit info: StdErrorInfo)
    extends TypeError(s"There is no value of both type $lhsType and $rhsType")(info)

  final case class FrameSignatureMismatch(msg: String)(implicit info: StdErrorInfo)
    extends Error(msg)(info)

  final case class SlotNotEmbeddable(key: Symbol)(implicit info: StdErrorInfo)
    extends Error(s"Cannot use slot $key that relies on a non-embedded representation")(info)
}

trait FrameVerification extends Verification with StdErrorInfo.Implicits {

  import FrameVerification._

  protected def requireInhabitedMeetType(lhsType: CypherType, rhsType: CypherType) =
    ifNot((lhsType meet rhsType).isInhabited.maybeTrue) failWith UnInhabitedMeetType(lhsType, rhsType)

  protected def requireIsSuperTypeOf(newType: CypherType, oldType: CypherType) =
    ifNot(newType `superTypeOf` oldType isTrue) failWith IsNoSuperTypeOf(newType, oldType)

  protected def requireEmbeddedRepresentation(lhsKey: Symbol, lhsSlot: StdSlot) =
    ifNot(lhsSlot.representation.isEmbedded) failWith SlotNotEmbeddable(lhsKey)

  protected def requireMateriallyIsSubTypeOf(actualType: CypherType, materialType: MaterialCypherType) =
    ifNot(actualType.material `subTypeOf` materialType isTrue) failWith IsNoSubTypeOf(actualType, materialType)
}
