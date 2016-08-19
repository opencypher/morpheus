package org.opencypher.spark.impl.newvalue

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.opencypher.spark.api.types.{CTBoolean, CTFloat, CTInteger, CTNull}
import org.opencypher.spark.api.{Ternary, _}

import scala.language.implicitConversions

// (1) construct them somehow
// (2) Need be AnyRef
// (3) null handling
// (4) orderability/comparability
// (5) equality/equivalence
// (6) Compute compatible hash code
// (7) interaction w spark

// equality/equivalence (one flag)
//
// comparability

sealed trait CypherValueCompanion[V <: CypherValue] extends Equiv[V] {
  def cypherType(v: V) : CypherType
  def scalaValue(v: V): Option[Any]

  def containsNull(v: V): Boolean = isNull(v)
  final def isNull(v: V): Boolean = v == null

  def equiv(l: V, r: V): Boolean =
    if (l eq r) true else orderability.compare(l, r) == 0

  def equal(l: V, r: V): Ternary = {
    val a = materialValue(l)
    if (a.isIndeterminate)
      Maybe
    else {
      val b = materialValue(r)
      if (b.isIndeterminate)
        Maybe
      else if (l eq r)
        True
      else {
        val cmp = comparability.tryCompare(a, b)
        Ternary.fromComparison(cmp)
      }
    }
  }

  object orderability extends Ordering[V] {
    override def compare(x: V, y: V): Int = order(x, y)
  }

  object comparability extends PartialOrdering[MaterialValue[V]] {
    override def tryCompare(x: MaterialValue[V], y: MaterialValue[V]): Option[Int] =
      // TODO: Handle incomparability
      Some(orderability.compare(x.v, y.v))

    override def lteq(x: MaterialValue[V], y: MaterialValue[V]): Boolean = tryCompare(x, y) match {
      case Some(cmp) => cmp <= 0
      case None      => false
    }
  }

  final def materialValue(v: V): MaterialValue[V] =
    MaterialValue(if (containsNull(v)) cypherNull else v)

  protected[newvalue] def order(l: V, r: V): Int = (l, r) match {
    case (null, null) => 0
    case (null, _)    => +1
    case (_, null)    => -1
  }
}

case object CypherValue extends CypherValueCompanion[CypherValue] {

  object Conversion extends Conversion

  trait Conversion extends LowPriorityConversion {
  //
  //    implicit def cypherString(v: String): CypherString = CypherString(v)
    implicit def cypherInteger(v: Int): CypherInteger = CypherInteger(v)
    implicit def cypherInteger(v: Long): CypherInteger = CypherInteger(v)
    implicit def cypherFloat(v: Float): CypherFloat =  CypherFloat(v)
    implicit def cypherFloat(v: Double): CypherFloat = CypherFloat(v)
    implicit def cypherBoolean(v: Boolean): CypherBoolean = CypherBoolean(v)

    implicit def cypherTernary(v: Ternary): CypherValue =
      if (v.isDefinite) CypherBoolean(v.isTrue) else cypherNull

    implicit def cypherOption[T](v: Option[T])(implicit ev: T => CypherValue): CypherValue =
      v.map(ev).getOrElse(cypherNull)

  //    implicit def cypherList[T](v: IndexedSeq[T])(implicit ev: T => CypherValue): CypherList =
  //      CypherList(v.map(ev))
  //
  //    implicit def cypherMap[T](v: Map[String, T])(implicit ev: T => CypherValue): CypherMap =
  //      CypherMap(v.mapValues(ev))
  //
  //    implicit def cypherPath(v: Seq[CypherEntity]): CypherPath =
  //      CypherPath(v: _*)
  }

  trait LowPriorityConversion {
      implicit def mapOfCypherValues[T](v: Map[String, T])(implicit ev: T => CypherValue): Map[String, CypherValue] =
        v.mapValues(ev)

      implicit def entryToCypherValue[T](v: (String, T))(implicit ev: T => CypherValue): (String, CypherValue) =
        v._1 -> v._2
  }

  object Encoders extends Encoders

  trait Encoders extends LowPriorityEncoders {
    //    implicit def cypherNodeEncoder: ExpressionEncoder[CypherNode] = kryo[CypherNode]
    //    implicit def cypherRelationshipEncoder: ExpressionEncoder[CypherRelationship] = kryo[CypherRelationship]
    //    implicit def cypherPathEncoder: ExpressionEncoder[CypherPath] = kryo[CypherPath]
    //
    //    implicit def cypherTuple1Encoder[T: ExpressionEncoder]: ExpressionEncoder[Tuple1[T]] =
    //      ExpressionEncoder.tuple(Seq(implicitly[ExpressionEncoder[T]])).asInstanceOf[ExpressionEncoder[Tuple1[T]]]
    //
    //    implicit def cypherTuple2Encoder[T1: ExpressionEncoder, T2: ExpressionEncoder]: ExpressionEncoder[(T1, T2)] =
    //      SparkEncoders.tuple(
    //        implicitly[ExpressionEncoder[T1]],
    //        implicitly[ExpressionEncoder[T2]]
    //      )
    //
    //    implicit def cypherTuple3Encoder[T1: ExpressionEncoder, T2: ExpressionEncoder, T3: ExpressionEncoder]: ExpressionEncoder[(T1, T2, T3)] =
    //      SparkEncoders.tuple(
    //        implicitly[ExpressionEncoder[T1]],
    //        implicitly[ExpressionEncoder[T2]],
    //        implicitly[ExpressionEncoder[T3]]
    //      )
    //
    //    implicit def cypherTuple4Encoder[T1: ExpressionEncoder, T2: ExpressionEncoder, T3: ExpressionEncoder, T4: ExpressionEncoder]: ExpressionEncoder[(T1, T2, T3, T4)] =
    //      SparkEncoders.tuple(
    //        implicitly[ExpressionEncoder[T1]],
    //        implicitly[ExpressionEncoder[T2]],
    //        implicitly[ExpressionEncoder[T3]],
    //        implicitly[ExpressionEncoder[T4]]
    //      )
    //
    //    implicit def cypherTuple5Encoder[T1: ExpressionEncoder, T2: ExpressionEncoder, T3: ExpressionEncoder, T4: ExpressionEncoder, T5: ExpressionEncoder]: ExpressionEncoder[(T1, T2, T3, T4, T5)] =
    //      SparkEncoders.tuple(
    //        implicitly[ExpressionEncoder[T1]],
    //        implicitly[ExpressionEncoder[T2]],
    //        implicitly[ExpressionEncoder[T3]],
    //        implicitly[ExpressionEncoder[T4]],
    //        implicitly[ExpressionEncoder[T5]]
    //      )
    //  }
  }

  trait LowPriorityEncoders {
    implicit def asExpressionEncoder[T](v: Encoder[T]): ExpressionEncoder[T] = v.asInstanceOf[ExpressionEncoder[T]]
    implicit def cypherValueEncoder: ExpressionEncoder[CypherValue] = kryo[CypherValue]
//    implicit def cypherRecordEncoder: ExpressionEncoder[Map[String, CypherValue]] = kryo[Map[String, CypherValue]]
  }

  implicit def companion[V <: CypherValue : CypherValueCompanion] = implicitly[CypherValueCompanion[V]]

  object Companions extends Companions

  trait Companions extends LowPriorityCompanions {
    implicit def cypherBooleanCompanion: CypherBoolean.type = CypherBoolean
    implicit def cypherFloatCompanion: CypherFloat.type = CypherFloat
    implicit def cypherIntegerCompanion: CypherInteger.type = CypherInteger
    implicit def cypherNumberCompanion: CypherNumber.type = CypherNumber
  }

  trait LowPriorityCompanions {
    implicit def cypherValueCompanion: CypherValue.type = CypherValue
  }

  override def cypherType(value: CypherValue): CypherType =
    value match {
      case v: CypherNumber  => CypherNumber.cypherType(v)
      case v: CypherBoolean => CypherBoolean.cypherType(v)
      case null             => CTNull
    }

  override def scalaValue(value: CypherValue): Option[AnyRef] = value match {
    case v: CypherNumber  => CypherNumber.scalaValue(v)
    case _                => None
  }

  override protected[newvalue] def order(l: CypherValue, r: CypherValue): Int =
    (l, r) match {
      case (a: CypherBoolean, b: CypherBoolean) => CypherBoolean.order(a, b)
      case (a: CypherBoolean, b: CypherNumber)  => -1
      case (a: CypherNumber, b: CypherNumber)   => CypherNumber.order(a, b)
      case (a: CypherNumber, b: CypherBoolean)  => +1
      case _                                    => super.order(l, r)
    }
}

sealed trait CypherValue {
  self: Serializable =>
}

case object CypherBoolean extends CypherValueCompanion[CypherBoolean] {
  val TRUE = new CypherBoolean(true)
  val FALSE = new CypherBoolean(false)

  def apply(value: Boolean): CypherBoolean = if (value) TRUE else FALSE
  def unapply(value: CypherBoolean): Option[Boolean] = if (value == null) None else Some(value.v)

  override def cypherType(value: CypherBoolean) = if (value == null) CTNull else CTBoolean
  override def scalaValue(value: CypherBoolean) = unapply(value).map(boolean2Boolean)

  override protected[newvalue] def order(l: CypherBoolean, r: CypherBoolean): Int =
    (l, r) match {
      case (CypherBoolean(xv), CypherBoolean(yv)) => Ordering.Boolean.compare(xv, yv)
      case _                                      => super.order(l, r)
    }
}

final class CypherBoolean(private val v: Boolean) extends CypherValue with Serializable {
  override def hashCode(): Int = v.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: CypherBoolean => CypherBoolean.equiv(this, other)
    case _                    => false
  }

  override def toString: String = if (v) "TRUE" else "FALSE"
}

sealed trait CypherNumberCompanion[V <: CypherNumber] extends CypherValueCompanion[V] {
  def scalaValue(v: V): Option[Number]
}

case object CypherNumber extends CypherNumberCompanion[CypherNumber] {

  def unapply(value: CypherNumber): Option[Number] = value match {
    case CypherInteger(v) => Some(v)
    case CypherFloat(v)   => Some(v)
    case _                => None
  }

  override def cypherType(value: CypherNumber): CypherType =
    if (value == null) CTNull else value match {
      case v: CypherInteger => CypherInteger.cypherType(v)
      case v: CypherFloat   => CypherFloat.cypherType(v)
    }

  override def scalaValue(v: CypherNumber): Option[Number] = unapply(v)

  override protected[newvalue] def order(l: CypherNumber, r: CypherNumber): Int =
    (l, r) match {
      case (a: CypherInteger, b: CypherInteger) =>
        CypherInteger.order(a, b)

      case (a: CypherFloat, b: CypherFloat) =>
        CypherFloat.order(a, b)

      case (a@CypherFloat(f), b@CypherInteger(i)) =>
        if (fitsDouble(i)) CypherFloat.order(a, CypherFloat(i))
        else BigDecimal.decimal(f).compare(BigDecimal.decimal(i))

      case (a@CypherInteger(i), b@CypherFloat(f)) =>
        if (fitsDouble(i)) CypherFloat.order(CypherFloat(i), b)
        else BigDecimal.decimal(i).compare(BigDecimal.decimal(f))

      case _ =>
        super.order(l, r)
    }

  // TODO: Get code for this
  private def fitsDouble(v: Long): Boolean = true
}

sealed trait CypherNumber extends CypherValue {
  self: Serializable =>
}

case object CypherInteger extends CypherNumberCompanion[CypherInteger] {
  def apply(value: Long): CypherInteger = new CypherInteger(value)
  def unapply(value: CypherInteger): Option[Long] = if (value == null) None else Some(value.v)

  override def cypherType(value: CypherInteger) = if (value == null) CTNull else CTInteger
  override def scalaValue(value: CypherInteger) = unapply(value).map(long2Long)

  override protected[newvalue] def order(l: CypherInteger, r: CypherInteger): Int =
    (l, r) match {
      case (CypherInteger(xv), CypherInteger(yv)) => Ordering.Long.compare(xv, yv)
      case _                                      => super.order(l, r)
    }
}

final class CypherInteger(private val v: Long) extends CypherNumber with Serializable {
  override def hashCode(): Int = v.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: CypherInteger => CypherInteger.equiv(this, other)
    case other: CypherNumber  => CypherNumber.equiv(this, other)
    case _                    => false
  }

  override def toString: String = s"$v :: INTEGER"
}

case object CypherFloat extends CypherNumberCompanion[CypherFloat] {
  def apply(value: Double): CypherFloat = if (value.isNaN) cypherNull else new CypherFloat(value)
  def unapply(value: CypherFloat): Option[Double] = if (value == null) None else Some(value.v)

  override def cypherType(value: CypherFloat) = if (value == null) CTNull else CTFloat
  override def scalaValue(value: CypherFloat) = unapply(value).map(double2Double)

  override protected[newvalue] def order(l: CypherFloat, r: CypherFloat): Int = (l, r) match {
    case (CypherFloat(xv), CypherFloat(yv)) => Ordering.Double.compare(xv, yv)
    case _                                  => super.order(l, r)
  }
}

final class CypherFloat(private val v: Double) extends CypherNumber with Serializable {
  override def hashCode(): Int = v.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: CypherFloat  => CypherFloat.equiv(this, other)
    case other: CypherNumber => CypherNumber.equiv(this, other)
    case _                   => false
  }

  override def toString: String = s"$v :: FLOAT"
}
