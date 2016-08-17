package org.opencypher.spark.impl.newvalue

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.opencypher.spark.api.types.{CTFloat, CTInteger, CTNull}
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
object CypherValue {

  object Conversion extends Conversion

  trait Conversion extends LowPriorityConversion {
  //
  //    implicit def cypherString(v: String): CypherString = CypherString(v)
    implicit def cypherInteger(v: Int): CypherInteger = CypherInteger(v)
    implicit def cypherInteger(v: Long): CypherInteger = CypherInteger(v)
    implicit def cypherFloat(v: Float): CypherFloat =  CypherFloat(v)
    implicit def cypherFloat(v: Double): CypherFloat = CypherFloat(v)
  //    implicit def cypherBoolean(v: Boolean): CypherBoolean = CypherBoolean(v)
  //
  //    implicit def cypherTernary(v: Ternary): CypherValue =
  //      if (v.isDefinite) CypherBoolean(v.isTrue) else cypherNull
  //
  //    implicit def cypherOption[T](v: Option[T])(implicit ev: T => CypherValue): CypherValue =
  //      v.map(ev).getOrElse(cypherNull)
  //
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
  //
  //    implicit def mapOfCypherValues[T](v: Map[String, T])(implicit ev: T => CypherValue): Map[String, CypherValue] =
  //      v.mapValues(ev)
  //
  //    implicit def entryToCypherValue[T](v: (String, T))(implicit ev: T => CypherValue): (String, CypherValue) =
  //      v._1 -> v._2
  }

  object Encoders extends Encoders

  //
  trait LowPriorityEncoders {
    implicit def asExpressionEncoder[T](v: Encoder[T]): ExpressionEncoder[T] = v.asInstanceOf[ExpressionEncoder[T]]
    implicit def cypherValueEncoder: ExpressionEncoder[CypherValue] = kryo[CypherValue]
    //    implicit def cypherRecordEncoder: ExpressionEncoder[Map[String, CypherValue]] = kryo[Map[String, CypherValue]]
  }

  //
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

  def companion[V <: CypherValue : Companion] = implicitly[Companion[V]]

  object Companions extends Companions

  trait Companions {
    implicit def cypherFloatCompanion: CypherFloat.type = CypherFloat
    implicit def cypherIntegerCompanion: CypherInteger.type = CypherInteger
    implicit def cypherNumberCompanion: CypherNumber.type = CypherNumber
  }

  sealed trait Companion[V <: CypherValue] extends Equiv[V] {
    def cypherType(v: V) : CypherType

    def equal(l: V, r: V): Ternary =
      if (l == null || r == null) Maybe
      else if (l eq r) True
      else Ternary.fromComparison(equality.tryCompare(l, r))

    def equiv(l: V, r: V): Boolean = if (l eq r) true else equivalence.compare(l, r) == 0

    object equality extends PartialOrdering[V] {
      override def tryCompare(x: V, y: V): Option[Int] = order(x, y)(Cmp.equality)
      override def lteq(x: V, y: V): Boolean = order(x, y)(Cmp.equality).exists(_ <= 0)
    }

    object equivalence extends Ordering[V] {
      override def compare(x: V, y: V): Int = order(x, y)(Cmp.equivalence).x
    }

    // compare two values under equivalence but map comparison results involving null using the uncertain function
    def order[C <: Option[Int]](l: V, r: V)(implicit cmp: Cmp[C]): C = (l, r) match {
      case (null, null) => cmp.nullable(0)
      case (null, _)    => cmp.nullable(+1)
      case (_, null)    => cmp.nullable(-1)
    }
  }
}

sealed trait CypherValue

object CypherNumber extends CypherValue.Companion[CypherNumber] {

  def unapply(value: CypherNumber): Option[Number] = value match {
    case CypherInteger(v) => Some(v)
    case CypherFloat(v)   => Some(v)
    case _                => None
  }

  def cypherType(value: CypherNumber): CypherType =
    if (value == null) CTNull else value match {
      case v: CypherInteger => CypherInteger.cypherType(v)
      case v: CypherFloat   => CypherFloat.cypherType(v)
    }

  override def order[C <: Option[Int]](l: CypherNumber, r: CypherNumber)(implicit cmp: Cmp[C]): C =
    (l, r) match {
      case (a: CypherInteger, b: CypherInteger) =>
        CypherInteger.order(a, b)

      case (a: CypherFloat, b: CypherFloat) =>
        CypherFloat.order(a, b)

      case (a@CypherFloat(f), b@CypherInteger(i)) =>
        if (fitsDouble(i)) CypherFloat.order(a, CypherFloat(i))
        else cmp.material(BigDecimal.decimal(f).compare(BigDecimal.decimal(i)))

      case (a@CypherInteger(i), b@CypherFloat(f)) =>
        if (fitsDouble(i)) CypherFloat.order(CypherFloat(i), b)
        else cmp.material(BigDecimal.decimal(i).compare(BigDecimal.decimal(f)))

      case _ =>
        super.order(l, r)
    }

  private def fitsDouble(v: Long): Boolean = true
}

sealed trait CypherNumber extends CypherValue

object CypherInteger extends CypherValue.Companion[CypherInteger] {
  def apply(value: Long): CypherInteger = new CypherInteger(value)
  def unapply(value: CypherInteger): Option[Long] = if (value == null) None else Some(value.v)

  def cypherType(value: CypherInteger) = if (value == null) CTNull else CTInteger

  override def order[C <: Option[Int]](l: CypherInteger, r: CypherInteger)(implicit cmp: Cmp[C]): C =
    (l, r) match {
      case (CypherInteger(xv), CypherInteger(yv)) => cmp.material(Ordering.Long.compare(xv, yv))
      case _                                      => super.order(l, r)
    }
}

final class CypherInteger(private val v: Long) extends CypherNumber {
  override def hashCode(): Int = v.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: CypherInteger => CypherInteger.equiv(this, other)
    case other: CypherNumber  => CypherNumber.equiv(this, other)
    case _                    => false
  }

  override def toString: String = s"$v :: INTEGER"
}

object CypherFloat extends CypherValue.Companion[CypherFloat] {
  def apply(value: Double): CypherFloat = if (value.isNaN) cypherNull else new CypherFloat(value)
  def unapply(value: CypherFloat): Option[Double] = if (value == null) None else Some(value.v)

  def cypherType(value: CypherFloat) = if (value == null) CTNull else CTFloat

  override def order[C <: Option[Int]](l: CypherFloat, r: CypherFloat)(implicit cmp: Cmp[C]): C =
    (l, r) match {
      case (CypherFloat(xv), CypherFloat(yv)) => cmp.material(Ordering.Double.compare(xv, yv))
      case _                                  => super.order(l, r)
    }
}

final class CypherFloat(private val v: Double) extends CypherNumber {
  override def hashCode(): Int = v.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: CypherFloat  => CypherFloat.equiv(this, other)
    case other: CypherNumber => CypherNumber.equiv(this, other)
    case _                   => false
  }

  override def toString: String = s"$v :: FLOAT"
}

