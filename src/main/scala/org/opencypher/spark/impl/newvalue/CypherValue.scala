package org.opencypher.spark.impl.newvalue

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.Encoders._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.opencypher.spark.api.{CypherFloat, CypherInteger, CypherList, CypherMap, CypherPath, CypherString, Ternary, _}

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

  //  object Conversion extends Conversion
  //
  //  trait Conversion extends LowPriorityConversion {
  //
  //    implicit def cypherString(v: String): CypherString = CypherString(v)
  //    implicit def cypherInteger(v: Int): CypherInteger = CypherInteger(v)
  //    implicit def cypherInteger(v: Long): CypherInteger = CypherInteger(v)
  //    implicit def cypherFloat(v: Float): CypherFloat =  CypherFloat(v)
  //    implicit def cypherFloat(v: Double): CypherFloat = CypherFloat(v)
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
  //  }
  //
  //  trait LowPriorityConversion {
  //
  //    implicit def mapOfCypherValues[T](v: Map[String, T])(implicit ev: T => CypherValue): Map[String, CypherValue] =
  //      v.mapValues(ev)
  //
  //    implicit def entryToCypherValue[T](v: (String, T))(implicit ev: T => CypherValue): (String, CypherValue) =
  //      v._1 -> v._2
  //  }
  //
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
}

sealed trait CypherValue extends AnyRef {
  /**
    * Represents `equivalence` in Cypher, not `equality`. This is confusing, but important.
    * @return true iff this and obj are equivalent.
    */
  override def equals(obj: scala.Any): Boolean
}

object CypherNumber {
  def unapply(value: CypherNumber): Option[Number] = value match {
    case CypherInteger(v) => Some(v)
    case CypherFloat(v) => Some(v)
    case _ => None
  }

  object orderability extends Ordering[CypherNumber] {
    override def compare(x: CypherNumber, y: CypherNumber): Int = (x, y) match {
      case (a: CypherInteger, b: CypherInteger) => CypherInteger.orderability.compare(a, b)
      case (a: CypherFloat, b: CypherFloat) => CypherFloat.orderability.compare(a, b)
      case (a @ CypherFloat(f), b @ CypherInteger(l)) =>
        if (fitsDouble(l))
          CypherFloat.orderability.compare(a, CypherFloat(l))
        else
          ???

      case (a @ CypherInteger(l), b @ CypherFloat(f)) =>
        if (fitsDouble(l))
          CypherFloat.orderability.compare(CypherFloat(l), b)
        else
          ???

      case (null, null) => 0
      case (_, null) => -1
      case (null, _) => 1
    }
  }

  private def fitsDouble(v: Long): Boolean = true
}

sealed trait CypherNumber extends CypherValue

object CypherInteger {
  def apply(value: Long) = new CypherInteger(value)
  def unapply(value: CypherInteger): Option[Long] = if (value == null) None else Some(value.v)

  object orderability extends Ordering[CypherInteger] {
    override def compare(x: CypherInteger, y: CypherInteger): Int = (x, y) match {
      case (null, null) => 0
      case (null, _) => 1
      case (_, null) => -1
      case (CypherInteger(xv), CypherInteger(yv)) => Ordering.Long.compare(xv, yv)
    }
  }
}

final class CypherInteger(private val v: Long) extends CypherNumber {
  override def equals(obj: scala.Any): Boolean = obj match {
    case other: AnyRef if eq(other) => true
    case other: CypherInteger       => CypherInteger.orderability.compare(this, other) == 0
    case _                          => false
  }
}

object CypherFloat {
  def apply(value: Double) = new CypherFloat(value)
  def unapply(value: CypherFloat): Option[Double] = if (value == null) None else Some(value.v)

  object orderability extends Ordering[CypherFloat] {
    override def compare(x: CypherFloat, y: CypherFloat): Int = (x, y) match {
      case (null, null) => 0
      case (null, _) => 1
      case (_, null) => -1
      case (CypherFloat(xv), CypherFloat(yv)) => Ordering.Double.compare(xv, yv)
    }
  }
}

final class CypherFloat(private val v: Double) extends CypherNumber {
  override def equals(obj: scala.Any): Boolean = obj match {
    case other: AnyRef if eq(other) => true
    case other: CypherFloat         => CypherFloat.orderability.compare(this, other) == 0
    case _                          => false
  }
}

