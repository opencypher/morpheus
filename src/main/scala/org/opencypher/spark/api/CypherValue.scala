package org.opencypher.spark.api

import java.lang

import org.apache.spark.sql.Encoders.kryo
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Encoder, Encoders => SparkEncoders}
import org.opencypher.spark.api.types._

import scala.language.implicitConversions

object CypherValue {

  object Conversion extends Conversion

  trait Conversion extends LowPriorityConversion {

    implicit def cypherString(v: String): CypherString = CypherString(v)
    implicit def cypherInteger(v: Int): CypherInteger = CypherInteger(v)
    implicit def cypherInteger(v: Long): CypherInteger = CypherInteger(v)
    implicit def cypherFloat(v: Float): CypherFloat = CypherFloat(v)
    implicit def cypherFloat(v: Double): CypherFloat = CypherFloat(v)
    implicit def cypherBoolean(v: Boolean): CypherBoolean = CypherBoolean(v)
    implicit def cypherTernary(v: Ternary): CypherValue = if (v.isDefinite) CypherBoolean(v.isTrue) else CypherNull

    implicit def cypherOption[T](v: Option[T])(implicit ev: T => CypherValue): CypherValue =
      v.map(ev).getOrElse(CypherNull)

    implicit def cypherList[T](v: Seq[T])(implicit ev: T => CypherValue): CypherList =
      CypherList(v.map(ev))

    implicit def cypherMap[T](v: Map[String, T])(implicit ev: T => CypherValue): CypherMap =
      CypherMap(v.mapValues(ev))

    implicit def cypherPath(v: Seq[CypherEntityValue]): CypherPath = CypherPath(v: _*)
  }

  trait LowPriorityConversion {

    implicit def mapOfCypherValues[T](v: Map[String, T])(implicit ev: T => CypherValue): Map[String, CypherValue] =
      v.mapValues(ev)

    implicit def entryToCypherValue[T](v: (String, T))(implicit ev: T => CypherValue): (String, CypherValue) =
      v._1 -> v._2
  }

  object Encoders extends Encoders

  trait LowPriorityEncoders {
    implicit def asExpressionEncoder[T](v: Encoder[T]): ExpressionEncoder[T] = v.asInstanceOf[ExpressionEncoder[T]]

    implicit def cypherValueEncoder: ExpressionEncoder[CypherValue] = kryo[CypherValue]
    implicit def cypherRecordEncoder: ExpressionEncoder[Map[String, CypherValue]] = kryo[Map[String, CypherValue]]
  }

  trait Encoders extends LowPriorityEncoders {
    implicit def cypherNodeEncoder: ExpressionEncoder[CypherNode] = kryo[CypherNode]
    implicit def cypherRelationshipEncoder: ExpressionEncoder[CypherRelationship] = kryo[CypherRelationship]
    implicit def cypherPathEncoder: ExpressionEncoder[CypherPath] = kryo[CypherPath]

    implicit def cypherTuple1Encoder[T: ExpressionEncoder]: ExpressionEncoder[Tuple1[T]] =
      ExpressionEncoder.tuple(Seq(implicitly[ExpressionEncoder[T]])).asInstanceOf[ExpressionEncoder[Tuple1[T]]]

    implicit def cypherTuple2Encoder[T1: ExpressionEncoder, T2: ExpressionEncoder]: ExpressionEncoder[(T1, T2)] =
      SparkEncoders.tuple(
        implicitly[ExpressionEncoder[T1]],
        implicitly[ExpressionEncoder[T2]]
      )

    implicit def cypherTuple3Encoder[T1: ExpressionEncoder, T2: ExpressionEncoder, T3: ExpressionEncoder]: ExpressionEncoder[(T1, T2, T3)] =
      SparkEncoders.tuple(
        implicitly[ExpressionEncoder[T1]],
        implicitly[ExpressionEncoder[T2]],
        implicitly[ExpressionEncoder[T3]]
      )

    implicit def cypherTuple4Encoder[T1: ExpressionEncoder, T2: ExpressionEncoder, T3: ExpressionEncoder, T4: ExpressionEncoder]: ExpressionEncoder[(T1, T2, T3, T4)] =
      SparkEncoders.tuple(
        implicitly[ExpressionEncoder[T1]],
        implicitly[ExpressionEncoder[T2]],
        implicitly[ExpressionEncoder[T3]],
        implicitly[ExpressionEncoder[T4]]
      )

    implicit def cypherTuple5Encoder[T1: ExpressionEncoder, T2: ExpressionEncoder, T3: ExpressionEncoder, T4: ExpressionEncoder, T5: ExpressionEncoder]: ExpressionEncoder[(T1, T2, T3, T4, T5)] =
      SparkEncoders.tuple(
        implicitly[ExpressionEncoder[T1]],
        implicitly[ExpressionEncoder[T2]],
        implicitly[ExpressionEncoder[T3]],
        implicitly[ExpressionEncoder[T4]],
        implicitly[ExpressionEncoder[T5]]
      )
  }
}

sealed trait CypherValue extends Any {
  type Repr
  def v: Repr

  def cypherType: CypherType
}

case object CypherNull extends CypherValue {
  override type Repr = this.type
  override def v = this

  def cypherType = CTNull
}

final case class CypherString(v: String) extends AnyVal with CypherValue {
  type Repr = String

  def cypherType = CTString
}

final case class CypherBoolean(v: Boolean) extends AnyVal with CypherValue {
  type Repr = Boolean

  def cypherType = CTBoolean
}

final case class CypherInteger(v: Long) extends AnyVal with CypherValue with IsNumber {
  type Repr = Long

  override def number: lang.Long = v.toLong

  def cypherType = CTInteger
}

final case class CypherFloat(v: Double) extends AnyVal with CypherValue with IsNumber {
  type Repr = Double

  override def number: lang.Double = v.toDouble

  def cypherType = CTFloat
}

object CypherList {
  def of(elts: CypherValue*) = CypherList(elts)
  val empty = CypherList(Seq.empty)
}

final case class CypherList(v: Seq[CypherValue]) extends AnyVal with CypherValue {
  type Repr = Seq[CypherValue]

  def cypherType = CTList(CTAny)
}

object CypherMap {
  def of(elts: (String, CypherValue)*) = CypherMap(Map(elts: _*))
  val empty = CypherMap(Map.empty)
}

final case class CypherMap(v: Map[String, CypherValue]) extends AnyVal with CypherValue with HasProperties {
  type Repr = Map[String, CypherValue]

  def properties = v

  def cypherType = CTMap
}

final case class CypherNode(id: EntityId, labels: Seq[String], properties: Map[String, CypherValue]) extends CypherValue with HasEntityId with HasProperties {
  type Repr = (Long, CypherNode)
  def v = id.v -> this

  def cypherType = CTNode
}

final case class CypherRelationship(id: EntityId, startId: EntityId, endId: EntityId, relationshipType: String, properties: Map[String, CypherValue]) extends CypherValue with HasEntityId with HasProperties {
  type Repr = ((Long, Long), CypherRelationship)

  def v = (startId.v -> endId.v) -> this

  def other(otherId: EntityId) =
    if (startId == otherId)
      endId
    else {
      if (endId == otherId)
        startId
      else
        throw new IllegalArgumentException(s"Expected either start $startId or end $endId of relationship $id, but got: $otherId")
    }

  def cypherType = CTRelationship
}

final case class CypherPath(v: CypherEntityValue*) extends CypherValue {
  // TODO: Validation
  type Repr = Seq[CypherEntityValue]

  def cypherType = CTPath
}

sealed trait IsNumber extends Any {
  self: CypherValue =>

  def number: Number
}

sealed trait HasEntityId extends Any {
  def id: EntityId
}

sealed trait HasProperties extends Any {
  self: CypherValue =>

  def properties: Map[String, CypherValue]
}



