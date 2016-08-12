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
    implicit def cypherFloat(v: Float): CypherFloat =  CypherFloat(v)
    implicit def cypherFloat(v: Double): CypherFloat = CypherFloat(v)
    implicit def cypherBoolean(v: Boolean): CypherBoolean = CypherBoolean(v)

    implicit def cypherTernary(v: Ternary): CypherValue =
      if (v.isDefinite) CypherBoolean(v.isTrue) else cypherNull

    implicit def cypherOption[T](v: Option[T])(implicit ev: T => CypherValue): CypherValue =
      v.map(ev).getOrElse(cypherNull)

    implicit def cypherList[T](v: IndexedSeq[T])(implicit ev: T => CypherValue): CypherList =
      CypherList(v.map(ev))

    implicit def cypherMap[T](v: Map[String, T])(implicit ev: T => CypherValue): CypherMap =
      CypherMap(v.mapValues(ev))

    implicit def cypherPath(v: Seq[CypherEntity]): CypherPath =
      CypherPath(v: _*)
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

  def unapply(v: Any) = v match {
    case m: MaterialCypherValue => Some(Some(m.value))
    case _ if v == null         => Some(None)
    case _                      => None
  }
}

sealed trait CypherValue extends Any

final case class CypherString(override val value: String) extends AnyVal with CypherValue with IsMaterial {
  override type Value = String

  override def cypherType = CTString
}

object CypherBoolean {
  def apply(v: Boolean): CypherBoolean = if (v) CypherTrue else CypherFalse

  def unapply(v: Any) = v match {
    case b: Boolean => Some(if (b) CypherTrue else CypherFalse)
    case _          => None
  }
}

case object CypherTrue extends CypherValue with IsMaterial with IsBoolean {
  override def value = true
}

case object CypherFalse extends CypherValue with IsMaterial with IsBoolean {
  override def value = false
}

final case class CypherInteger(override val value: Long) extends AnyVal with CypherValue with IsMaterial with IsNumber {
  override type Value = Long

  override def cypherType = CTInteger

  override def number: lang.Long = value.toLong
}

final case class CypherFloat(override val value: Double) extends AnyVal with CypherValue with IsMaterial with IsNumber {
  type Value = Double

  override def cypherType = CTFloat

  override def number: lang.Double = value.toDouble
}

object CypherList {
  def of(elts: CypherValue*) = CypherList(elts.toVector)
  val empty = CypherList(Vector.empty)
}

final case class CypherList(value: IndexedSeq[CypherValue]) extends AnyVal with CypherValue with IsMaterial {
  type Value = Seq[CypherValue]

  override def cypherType = CTList(CTAny.nullable)
}

object CypherMap {
  def of(elts: (String, CypherValue)*) = CypherMap(Map(elts: _*))
  val empty = CypherMap(Map.empty)
}

final case class CypherMap(override val value: Map[String, CypherValue])
  extends AnyVal with CypherValue with IsMaterial with HasProperties {

  type Value = Map[String, CypherValue]

  def properties = value

  override def cypherType = CTMap
}

final case class CypherNode(id: EntityId, labels: Seq[String], properties: Map[String, CypherValue])
  extends CypherValue with IsMaterial with HasEntityId with HasProperties {

  type Value = (EntityId, NodeData)

  override def value = id -> NodeData(labels, properties)

  override def cypherType = CTNode
}

final case class CypherRelationship(id: EntityId,
                                    startId: EntityId,
                                    relationshipType: String,
                                    endId: EntityId,
                                    properties: Map[String, CypherValue])
  extends CypherValue with IsMaterial with HasEntityId with HasProperties {

  type Value = (EntityId, RelationshipData)

  override def value = id -> RelationshipData(startId, relationshipType, endId, properties)

  def other(otherId: EntityId) =
    if (startId == otherId)
      endId
    else {
      if (endId == otherId)
        startId
      else
        throw new IllegalArgumentException(
          s"Expected either start $startId or end $endId of relationship $id, but got: $otherId"
        )
    }

  override def cypherType = CTRelationship
}

final case class CypherPath(override val value: CypherEntity*) extends AnyVal with CypherValue with IsMaterial {
  // TODO: Validation
  type Value = Seq[CypherEntity]

  override def cypherType = CTPath
}

sealed trait IsMaterial extends Any {
  self: CypherValue =>

  type Value
  def value: Value

  def cypherType: MaterialCypherType
}

sealed trait IsBoolean extends Any {
  self: CypherValue with IsMaterial =>

  override type Value = Boolean
  override def cypherType = CTBoolean
}

sealed trait IsNumber extends Any {
  self: CypherValue with IsMaterial =>

  def number: Number
}

sealed trait HasEntityId extends Any {
  self: CypherValue with IsMaterial =>

  override type Value <: (EntityId, EntityData)

  def id: EntityId
}

sealed trait HasProperties extends Any {
  self: CypherValue with IsMaterial  =>

  def properties: Map[String, CypherValue]
}
