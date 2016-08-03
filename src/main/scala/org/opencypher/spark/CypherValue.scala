package org.opencypher.spark

import java.lang

import org.apache.spark.sql.Encoder

import scala.reflect.ClassTag

object CypherValue {
  trait implicits {
    implicit def cypherValueEncoder[T <: CypherValue : ClassTag]: Encoder[T] = org.apache.spark.sql.Encoders.kryo[T]
    implicit def cypherEntityIdEncoder = org.apache.spark.sql.Encoders.kryo[EntityId]
  }
  object implicits extends implicits
}

sealed trait CypherValue extends Any {
  type Repr
  def v: Repr
}

final case class CypherString(v: String) extends AnyVal with CypherValue {
  type Repr = String
}


final case class CypherBoolean(v: Boolean) extends AnyVal with CypherValue {
  type Repr = Boolean
}

sealed trait ToNumber extends Any {
  self: CypherValue =>

  def toNumber: Number
}

final case class CypherInteger(v: Long) extends AnyVal with CypherValue with ToNumber {
  type Repr = Long

  override def toNumber: lang.Long = v.toLong
}

final case class CypherFloat(v: Double) extends AnyVal with CypherValue with ToNumber {
  type Repr = Double

  override def toNumber: lang.Float = v.toFloat
}

object CypherList {
  def empty = CypherList(Seq.empty)
}

final case class CypherList(v: Seq[CypherValue]) extends AnyVal with CypherValue {
  type Repr = Seq[CypherValue]
}

object CypherMap {
  val empty = CypherMap(Map.empty)
}

final case class CypherMap(v: Map[String, CypherValue]) extends AnyVal with CypherValue with HasProperties {
  type Repr = Map[String, CypherValue]

  def properties = v
}

sealed trait HasProperties extends Any {
  self: CypherValue =>

  def properties: Map[String, CypherValue]
}

object EntityId {
  val ordering = Ordering.by[EntityId, Long](_.v)
}

final case class EntityId(v: Long) extends AnyVal with HasEntityId {
  self =>

  override def toString = s"#$v"
  override def id: EntityId = self
}

object HasEntityId {
  val ordering = Ordering.by[HasEntityId, EntityId](_.id)(EntityId.ordering)
}

sealed trait HasEntityId extends Any {
  def id: EntityId
}

final case class CypherNode(id: EntityId, labels: Seq[String], properties: Map[String, CypherValue] = Map.empty) extends CypherValue with HasEntityId with HasProperties {
  type Repr = (Long, CypherNode)
  def v = id.v -> this
}

final case class CypherRelationship(id: EntityId, start: EntityId, end: EntityId, typ: String, properties: Map[String, CypherValue] = Map.empty) extends CypherValue with HasEntityId with HasProperties {
  type Repr = ((Long, Long), CypherRelationship)
  def v = (start.v -> end.v) -> this

  def other(otherId: EntityId) =
    if (start == otherId) end else start
}

final case class CypherPath(v: Seq[CypherEntityValue]) extends CypherValue {
  // TODO: Validation
  type Repr = Seq[CypherEntityValue]
}
