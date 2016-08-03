package org.opencypher.spark

import java.lang

import org.apache.spark.sql.Encoder

import scala.reflect.ClassTag

object CypherValue {
  trait implicits {
    implicit def cypherValueEncoder[T <: CypherValue : ClassTag]: Encoder[T] = org.apache.spark.sql.Encoders.kryo[T]
    implicit def cypherEntityIdEncoder = org.apache.spark.sql.Encoders.kryo[EntityId]

    implicit def cypherString(v: String): CypherString = CypherString(v)
    implicit def cypherInteger(v: Int): CypherInteger = CypherInteger(v)
    implicit def cypherInteger(v: Long): CypherInteger = CypherInteger(v)
    implicit def cypherFloat(v: Float): CypherFloat = CypherFloat(v)
    implicit def cypherFloat(v: Double): CypherFloat = CypherFloat(v)
    implicit def cypherBoolean(v: Boolean): CypherBoolean = CypherBoolean(v)
    implicit def cypherPair[T](v: (String, T))(implicit ev: T => CypherValue): (String, CypherValue) = v._1 -> v._2
    implicit def cypherList[T](v: Seq[T])(implicit ev: T => CypherValue): CypherList = CypherList(v.map(ev))
    implicit def cypherMap[T](v: Map[String, T])(implicit ev: T => CypherValue): CypherMap = CypherMap(v.mapValues(ev))
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

  override def toNumber: lang.Double = v.toDouble
}

object CypherList {
  def of(elts: CypherValue*) = CypherList(elts)
  val empty = CypherList(Seq.empty)
}

final case class CypherList(v: Seq[CypherValue]) extends AnyVal with CypherValue {
  type Repr = Seq[CypherValue]
}

object CypherMap {
  def of(elts: (String, CypherValue)*) = CypherMap(Map(elts: _*))
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

final case class EntityId(v: Long) extends AnyVal {
  self =>

  override def toString = s"#$v"
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
    if (start == otherId)
      end
    else {
      if (end == otherId)
        start
      else
        throw new IllegalArgumentException(s"Expected either start $start or end $end of relationship $id, but got: $otherId")
    }
}

final case class CypherPath(v: Seq[CypherEntityValue]) extends CypherValue {
  // TODO: Validation
  type Repr = Seq[CypherEntityValue]
}
