package org.opencypher.spark

import java.lang

import org.apache.spark.sql.{Encoder, Encoders}
import org.opencypher.spark.CypherTypes._

import scala.reflect.ClassTag

object CypherValue {
  trait implicits {
    implicit def cypherValueEncoder[T <: CypherValue : ClassTag]: Encoder[T] = Encoders.kryo[T]
    // TODO: Add more
    implicit def cypherTuple2Encoder[T1: Encoder, T2: Encoder]: Encoder[(T1, T2)] = Encoders.tuple(implicitly[Encoder[T1]], implicitly[Encoder[T2]])
    implicit def cypherTuple3Encoder[T1: Encoder, T2: Encoder, T3: Encoder]: Encoder[(T1, T2, T3)] = Encoders.tuple(implicitly[Encoder[T1]], implicitly[Encoder[T2]], implicitly[Encoder[T3]])
    implicit def cypherTuple4Encoder[T1: Encoder, T2: Encoder, T3: Encoder, T4: Encoder]: Encoder[(T1, T2, T3, T4)] = Encoders.tuple(implicitly[Encoder[T1]], implicitly[Encoder[T2]], implicitly[Encoder[T3]], implicitly[Encoder[T4]])
    implicit def cypherEntityIdEncoder = Encoders.kryo[EntityId]

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

sealed trait ToNumber extends Any {
  self: CypherValue =>

  def toNumber: Number
}

final case class CypherInteger(v: Long) extends AnyVal with CypherValue with ToNumber {
  type Repr = Long

  override def toNumber: lang.Long = v.toLong

  def cypherType = CTInteger
}

final case class CypherFloat(v: Double) extends AnyVal with CypherValue with ToNumber {
  type Repr = Double

  override def toNumber: lang.Double = v.toDouble

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

  def cypherType = CTNode
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

  def cypherType = CTRelationship
}

final case class CypherPath(v: Seq[CypherEntityValue]) extends CypherValue {
  // TODO: Validation
  type Repr = Seq[CypherEntityValue]

  def cypherType = CTPath
}
