package org.opencypher.spark.api

import scala.language.implicitConversions

case object EntityId {

  def invalid: EntityId = EntityId(-1)

  object ordering extends Ordering[EntityId] {
    override def compare(x: EntityId, y: EntityId): Int =
      Ordering.Long.compare(x.v, y.v)
  }

  object Conversion extends Conversion

  trait Conversion {
    implicit def intId(v: Int): EntityId = EntityId(v)
    implicit def longId(v: Long): EntityId = EntityId(v)
  }
}

// TODO: Prohibit invalid ids
final case class EntityId(v: Long) extends AnyVal {
  self =>

  override def toString = s"#$v"
}
