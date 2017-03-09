package org.opencypher.spark.prototype.api.value

import scala.language.implicitConversions

case object EntityId {

  implicit object ordering extends Ordering[EntityId] {
    override def compare(x: EntityId, y: EntityId): Int = Ordering.Long.compare(x.v, y.v)
  }

  implicit def apply(v: Long): EntityId = new EntityId(v)
}

// TODO: Prohibit invalid ids
final class EntityId(val v: Long) extends AnyVal with Serializable {
  self =>

  override def toString = s"#$v"
}
