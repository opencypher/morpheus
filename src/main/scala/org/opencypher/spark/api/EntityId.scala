package org.opencypher.spark.api

import scala.language.implicitConversions

object EntityId {

  object Conversion extends Conversion

  trait Conversion {
    implicit def intId(v: Int): EntityId = EntityId(v)
    implicit def longId(v: Long): EntityId = EntityId(v)
  }
}

final case class EntityId(v: Long) extends AnyVal {
  self =>

  override def toString = s"#$v"
}
