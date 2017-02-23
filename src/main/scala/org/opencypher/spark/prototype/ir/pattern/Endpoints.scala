package org.opencypher.spark.prototype.ir.pattern

import org.opencypher.spark.prototype.ir.Field

import scala.language.implicitConversions

sealed trait Endpoints extends Traversable[Field] {
  def contains(f: Field): Boolean
}

case object Endpoints {

  def apply(source: Field, target: Field): Endpoints =
    if (source == target) OneSingleEndpoint(source) else TwoDifferentEndpoints(source, target)

  implicit def one(field: Field): IdenticalEndpoints =
    OneSingleEndpoint(field)

  implicit def two(fields: (Field, Field)): Endpoints = {
    val (source, target) = fields
    apply(source, target)
  }

  private case class OneSingleEndpoint(field: Field) extends IdenticalEndpoints {
    override def contains(f: Field) = field == f
  }
  private case class TwoDifferentEndpoints(source: Field, target: Field) extends DifferentEndpoints {
    override def flip = copy(target, source)

    override def contains(f: Field) = f == source || f == target
  }
}

sealed trait IdenticalEndpoints extends Endpoints {
  def field: Field

  final override def foreach[U](f: (Field) => U): Unit = f(field)
}

sealed trait DifferentEndpoints extends Endpoints {
  def source: Field
  def target: Field

  def flip: DifferentEndpoints

  override def foreach[U](f: (Field) => U): Unit = { f(source); f(target) }
}
