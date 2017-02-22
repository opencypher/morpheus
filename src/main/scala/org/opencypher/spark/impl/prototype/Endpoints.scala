package org.opencypher.spark.impl.prototype

import scala.language.implicitConversions

sealed trait Endpoints extends Traversable[Field]

case object Endpoints {

  def apply(source: Field, target: Field): Endpoints =
    if (source == target) OneSingleEndpoint(source) else TwoDifferentEndpoints(source, target)

  implicit def one(field: Field): IdenticalEndpoints =
    OneSingleEndpoint(field)

  implicit def two(fields: (Field, Field)): Endpoints = {
    val (source, target) = fields
    apply(source, target)
  }

  private case class OneSingleEndpoint(field: Field) extends IdenticalEndpoints
  private case class TwoDifferentEndpoints(source: Field, target: Field) extends DifferentEndpoints {
    def flip = copy(target, source)
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
