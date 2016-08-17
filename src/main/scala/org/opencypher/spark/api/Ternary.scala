package org.opencypher.spark.api

import scala.language.implicitConversions

object Ternary {

  object Conversion extends Conversion

  trait Conversion {
    implicit def booleanAsTernary(b: Boolean): Ternary = Ternary(b)
    implicit def optionalBooleanAsTernary(optB: Option[Boolean]): Ternary = Ternary(optB)
    implicit def optionalIntAsTernary(optI: Option[Int]): Ternary = Ternary.fromComparison(optI)
  }

  def apply(v: Boolean): Ternary = if (v) True else False
  def apply(v: Option[Boolean]): Ternary = v.map(Ternary(_)).getOrElse(Maybe)

  def fromComparison(v: Option[Int]): Ternary = v.map(cmp => Ternary(cmp == 0)).getOrElse(Maybe)
}

sealed trait Ternary {
  def isTrue: Boolean
  def isFalse: Boolean
  def isDefinite: Boolean
  def isUnknown: Boolean

  def maybeTrue: Boolean
  def maybeFalse: Boolean

  def negated: Ternary
}

sealed private[spark] trait DefiniteTernary extends Ternary {
  def isDefinite: Boolean = true
  def isUnknown: Boolean  = false
}

case object True extends DefiniteTernary {
  def isTrue = true
  def isFalse = false

  def maybeTrue = true
  def maybeFalse = false

  def negated = False

  override def toString = "definitely true"
}

case object False extends DefiniteTernary {
  def isTrue = false
  def isFalse = true

  def maybeTrue = false
  def maybeFalse = true

  def negated = True

  override def toString = "definitely false"
}

case object Maybe extends Ternary {
  def isTrue = false
  def isFalse = false
  def isDefinite = false
  def isUnknown = true

  def maybeTrue = true
  def maybeFalse = true

  def negated = Maybe

  override def toString = "maybe"
}
