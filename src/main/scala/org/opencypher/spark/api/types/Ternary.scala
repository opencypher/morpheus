package org.opencypher.spark.api.types

import scala.language.implicitConversions

object Ternary {
  implicit def apply(v: Boolean): Ternary = if (v) True else False
  implicit def apply(v: Option[Boolean]): Ternary = v.map(Ternary(_)).getOrElse(Maybe)
}

sealed trait Ternary {
  def isTrue: Boolean
  def isFalse: Boolean
  def isDefinite: Boolean
  def isUnknown: Boolean

  def maybeTrue: Boolean
  def maybeFalse: Boolean

  def and(other: Ternary): Ternary
  def or(other: Ternary): Ternary

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

  def and(other: Ternary): Ternary = other match {
    case True => True
    case False => False
    case Maybe => Maybe
  }

  def or(other: Ternary): Ternary = other match {
    case True => True
    case False => True
    case Maybe => True
  }

  def negated = False

  override def toString = "definitely true"
}

case object False extends DefiniteTernary {
  def isTrue = false
  def isFalse = true

  def maybeTrue = false
  def maybeFalse = true

  def and(other: Ternary): Ternary = other match {
    case True => False
    case False => False
    case Maybe => False
  }

  def or(other: Ternary): Ternary = other match {
    case True => True
    case False => False
    case Maybe => Maybe
  }

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

  def and(other: Ternary): Ternary = other match {
    case True => Maybe
    case False => False
    case Maybe => Maybe
  }

  def or(other: Ternary): Ternary = other match {
    case True => True
    case False => Maybe
    case Maybe => Maybe
  }

  def negated = Maybe

  override def toString = "maybe"
}
