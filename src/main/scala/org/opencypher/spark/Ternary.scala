package org.opencypher.spark

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

case object TTrue extends DefiniteTernary {
  def isTrue = true
  def isFalse = false

  def maybeTrue = true
  def maybeFalse = false

  def negated = TFalse

  override def toString = "definitely true"
}

case object TFalse extends DefiniteTernary {
  def isTrue = false
  def isFalse = true

  def maybeTrue = false
  def maybeFalse = true

  def negated = TTrue

  override def toString = "definitely false"
}

case object TMaybe extends Ternary {
  def isTrue = false
  def isFalse = false
  def isDefinite = false
  def isUnknown = true

  def maybeTrue = true
  def maybeFalse = true

  def negated = TMaybe

  override def toString = "maybe"
}
