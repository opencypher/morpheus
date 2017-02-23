package org.opencypher.spark.prototype.ir.token

sealed trait Token {
  def name: String
}

sealed trait TokenRef[D <: Token] {
  def id: Int
}

final case class Label(name: String) extends Token
final case class LabelRef(id: Int) extends TokenRef[Label]

final case class PropertyKey(name: String) extends Token
final case class PropertyKeyRef(id: Int) extends TokenRef[PropertyKey]

final case class RelType(name: String) extends Token
final case class RelTypeRef(id: Int) extends TokenRef[RelType]

final case class Parameter(name: String) extends Token
final case class ParameterRef(id: Int) extends TokenRef[Parameter]



