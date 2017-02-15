package org.opencypher.spark.impl.prototype

sealed trait TokenRef[D <: TokenDef] {
  def id: Int
}

sealed trait TokenDef {
  def name: String
}

final case class LabelDef(name: String) extends TokenDef
final case class LabelRef(id: Int) extends TokenRef[LabelDef]

final case class PropertyKeyDef(name: String) extends TokenDef
final case class PropertyKeyRef(id: Int) extends TokenRef[PropertyKeyDef]

final case class RelTypeDef(name: String) extends TokenDef
final case class RelTypeRef(id: Int) extends TokenRef[RelTypeDef]
