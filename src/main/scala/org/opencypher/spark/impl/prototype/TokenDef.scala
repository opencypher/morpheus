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


case object WithEvery {
  def of[T](elts: T*) = WithEvery(elts.toSet)
  def empty[T] = WithEvery[T](Set.empty)
}

final case class WithEvery[T](elts: Set[T])


case object WithAny {
  def of[T](elts: T*) = WithAny(elts.toSet)
  def empty[T] = WithAny[T](Set.empty)
}

final case class WithAny[T](elts: Set[T])
