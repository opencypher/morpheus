package org.opencypher.spark.impl.prototype

trait BasicBlockDef[E] extends BlockDef[E] {
  def given: Given
  def where: Where[E]
}

case object Given {
  val nothing = Given(Set.empty)
}

final case class Given(entities: Set[AnyEntity]) {
  def +(entity: AnyEntity) = if (entities.contains(entity)) this else copy(entities = entities + entity)
}

sealed trait AnyEntity {
  def entity: Field
}

final case class AnyNode(entity: Field) extends AnyEntity

final case class AnyRelationship(from: Field, entity: Field, to: Field, typ: Option[RelTypeRef] = None)
  extends AnyEntity


case object Where {
  def everything[E] = Where[E](Set.empty)
}

final case class Where[E](predicates: Set[E])

case object Yields {
  val nothing = Yields(Set.empty)
}

final case class Yields[E](exprs: Set[E])
