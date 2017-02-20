package org.opencypher.spark.impl.prototype

trait BasicBlockDef[E] extends BlockDef[E] {
  def given: Given
  def where: Where[E]
}

case object Given {
  val nothing = Given(entities = Map.empty, topology = Map.empty)
}

final case class Given(entities: Map[Field, EntityDef], topology: Map[Field, Connection]) {

  lazy val nodes: Map[Field, AnyNode] = entities.collect { case (k, v: AnyNode) => k -> v }
  lazy val rels: Map[Field, AnyRelationship] = entities.collect { case (k, v: AnyRelationship) => k -> v }

  def withConnection(key: Field, connection: Connection) =
    if (topology.get(key).contains(connection)) this else copy(topology = topology.updated(key, connection))

  def withEntity(key: Field, value: EntityDef) =
    if (entities.get(key).contains(value)) this else copy(entities = entities.updated(key, value))
}

sealed trait EntityDef

final case class AnyNode(labels: WithEvery[LabelRef] = WithEvery.empty[LabelRef]) extends EntityDef

final case class AnyRelationship(relTypes: WithAny[RelTypeRef] = WithAny.empty[RelTypeRef]) extends EntityDef

sealed trait Connection {
  def endpoints: Set[Field]
  def source: Field
  def target: Field
}

final case class SimpleConnection(source: Field, target: Field) extends Connection {
  override def endpoints = Set(source, target)
  def reverse = copy(target, source)
}

case object UndirectedConnection {
  def apply(fst: Field, snd: Field): UndirectedConnection = UndirectedConnection(Set(fst, snd))
}

// TODO: Can we hide this constructor
final case class UndirectedConnection(endpoints: Set[Field]) extends Connection {
  if (endpoints.size != 2)
    throw new IllegalStateException("Undirected connection must have exactly two endpoints")

  override def source = endpoints.head
  override def target = endpoints.tail.head
}

case object Where {
  def everything[E] = Where[E](Set.empty)
}

final case class Where[E](predicates: Set[E])

case object Yields {
  val nothing = Yields(Set.empty)
}

final case class Yields[E](exprs: Set[E])
