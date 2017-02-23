package org.opencypher.spark.prototype.ir.pattern

import org.opencypher.spark.prototype.ir._
import org.opencypher.spark.prototype.ir.pattern.Orientation.{Cyclic, Directed, Undirected}

import scala.language.higherKinds

sealed trait Connection {
  type SELF[XO, XE] <: Connection { type O = XO; type E = XE }
  type O <: Orientation[E]
  type E <: Endpoints

  def orientation: Orientation[E]
  def endpoints: E

  def source: Field
  def target: Field

  def flip: SELF[O, E]

  override def hashCode(): Int = orientation.hash(endpoints, seed)
  override def equals(obj: scala.Any) = super.equals(obj) || (obj != null && equalsIfNotEq(obj))

  protected def seed: Int
  protected def equalsIfNotEq(obj: scala.Any): Boolean
}

sealed trait DirectedConnection extends Connection {
  override type SELF[XO, XE] <: DirectedConnection { type O = XO; type E = XE }
  override type O = Directed.type
  override type E = DifferentEndpoints

  final override def orientation = Directed

  final override def source = endpoints.source
  final override def target = endpoints.target
}

sealed trait UndirectedConnection extends Connection {
  override type SELF[XO, XE] <: UndirectedConnection { type O = XO; type E = XE }
  override type O = Undirected.type
  override type E = DifferentEndpoints

  final override def orientation = Undirected

  final override def source = endpoints.source
  final override def target = endpoints.target
}

sealed trait CyclicConnection extends Connection {
  override type SELF[XO, XE] <: CyclicConnection { type O = XO; type E = XE }
  override type O = Cyclic.type
  override type E = IdenticalEndpoints

  final override def orientation = Cyclic

  final override def source = endpoints.field
  final override def target = endpoints.field
}

case object SingleRelationship {
  val seed = "SimpleConnection".hashCode
}

sealed trait SingleRelationship extends Connection {
  override type SELF[XO, XE] <: SingleRelationship { type O = XO; type E = XE }
  final protected override def seed = SingleRelationship.seed
}

final case class DirectedRelationship(endpoints: DifferentEndpoints)
  extends SingleRelationship with DirectedConnection {

  override type SELF[XO, XE] = DirectedRelationship { type O = XO; type E = XE }

  protected def equalsIfNotEq(obj: scala.Any) = obj match {
    case other: DirectedRelationship => orientation.eqv(endpoints, other.endpoints)
    case _ => false
  }

  override def flip = copy(endpoints.flip)
}

case object DirectedRelationship {
  def apply(source: Field, target: Field): SingleRelationship = Endpoints(source, target) match {
    case ends: IdenticalEndpoints => CyclicRelationship(ends)
    case ends: DifferentEndpoints => DirectedRelationship(ends)
  }
}

final case class UndirectedRelationship(endpoints: DifferentEndpoints)
  extends SingleRelationship with UndirectedConnection {

  override type SELF[XO, XE] = UndirectedRelationship { type O = XO; type E = XE }

  protected def equalsIfNotEq(obj: scala.Any) = obj match {
    case other: UndirectedRelationship => orientation.eqv(endpoints, other.endpoints)
    case _ => false
  }

  override def flip = copy(endpoints.flip)
}

case object UndirectedRelationship {
  def apply(source: Field, target: Field): SingleRelationship = Endpoints(source, target) match {
    case ends: IdenticalEndpoints => CyclicRelationship(ends)
    case ends: DifferentEndpoints => UndirectedRelationship(ends)
  }
}

final case class CyclicRelationship(endpoints: IdenticalEndpoints) extends SingleRelationship with CyclicConnection {

  override type SELF[XO, XE] = CyclicRelationship { type O = XO; type E = XE }

  protected def equalsIfNotEq(obj: scala.Any) = obj match {
    case other: CyclicRelationship => orientation.eqv(endpoints, other.endpoints)
    case _ => false
  }

  override def flip = this
}
