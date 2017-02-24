package org.opencypher.spark.prototype.ir.pattern

import org.opencypher.spark.prototype.ir._
import org.opencypher.spark.prototype.ir.block.Binds

case object Pattern {
  def empty[E] = Pattern[E](entities = Map.empty, topology = Map.empty)
}

final case class Pattern[E](entities: Map[Field, EveryEntity], topology: Map[Field, Connection]) extends Binds[E] {

  lazy val nodes: Map[Field, EveryNode] = entities.collect { case (k, v: EveryNode) => k -> v }
  lazy val rels: Map[Field, EveryRelationship] = entities.collect { case (k, v: EveryRelationship) => k -> v }

  override def fields = entities.keySet

  def connectionsFor(node: Field): Map[Field, Connection] = {
    topology.filter {
      case (_, c) => c.endpoints.contains(node)
    }
  }

  def solvedConnection(rel: Field): Pattern[E] = {
    val c = topology(rel)
    copy(entities = entities - c.source - c.target - rel,
         topology = topology - rel)
  }

  def solvedNode(key: Field): Pattern[E] = {
    copy(entities = entities - key)
  }

  def solved: Boolean = this == Pattern.empty

  def withConnection(key: Field, connection: Connection): Pattern[E] =
    if (topology.get(key).contains(connection)) this else copy(topology = topology.updated(key, connection))

  def withEntity(key: Field, value: EveryEntity): Pattern[E] =
    if (entities.get(key).contains(value)) this else copy(entities = entities.updated(key, value))
}

