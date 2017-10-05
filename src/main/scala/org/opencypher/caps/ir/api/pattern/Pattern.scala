/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencypher.caps.ir.api.pattern

import org.opencypher.caps.impl.spark.exception.Raise
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types.{CTNode, CTRelationship}
import org.opencypher.caps.ir.api._
import org.opencypher.caps.ir.api.block.Binds

import scala.annotation.tailrec

case object Pattern {
  def empty[E]: Pattern[E] = Pattern[E](entities = Map.empty, topology = Map.empty)
  def node[E](entry: (IRField, EveryEntity)): Pattern[E] = Pattern[E](entities = Map(entry), topology = Map.empty)
}

final case class Pattern[E](entities: Map[IRField, EveryEntity], topology: Map[IRField, Connection]) extends Binds[E] {

  lazy val nodes: Map[IRField, EveryNode] = entities.collect { case (k, v: EveryNode) => k -> v }
  lazy val rels: Map[IRField, EveryRelationship] = entities.collect { case (k, v: EveryRelationship) => k -> v }

  override def fields: Set[IRField] = entities.keySet

  /**
    * Fuse patterns but fail if they disagree in the definitions of entities or connections
    *
    * @return A pattern that contains all entities and connections of their input
    */
  def ++(other: Pattern[E]): Pattern[E] = {
    val entityFields = entities.keySet ++ other.entities.keySet
    val newEntities = entityFields.foldLeft(Map.empty[IRField, EveryEntity]) { case (m, f) =>
      val candidates = entities.get(f).toSet ++ other.entities.get(f).toSet
      if (candidates.size == 1) m.updated(f, candidates.head)
      else Raise.invalidArgument("disjoint patterns", s"conflicting entities $f")
    }

    val topologyFields = topology.keySet ++ other.topology.keySet
    val newTopology = topologyFields.foldLeft(Map.empty[IRField, Connection]) { case (m, f) =>
      val candidates = topology.get(f).toSet ++ other.topology.get(f).toSet
      if (candidates.size == 1) m.updated(f, candidates.head)
      else Raise.invalidArgument("disjoint patterns", s"conflicting connections $f")
    }

    Pattern(newEntities, newTopology)
  }

  def connectionsFor(node: IRField): Map[IRField, Connection] = {
    topology.filter {
      case (_, c) => c.endpoints.contains(node)
    }
  }

  def withoutConnection(rel: IRField): Pattern[E] = {
    val c = topology(rel)
    copy(entities = entities - c.source - c.target - rel,
      topology = topology - rel)
  }

  def solvedNode(key: IRField): Pattern[E] = {
    copy(entities = entities - key)
  }

  def isEmpty: Boolean = this == Pattern.empty

  def withConnection(key: IRField, connection: Connection): Pattern[E] =
    if (topology.get(key).contains(connection)) this else copy(topology = topology.updated(key, connection))

  def withEntity(key: IRField, value: EveryEntity): Pattern[E] =
    if (entities.get(key).contains(value)) this else copy(entities = entities.updated(key, value))

  def components: Set[Pattern[E]] = {
    val fields = entities.keySet.foldLeft(Map.empty[IRField, Int]) { case (m, f) => m.updated(f, m.size) }
    val components = nodes.foldLeft(Map.empty[Int, Pattern[E]]) { case (m, entry@(f, _)) => m.updated(fields(f), Pattern.node(entry)) }
    val input = topology.toSeq.flatMap { case (f, c) => entities.get(f).map { e => (f, e, c) } }
    computeComponents(input, components, fields.size, fields)
  }

  @tailrec
  private def computeComponents(
    input: Seq[(IRField, EveryEntity, Connection)],
    components: Map[Int, Pattern[E]],
    count: Int,
    fields: Map[IRField, Int]
  ): Set[Pattern[E]] = input match {
    case Seq((field, entity, connection), tail@_*) =>
      val endpoints = connection.endpoints.toSet
      val links = endpoints.flatMap(fields.get).toSet

      if (links.isEmpty) {
        // Connection forms a new connected component on its own
        val newCount = count + 1
        val newPattern = Pattern[E](
          entities = endpoints.flatMap(field => entities.get(field).map(field -> _)).toMap,
          topology = Map(field -> connection)
        ).withEntity(field, entity)
        val newComponents = components.updated(count, newPattern)
        val newFields = endpoints.foldLeft(fields) { case (m, endpoint) => m.updated(endpoint, count) }
        computeComponents(tail, newComponents, newCount, newFields)
      } else if (links.size == 1) {
        // Connection should be added to a single, existing component
        val link = links.head
        val oldPattern = components(link) // This is not supposed to fail
        val newPattern = oldPattern
          .withConnection(field, connection)
          .withEntity(field, entity)
        val newComponents = components.updated(link, newPattern)
        computeComponents(tail, newComponents, count, fields)
      } else {
        // Connection bridges two connected components
        val fusedPattern = links.flatMap(components.get).reduce(_ ++ _)
        val newPattern = fusedPattern
          .withConnection(field, connection)
          .withEntity(field, entity)
        val newCount = count + 1
        val newComponents = links
          .foldLeft(components) { case (m, l) => m - l }
          .updated(newCount, newPattern)
        val newFields = fields.mapValues(l => if (links(l)) newCount else l)
        computeComponents(tail, newComponents, newCount, newFields)
      }

    case Seq() =>
      components.values.toSet
  }

}

