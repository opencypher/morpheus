/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.okapi.ir.api.pattern

import org.opencypher.okapi.api.types.{CTNode, CTRelationship, CypherType}
import org.opencypher.okapi.ir.api._
import org.opencypher.okapi.ir.api.block.Binds
import org.opencypher.okapi.ir.api.expr.{EquivalenceModel, MapExpression}
import org.opencypher.okapi.ir.impl.exception.PatternConversionException

import scala.annotation.tailrec

case object Pattern {
  def empty[E]: Pattern[E] = Pattern[E](fields = Set.empty, topology = Map.empty)

  def node[E](node: IRField): Pattern[E] = Pattern[E](fields = Set(node), topology = Map.empty)
}

final case class Pattern[E](
  fields: Set[IRField],
  topology: Map[IRField, Connection],
  equivalences: Map[IRField, EquivalenceModel] = Map.empty,
  properties: Map[IRField, MapExpression] = Map.empty
) extends Binds[E] {

  lazy val nodes: Set[IRField] = getEntity(CTNode)
  lazy val rels: Set[IRField] = getEntity(CTRelationship)

  private def getEntity(t: CypherType) =
    fields.collect { case e if e.cypherType.subTypeOf(t).maybeTrue => e }

  /**
    * Fuse patterns but fail if they disagree in the definitions of entities or connections
    *
    * @return A pattern that contains all entities and connections of their input
    */
  def ++(other: Pattern[E]): Pattern[E] = {
    val thisMap = fields.map(f => f.name -> f.cypherType).toMap
    val otherMap = other.fields.map(f => f.name -> f.cypherType).toMap

    verifyFieldTypes(thisMap, otherMap)

    val conflicts = topology.keySet.intersect(other.topology.keySet).filter(k => topology(k) != other.topology(k))
    if (conflicts.nonEmpty) throw new PatternConversionException(
      s"Expected disjoint patterns but found conflicting connection for ${conflicts.head}:\n" +
        s"${topology(conflicts.head)} and ${other.topology(conflicts.head)}")
    val newTopology = topology ++ other.topology
    Pattern(fields ++ other.fields, newTopology, equivalences ++ other.equivalences, properties ++ other.properties)
  }

  private def verifyFieldTypes(map1: Map[String, CypherType], map2: Map[String, CypherType]): Unit = {
    (map1.keySet ++ map2.keySet).foreach { f =>
      map1.get(f) -> map2.get(f) match {
        case (Some(t1), Some(t2)) =>
          if (t1 != t2)
            throw PatternConversionException(s"Expected disjoint patterns but found conflicting entities $f")
        case _ =>
      }
    }
  }

  def connectionsFor(node: IRField): Map[IRField, Connection] = {
    topology.filter {
      case (_, c) => c.endpoints.contains(node)
    }
  }

  def isEmpty: Boolean = this == Pattern.empty

  def withConnection(key: IRField, connection: Connection, propertiesOpt: Option[MapExpression] = None): Pattern[E] = {
    val withProperties: Pattern[E] = propertiesOpt match {
      case Some(props) => copy(properties = properties.updated(key, props))
      case None => this
    }

    if (topology.get(key).contains(connection)) withProperties else withProperties.copy(topology = topology.updated(key, connection))
  }

  def withEntity(field: IRField, propertiesOpt: Option[MapExpression] = None, equivalence: Option[EquivalenceModel] = None): Pattern[E] = {
    val withProperties: Pattern[E] = propertiesOpt match {
      case Some(props) => copy(properties = properties.updated(field, props))
      case None => this
    }

    equivalence match {
      case None => if (fields(field)) withProperties else withProperties.copy(fields = fields + field)
      case Some(e) =>
        if (fields(field)) withProperties.copy(equivalences = equivalences + (field -> e))
        else withProperties.copy(fields = fields + field, equivalences = equivalences + (field -> e))
    }
  }

  def components: Set[Pattern[E]] = {
    val _fields = fields.foldLeft(Map.empty[IRField, Int]) { case (m, f) => m.updated(f, m.size) }
    val components = nodes.foldLeft(Map.empty[Int, Pattern[E]]) {
      case (m, f) => m.updated(_fields(f), Pattern.node(f))
    }
    computeComponents(topology.toSeq, components, _fields.size, _fields)
  }

  @tailrec
  private def computeComponents(
    input: Seq[(IRField, Connection)],
    components: Map[Int, Pattern[E]],
    count: Int,
    fieldToComponentIndex: Map[IRField, Int]
  ): Set[Pattern[E]] = input match {
    case Seq((field, connection), tail@_*) =>
      val endpoints = connection.endpoints.toSet
      val links = endpoints.flatMap(fieldToComponentIndex.get)

      if (links.isEmpty) {
        // Connection forms a new connected component on its own
        val newCount = count + 1
        val newPattern = Pattern[E](
          fields = fields intersect endpoints,
          topology = Map(field -> connection)
        ).withEntity(field)
        val newComponents = components.updated(count, newPattern)
        val newFields = endpoints.foldLeft(fieldToComponentIndex) { case (m, endpoint) => m.updated(endpoint, count) }
        computeComponents(tail, newComponents, newCount, newFields)
      } else if (links.size == 1) {
        // Connection should be added to a single, existing component
        val link = links.head
        val oldPattern = components(link) // This is not supposed to fail
        val newPattern = oldPattern
          .withConnection(field, connection)
          .withEntity(field)
        val newComponents = components.updated(link, newPattern)
        computeComponents(tail, newComponents, count, fieldToComponentIndex)
      } else {
        // Connection bridges two connected components
        val fusedPattern = links.flatMap(components.get).reduce(_ ++ _)
        val newPattern = fusedPattern
          .withConnection(field, connection)
          .withEntity(field)
        val newCount = count + 1
        val newComponents = links
          .foldLeft(components) { case (m, l) => m - l }
          .updated(newCount, newPattern)
        val newFields = fieldToComponentIndex.mapValues(l => if (links(l)) newCount else l)
        computeComponents(tail, newComponents, newCount, newFields)
      }

    case Seq() =>
      components.values.toSet
  }

}
