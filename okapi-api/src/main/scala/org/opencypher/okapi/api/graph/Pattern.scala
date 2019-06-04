/**
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.okapi.api.graph

import org.opencypher.okapi.api.graph.Pattern._

sealed trait Direction
case object Outgoing extends Direction
case object Incoming extends Direction
case object Both extends Direction

case class Connection(
  source: Option[PatternElement],
  target: Option[PatternElement],
  direction: Direction,
  lower: Int = 1,
  upper: Int = 1
)

trait PatternElement {
  def name: String
  def labels: Set[String]
}

case class NodeElement(name: String, labels: Set[String]) extends PatternElement
case class RelationshipElement(name: String, labels: Set[String]) extends PatternElement


object Pattern {
  val DEFAULT_NODE_NAME = "node"
  val DEFAULT_REL_NAME = "rel"

  /**
    * Patterns can be ordered by the number of relationships
    */
  implicit case object PatternOrdering extends Ordering[Pattern] {
    override def compare(
      x: Pattern,
      y: Pattern
    ): Int = x.topology.size.compare(y.topology.size)
  }
}


sealed trait Pattern {

  /**
    * All elements contained in the pattern
    *
    * @return elements contained in the pattern
    */
  def elements: Set[PatternElement]

  /**
    * The patterns topology, describing connections between node elements via relationships
    *
    * @return the patterns topology
    */
  def topology: Map[String, Connection]

  //TODO: to support general patterns implement a pattern matching algorithm
  /**
    * Tries to find an bijective mapping between from the search pattern into this pattern.
    *
    * If exactMatch is true, then two elements can be mapped if they CypherTypes are equal,
    * e.g. both are CTNode with the same label set. Otherwise a mapping exists if the search element
    * type is supertype of the target element type.
    *
    * @param searchPattern the pattern for which to find the mapping
    * @param exactMatch controls how elements are matched
    * @return a bijective mapping between the search pattern and the target pattern
    */
  def findMapping(searchPattern: Pattern, exactMatch: Boolean): Option[Map[PatternElement, PatternElement]] = {
    if((exactMatch && searchPattern == this) || (!exactMatch && subTypeOf(searchPattern))) {
      searchPattern -> this match {
        case (searchNode: NodePattern, otherNode: NodePattern) =>
          Some(Map(searchNode.nodeElement -> otherNode.nodeElement))
        case (searchRel: RelationshipPattern, otherRel: RelationshipPattern) =>
          Some(Map(searchRel.relElement -> otherRel.relElement))
        case (search: NodeRelPattern, other: NodeRelPattern) =>
          Some(Map(search.nodeElement -> other.nodeElement, search.relElement -> other.relElement))
        case (search: TripletPattern, other: TripletPattern) =>
          Some(Map(search.sourceElement -> other.sourceElement, search.relElement -> other.relElement, search.targetElement -> other.targetElement))
        case _ => None
      }
    } else {
      None
    }
  }

  /**
    * Returns true if the current pattern is a sub type of the other pattern.
    * A pattern is subtype of another if there is an bijective mapping between this patterns
    * elements into the other pattern's elements and for every mapping the source element type is
    * a subtype of the target element type.
    *
    * @param other reference pattern
    * @return true if this pattern is subtype of the reference pattern
    */
  def subTypeOf(other: Pattern): Boolean

  /**
    * Returns true if the current pattern is a super type of the other pattern.
    * A pattern is super type of another pattern iff the other pattern is subtype of the first pattern.
    *
    * @see [[org.opencypher.okapi.api.graph.Pattern#subTypeOf]]
    * @param other reference
    * @return true if this pattern s supertype of the reference pattern
    */
  def superTypeOf(other: Pattern): Boolean = other.subTypeOf(this)
}

case class NodePattern(nodeLabels: Set[String]) extends Pattern {
  val nodeElement = NodeElement(DEFAULT_NODE_NAME, nodeLabels)

  override def elements: Set[PatternElement] = Set(nodeElement)
  override def topology: Map[String, Connection] = Map.empty

  override def subTypeOf(other: Pattern): Boolean = other match {
    case NodePattern(otherNodeLabels) => nodeLabels.subsetOf(otherNodeLabels) || otherNodeLabels.isEmpty
    case _ => false
  }
}

case class RelationshipPattern(relTypes: Set[String]) extends Pattern {
  val relElement = RelationshipElement(DEFAULT_REL_NAME, relTypes)

  override def elements: Set[PatternElement] = Set(relElement)
  override def topology: Map[String, Connection] = Map.empty

  override def subTypeOf(other: Pattern): Boolean = other match {
    case RelationshipPattern(otherRelTypes) => relTypes.subsetOf(otherRelTypes) || otherRelTypes.isEmpty
    case _ => false
  }
}

case class NodeRelPattern(nodeLabels: Set[String], relTypes: Set[String]) extends Pattern {

  val nodeElement = NodeElement(DEFAULT_NODE_NAME, nodeLabels)
  val relElement = RelationshipElement(DEFAULT_REL_NAME, relTypes)

  override def elements: Set[PatternElement] = {
    Set(
      nodeElement,
      relElement
    )
  }

  override def topology: Map[String, Connection] = Map(
    relElement.name -> Connection(Some(nodeElement), None, Outgoing)
  )

  override def subTypeOf(other: Pattern): Boolean = other match {
    case NodeRelPattern(otherNodeLabels, otherRelTypes) => (nodeLabels.subsetOf(otherNodeLabels) || otherNodeLabels.isEmpty) && (relTypes.subsetOf(otherRelTypes) || otherRelTypes.isEmpty)
    case _ => false
  }
}

case class TripletPattern(sourceNodeLabels: Set[String], relTypes: Set[String], targetNodeLabels: Set[String]) extends Pattern {
  val sourceElement = NodeElement("source_" + DEFAULT_NODE_NAME, sourceNodeLabels)
  val targetElement = NodeElement("target_" + DEFAULT_NODE_NAME, targetNodeLabels)
  val relElement = RelationshipElement(DEFAULT_REL_NAME, relTypes)

  override def elements: Set[PatternElement] = Set(
    sourceElement,
    relElement,
    targetElement
  )

  override def topology: Map[String, Connection] = Map(
    relElement.name -> Connection(Some(sourceElement), Some(targetElement), Outgoing)
  )

  override def subTypeOf(other: Pattern): Boolean = other match {
    case tr: TripletPattern =>
      (sourceNodeLabels.subsetOf(tr.sourceNodeLabels) || tr.sourceNodeLabels.isEmpty) &&
      (relTypes.subsetOf(tr.relTypes) || tr.relTypes.isEmpty) &&
      (targetNodeLabels.subsetOf(tr.targetNodeLabels) || tr.targetNodeLabels.isEmpty)
    case _ => false
  }
}
