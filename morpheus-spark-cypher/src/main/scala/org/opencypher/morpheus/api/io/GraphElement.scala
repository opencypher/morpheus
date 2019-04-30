/*
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
package org.opencypher.morpheus.api.io

import org.opencypher.okapi.api.graph.{SourceEndNodeKey, SourceIdKey, SourceStartNodeKey}

import scala.annotation.StaticAnnotation

sealed trait GraphElement extends Product {
  def id: Long
}

object GraphElement {
  val sourceIdKey: String = SourceIdKey.name
  val nodeSourceIdKey: String = s"node_${SourceIdKey.name}"
  val relationshipSourceIdKey: String = s"relationship_${SourceIdKey.name}"
}

/**
  * If a node has no label annotation, then the class name is used as its label.
  * If a `Labels` annotation, for example `@Labels("Person", "Mammal")`, is present,
  * then the labels from that annotation are used instead.
  */
trait Node extends GraphElement

object Relationship {
  val sourceStartNodeKey: String = SourceStartNodeKey.name

  val sourceEndNodeKey: String = SourceEndNodeKey.name

  val nonPropertyAttributes: Set[String] = Set(GraphElement.sourceIdKey, sourceStartNodeKey, sourceEndNodeKey)
}

/**
  * If a relationship has no type annotation, then the class name in upper case is used as its type.
  * If a `Type` annotation, for example `@RelationshipType("FRIEND_OF")` is present,
  * then the type from that annotation is used instead.
  */
trait Relationship extends GraphElement {
  def source: Long

  def target: Long
}

/**
  * Annotation to use when mapping a case class to a node with more than one label, or a label different to the class name.
  *
  * {{{
  *   @ Labels("Person", "Employee")
  *   case class Employee(id: Long, name: String, salary: Double)
  * }}}
  *
  * @param labels the labels that the node has.
  */
case class Labels(labels: String*) extends StaticAnnotation

/**
  * Annotation to use when mapping a case class to a relationship with a different relationship type to the class name.
  *
  * {{{
  *   @ RelationshipType("FRIEND_OF")
  *   case class Friend(id: Long, src: Long, dst: Long, since: Int)
  * }}}
  *
  * @param relType the relationship type that the relationship has.
  */
case class RelationshipType(relType: String) extends StaticAnnotation
