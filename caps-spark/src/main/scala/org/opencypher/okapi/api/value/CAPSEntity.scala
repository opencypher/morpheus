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
 */
package org.opencypher.okapi.api.value

import org.opencypher.okapi.api.value.CypherValue._

/**
  * Representation of a Cypher node in the CAPS implementation. A node contains an id of type [[Long]], a set of string labels and a map of properties.
  *
  * @param id         the id of the node, unique within the containing graph.
  * @param labels     the labels of the node.
  * @param properties the properties of the node.
  */
case class CAPSNode(
  override val id: Long,
  override val labels: Set[String] = Set.empty,
  override val properties: CypherMap = CypherMap.empty) extends CypherNode[Long] {

  override type I = CAPSNode

  override def copy(id: Long = id, labels: Set[String] = labels, properties: CypherMap = properties): CAPSNode = {
    CAPSNode(id, labels, properties)
  }

}

/**
  * Representation of a Cypher relationship in the CAPS implementation. A relationship contains an id of type [[Long]], ids of its adjacent nodes, a relationship type and a map of properties.
  *
  * @param id         the id of the relationship, unique within the containing graph.
  * @param source     the id of the source node.
  * @param target     the id of the target node.
  * @param relType    the relationship type.
  * @param properties the properties of the node.
  */
case class CAPSRelationship(
  override val id: Long,
  override val source: Long,
  override val target: Long,
  override val relType: String,
  override val properties: CypherMap = CypherMap.empty) extends CypherRelationship[Long] {

  override type I = CAPSRelationship

  override def copy(id: Long = id, source: Long = source, target: Long = target, relType: String = relType, properties: CypherMap = properties): CAPSRelationship = {
    CAPSRelationship(id, source, target, relType, properties).asInstanceOf[this.type]
  }

}
