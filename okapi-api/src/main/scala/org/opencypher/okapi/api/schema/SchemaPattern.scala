/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.okapi.api.schema

object SchemaPattern{
  def apply(
    sourceLabel: String,
    relType: String,
    targetLabel: String
  ): SchemaPattern = SchemaPattern(Set(sourceLabel), relType, Set(targetLabel))
}

/**
  * Describes a (node)-[relationship]->(node) triple in a graph as part of the graph's schema.
  * A pattern only applies to nodes with the exact label combination defined by `source/targetLabels`
  * and relationships with the specified relationship type.
  *
  * @example Given a graph that only contains the following patterns
  *                        {{{(:A)-[r1:REL]->(:C),}}}
  *                        {{{(:B)-[r2:REL]->(:C),}}}
  *                        {{{(:A:B)-[r3:REL]->(:C)}}}
  *
  *          then the schema pattern `SchemaPattern(Set("A"), "REL", Set("C"))` would only apply to `r1`
  *          and the schema pattern `SchemaPattern(Set("A", "B"), "REL", Set("C"))` would only apply to `r3`
  *
  * @param sourceLabels label combination for source nodes
  * @param relType relationship type
  * @param targetLabels label combination for target nodes
  */
case class SchemaPattern(sourceLabels: Set[String], relType: String, targetLabels: Set[String]) {
  override def toString: String = {
    val sourceLabelString = if(sourceLabels.isEmpty) "" else sourceLabels.mkString(":", ":", "")
    val targetLabelString = if(targetLabels.isEmpty) "" else targetLabels.mkString(":", ":", "")
    s"($sourceLabelString)-[:$relType]->($targetLabelString)"
  }
}