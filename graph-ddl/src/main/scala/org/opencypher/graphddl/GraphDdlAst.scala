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
package org.opencypher.graphddl

import org.opencypher.graphddl.GraphDdlAst._
import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.trees.AbstractTreeNode

object GraphDdlAst {
  type Property = (String, CypherType)

  type EntityDefinition = (String, Map[String, CypherType])

  type KeyDefinition = (String, Set[String])

  type ColumnIdentifier = List[String]

  type PropertyToColumnMappingDefinition = Map[String, String]

  type LabelCombination = Set[String]

  type RelationshipType = String
}

abstract class GraphDdlAst extends AbstractTreeNode[GraphDdlAst]

case class DdlDefinition(
  statements: List[DdlStatement]
) extends GraphDdlAst

sealed trait DdlStatement
sealed trait GraphTypeStatement

case class SetSchemaDefinition(
  dataSource: String,
  schema: String
) extends GraphDdlAst with DdlStatement

case class LabelDefinition(
  name: String,
  properties: Map[String, CypherType] = Map.empty,
  maybeKeyDefinition: Option[KeyDefinition] = None
) extends GraphDdlAst with DdlStatement with GraphTypeStatement

case class GraphTypeDefinition(
  name: String,
  graphTypeBody: GraphTypeBody
) extends GraphDdlAst with DdlStatement

case class GraphTypeBody(
  statements: List[GraphTypeStatement] = List.empty
) extends GraphDdlAst

case class GraphDefinition(
  name: String,
  maybeGraphTypeName: Option[String] = None,
  graphTypeBody: GraphTypeBody = GraphTypeBody(),
  mappings: List[MappingDefinition] = List.empty
) extends GraphDdlAst with DdlStatement

object NodeDefinition {
  def apply(labelCombination: String*): NodeDefinition = NodeDefinition(labelCombination.toSet)
}

case class NodeDefinition(
  labelCombination: LabelCombination
) extends GraphDdlAst with GraphTypeStatement

case class RelationshipDefinition(
  label: RelationshipType
) extends GraphDdlAst with GraphTypeStatement

case class CardinalityConstraint(from: Int, to: Option[Int])

case class PatternDefinition(
  sourceLabelCombinations: Set[LabelCombination],
  sourceCardinality: CardinalityConstraint = CardinalityConstraint(0, None),
  relTypes: Set[String],
  targetCardinality: CardinalityConstraint = CardinalityConstraint(0, None),
  targetLabelCombinations: Set[LabelCombination]
) extends GraphDdlAst with GraphTypeStatement

trait ElementToViewDefinition {
  def maybePropertyMapping: Option[PropertyToColumnMappingDefinition]
}

case class NodeToViewDefinition (
  viewId: List[String],
  override val maybePropertyMapping: Option[PropertyToColumnMappingDefinition] = None
) extends GraphDdlAst with ElementToViewDefinition

trait MappingDefinition

case class NodeMappingDefinition(
  nodeDefinition: NodeDefinition,
  nodeToViewDefinitions: List[NodeToViewDefinition] = List.empty
) extends GraphDdlAst with MappingDefinition

case class ViewDefinition(
  viewId: List[String],
  alias: String
) extends GraphDdlAst

case class JoinOnDefinition(joinPredicates: List[(ColumnIdentifier, ColumnIdentifier)]) extends GraphDdlAst

case class LabelToViewDefinition(
  nodeDefinition: NodeDefinition,
  viewDefinition: ViewDefinition,
  joinOn: JoinOnDefinition
) extends GraphDdlAst

case class RelationshipToViewDefinition(
  viewDefinition: ViewDefinition,
  override val maybePropertyMapping: Option[PropertyToColumnMappingDefinition] = None,
  startNodeToViewDefinition: LabelToViewDefinition,
  endNodeToViewDefinition: LabelToViewDefinition
) extends GraphDdlAst with ElementToViewDefinition

case class RelationshipMappingDefinition(
  relDefinition: RelationshipDefinition,
  relationshipToViewDefinitions: List[RelationshipToViewDefinition]
) extends GraphDdlAst with MappingDefinition
