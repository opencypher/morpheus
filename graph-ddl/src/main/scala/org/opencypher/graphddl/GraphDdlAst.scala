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
package org.opencypher.graphddl

import org.opencypher.graphddl.GraphDdlAst._
import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.trees.AbstractTreeNode

object GraphDdlAst {
  type Property = (String, CypherType)

  type KeyDefinition = (String, Set[String])

  type ColumnIdentifier = List[String]

  type PropertyToColumnMappingDefinition = Map[String, String]
}

abstract class GraphDdlAst extends AbstractTreeNode[GraphDdlAst]

case class DdlDefinition(
  statements: List[DdlStatement]
) extends GraphDdlAst

sealed trait DdlStatement
sealed trait GraphStatement
sealed trait GraphTypeStatement extends GraphStatement

case class SetSchemaDefinition(
  dataSource: String,
  schema: String
) extends GraphDdlAst with DdlStatement

case class ElementTypeDefinition(
  name: String,
  parents: Set[String] = Set.empty,
  properties: Map[String, CypherType] = Map.empty,
  maybeKey: Option[KeyDefinition] = None
) extends GraphDdlAst with DdlStatement with GraphTypeStatement

case class GraphTypeDefinition(
  name: String,
  statements: List[GraphTypeStatement] = List.empty
) extends GraphDdlAst with DdlStatement

case class GraphDefinition(
  name: String,
  maybeGraphTypeName: Option[String] = None,
  statements: List[GraphStatement] = List.empty
) extends GraphDdlAst with DdlStatement

object NodeTypeDefinition {
  def apply(elementTypes: String*): NodeTypeDefinition = NodeTypeDefinition(elementTypes.toSet)
}

case class NodeTypeDefinition(
  elementTypes: Set[String]
) extends GraphDdlAst with GraphTypeStatement {
  override def toString: String = s"(${elementTypes.mkString(",")})"
}

object RelationshipTypeDefinition {
  def apply(startNodeElementType: String, elementType: String, endNodeElementType: String): RelationshipTypeDefinition =
    RelationshipTypeDefinition(NodeTypeDefinition(startNodeElementType), Set(elementType), NodeTypeDefinition(endNodeElementType))

  def apply(startNodeElementTypes: String*)(elementTypes: String*)(endNodeElementTypes: String*): RelationshipTypeDefinition =
    RelationshipTypeDefinition(NodeTypeDefinition(startNodeElementTypes.toSet), elementTypes.toSet, NodeTypeDefinition(endNodeElementTypes.toSet))
}

case class RelationshipTypeDefinition(
  startNodeType: NodeTypeDefinition,
  elementTypes: Set[String],
  endNodeType: NodeTypeDefinition
) extends GraphDdlAst with GraphTypeStatement {
  override def toString: String = s"$startNodeType-[${elementTypes.mkString(",")}]->$endNodeType"
}

trait ElementToViewDefinition {
  def maybePropertyMapping: Option[PropertyToColumnMappingDefinition]
}

case class NodeToViewDefinition (
  viewId: List[String],
  override val maybePropertyMapping: Option[PropertyToColumnMappingDefinition] = None
) extends GraphDdlAst with ElementToViewDefinition

case class NodeMappingDefinition(
  nodeType: NodeTypeDefinition,
  nodeToView: List[NodeToViewDefinition] = List.empty
) extends GraphDdlAst with GraphStatement

case class ViewDefinition(
  viewId: List[String],
  alias: String
) extends GraphDdlAst

case class JoinOnDefinition(joinPredicates: List[(ColumnIdentifier, ColumnIdentifier)]) extends GraphDdlAst

case class NodeTypeToViewDefinition(
  nodeType: NodeTypeDefinition,
  viewDef: ViewDefinition,
  joinOn: JoinOnDefinition
) extends GraphDdlAst

case class RelationshipTypeToViewDefinition(
  viewDef: ViewDefinition,
  override val maybePropertyMapping: Option[PropertyToColumnMappingDefinition] = None,
  startNodeTypeToView: NodeTypeToViewDefinition,
  endNodeTypeToView: NodeTypeToViewDefinition
) extends GraphDdlAst with ElementToViewDefinition

case class RelationshipMappingDefinition(
  relType: RelationshipTypeDefinition,
  relTypeToView: List[RelationshipTypeToViewDefinition]
) extends GraphDdlAst with GraphStatement
