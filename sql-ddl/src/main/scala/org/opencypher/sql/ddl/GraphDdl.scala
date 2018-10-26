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
package org.opencypher.sql.ddl

import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.schema.{PropertyKeys, Schema, SchemaPattern}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, SchemaException}
import org.opencypher.sql.ddl.GraphDdlAst.ColumnIdentifier
import org.opencypher.sql.ddl.GraphDdl._

/*
TODO:
 validate
 - name resolution
   x schema names
   x alias names
   - property names in mappings must exist
   - referenced graph schema must exist
 - name conflicts
 - doubly mapped nodes/rels

 other
 - construct all property mappings (even if mapping to same name)

  */

object GraphDdl {

  type GraphType = Schema

  type NodeType = Set[String]
  type EdgeType = Set[String]

  type ViewId = String
  type ColumnId = String

  type PropertyMappings = Map[String, String]
  type LabelKey = (String, Set[String])

  def apply(ddl: String): GraphDdl = GraphDdl(GraphDdlParser.parse(ddl))

  def apply(ddl: DdlDefinition): GraphDdl = {

    val globalLabelDefinitions: Map[String, LabelDefinition] = ddl.labelDefinitions.keyBy(_.name)

    val graphTypes = ddl.schemaDefinitions
      .mapValues(schemaDefinition => toGraphType(globalLabelDefinitions, schemaDefinition))

    val inlineGraphTypes = ddl.graphDefinitions.keyBy(_.name)
      .mapValues(_.localSchemaDefinition)
      .mapValues(schemaDefinition => toGraphType(globalLabelDefinitions, schemaDefinition))

    val graphs = ddl.graphDefinitions
      .map(toGraph(inlineGraphTypes, graphTypes)).keyBy(_.name)

    GraphDdl(
      graphs = graphs
    )
  }

  private[ddl] def toGraphType(
    globalLabelDefinitions: Map[String, LabelDefinition],
    schemaDefinition: SchemaDefinition
  ): Schema = {
    val labelDefinitions = globalLabelDefinitions ++ schemaDefinition.localLabelDefinitions.map(labelDef => labelDef.name -> labelDef).toMap

    def undefinedLabelException(label: String) = IllegalArgumentException(s"Defined label (one of: ${labelDefinitions.keys.mkString("[", ", ", "]")})", label)

    // track all node / rel definitions (e.g. explicit ones and implicit ones from schema pattern definitions)
    val nodeDefinitionsFromPatterns = schemaDefinition.schemaPatternDefinitions.flatMap(schemaDef =>
      schemaDef.sourceLabelCombinations ++ schemaDef.targetLabelCombinations)
    val relDefinitionsFromPatterns = schemaDefinition.schemaPatternDefinitions.flatMap(_.relTypes)
    // Nodes

    val schemaWithNodes = (nodeDefinitionsFromPatterns ++ schemaDefinition.nodeDefinitions).foldLeft(Schema.empty) {
      case (currentSchema, labelCombo) =>
        val comboProperties = labelCombo.foldLeft(PropertyKeys.empty) { case (currProps, label) =>
          val labelProperties = labelDefinitions.getOrElse(label, throw undefinedLabelException(label)).properties
          currProps.keySet.intersect(labelProperties.keySet).foreach { key =>
            if (currProps(key) != labelProperties(key)) {
              throw SchemaException(
                s"""|Incompatible property types for label combination (${labelCombo.mkString(",")})
                    |Property key `$key` has conflicting types ${currProps(key)} and ${labelProperties(key)}
                 """.stripMargin)
            }
          }
          currProps ++ labelProperties
        }
        currentSchema.withNodePropertyKeys(labelCombo, comboProperties)
    }

    val usedLabels = schemaDefinition.nodeDefinitions.flatten
    val schemaWithNodeKeys = usedLabels.foldLeft(schemaWithNodes) {
      case (currentSchema, label) =>
        labelDefinitions(label).maybeKeyDefinition match {
          case Some((_, keys)) => currentSchema.withNodeKey(label, keys)
          case None => currentSchema
        }
    }

    // Relationships

    val schemaWithRels = (relDefinitionsFromPatterns ++ schemaDefinition.relDefinitions).foldLeft(schemaWithNodeKeys) {
      case (currentSchema, relType) =>
        currentSchema.withRelationshipPropertyKeys(relType, labelDefinitions.getOrElse(relType, throw undefinedLabelException(relType)).properties)
    }

    val usedRelTypes = schemaDefinition.relDefinitions
    val schemaWithRelationshipKeys = usedRelTypes.foldLeft(schemaWithRels) {
      case (currentSchema, relType) =>
        labelDefinitions(relType).maybeKeyDefinition match {
          case Some((_, keys)) => currentSchema.withRelationshipKey(relType, keys)
          case None => currentSchema
        }
    }

    // Schema patterns

    schemaDefinition.schemaPatternDefinitions.foldLeft(schemaWithRelationshipKeys) {
      // TODO: extend OKAPI schema with cardinality constraints
      case (currentSchema, SchemaPatternDefinition(sourceLabelCombinations, _, relTypes, _, targetLabelCombinations)) =>
        val expandedPatterns = for {
          sourceCombination <- sourceLabelCombinations
          relType <- relTypes
          targetLabelCombination <- targetLabelCombinations
        } yield SchemaPattern(sourceCombination, relType, targetLabelCombination)
        currentSchema.withSchemaPatterns(expandedPatterns.toSeq: _*)
    }
  }


  def toGraph(inlineTypes: Map[String, GraphType], graphTypes: Map[String, GraphType])
    (graph: GraphDefinition): Graph = {
    Graph(
      name = GraphName(graph.name),
      graphType = graph.maybeSchemaName
        .map(schemaName => graphTypes.getOrFail(schemaName, "Unresolved schema name"))
        .getOrElse(inlineTypes.getOrFail(graph.name, "Unresolved schema name")),
      nodeMappings = graph.nodeMappings.flatMap(toNodeMappings).keyBy(_.key),
      edgeMappings = graph.relationshipMappings.flatMap(toEdgeMappings).keyBy(_.key)
    )
  }

  def toNodeMappings(nmd: NodeMappingDefinition): Seq[NodeMapping] = {
    nmd.nodeToViewDefinitions.map { nvd =>
      NodeMapping(
        environment = DbEnv(DataSourceConfig()),
        nodeType = nmd.labelNames,
        view = nvd.viewName,
        properties = nvd.maybePropertyMapping.getOrElse(Map())
      )
    }
  }

  def toEdgeMappings(rmd: RelationshipMappingDefinition): Seq[EdgeMapping] = {
    rmd.relationshipToViewDefinitions.map { rvd =>
      EdgeMapping(
        environment = DbEnv(DataSourceConfig()),
        edgeType = Set(rmd.relType),
        view = rvd.viewDefinition.name,
        start = EdgeSource(
          nodes = NodeViewKey(
            nodeType = rvd.startNodeToViewDefinition.labelSet,
            view = rvd.startNodeToViewDefinition.viewDefinition.name
          ),
          joinPredicates = rvd.startNodeToViewDefinition.joinOn.joinPredicates.map(toJoin(
            nodeAlias = rvd.viewDefinition.alias,
            edgeAlias = rvd.startNodeToViewDefinition.viewDefinition.alias
          ))
        ),
        end = EdgeSource(
          nodes = NodeViewKey(
            nodeType = rvd.endNodeToViewDefinition.labelSet,
            view = rvd.endNodeToViewDefinition.viewDefinition.name
          ),
          joinPredicates = rvd.endNodeToViewDefinition.joinOn.joinPredicates.map(toJoin(
            nodeAlias = rvd.viewDefinition.alias,
            edgeAlias = rvd.endNodeToViewDefinition.viewDefinition.alias
          ))
        ),
        properties = rvd.maybePropertyMapping.getOrElse(Map())
      )
    }
  }

  def toJoin(nodeAlias: String, edgeAlias: String)(join: (ColumnIdentifier, ColumnIdentifier)): Join = {
    val (left, right) = join
    val (leftAlias, rightAlias) = (left.head, right.head)
    val (leftColumn, rightColumn) = (left.tail.mkString("."), right.tail.mkString("."))
    (leftAlias, rightAlias) match {
      case (`nodeAlias`, `edgeAlias`) => Join(nodeColumn = leftColumn, edgeColumn = rightColumn)
      case (`edgeAlias`, `nodeAlias`) => Join(nodeColumn = rightColumn, edgeColumn = leftColumn)
      case _ =>
        val aliases = Set(nodeAlias, edgeAlias)
        if (!aliases.contains(leftAlias)) notFound("Unresolved alias", leftAlias, aliases)
        if (!aliases.contains(rightAlias)) notFound("Unresolved alias", rightAlias, aliases)
        failure(s"Unable to resolve aliases: $leftAlias, $rightAlias")
    }
  }

  def notFound(msg: String, needle: Any, haystack: Traversable[Any]) =
    throw IllegalArgumentException(
      expected = if (haystack.nonEmpty) s"one of ${stringList(haystack)}" else "",
      actual = needle
    )

  def failure(msg: String): Nothing = ???

  private def stringList(elems: Traversable[Any]): String =
    elems.mkString("[", ",", "]")

  implicit class ListOps[T](list: List[T]) {
    def keyBy[K](key: T => K): Map[K, T] = list.map(t => key(t) -> t).toMap
  }

  implicit class MapOps[K, V](map: Map[K, V]) {
    def getOrFail(key: K, msg: String): V = map.getOrElse(key, notFound(msg, key, map.keySet))
  }
}

case class GraphDdl(
  graphs: Map[GraphName, Graph]
)

case class Graph(
  name: GraphName,
  graphType: GraphType,
  nodeMappings: Map[NodeViewKey, NodeMapping],
  edgeMappings: Map[EdgeViewKey, EdgeMapping]
)

sealed trait EntityMapping

case class NodeMapping(
  nodeType: NodeType,
  view: ViewId,
  properties: PropertyMappings,
  environment: DbEnv
) extends EntityMapping {
  def key: NodeViewKey = NodeViewKey(nodeType, view)
}

case class EdgeMapping(
  edgeType: EdgeType,
  view: ViewId,
  start: EdgeSource,
  end: EdgeSource,
  properties: PropertyMappings,
  environment: DbEnv
) extends EntityMapping {
  def key: EdgeViewKey = EdgeViewKey(edgeType, view)
}

case class EdgeSource(
  nodes: NodeViewKey,
  joinPredicates: List[Join]
)

case class Join(
  nodeColumn: String,
  edgeColumn: String
)

case class DbEnv(
  dataSource: DataSourceConfig
)

case class DataSourceConfig()

case class NodeViewKey(nodeType: Set[String], view: String)
case class EdgeViewKey(edgeType: Set[String], view: String)
