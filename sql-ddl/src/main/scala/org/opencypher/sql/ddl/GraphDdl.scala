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
import org.opencypher.sql.ddl.GraphDdl._
import org.opencypher.sql.ddl.GraphDdlAst.{ColumnIdentifier, PropertyToColumnMappingDefinition}

import scala.language.higherKinds

/*
TODO:
 validate
 - name resolution
   x schema names
   x alias names
   x property names in mappings must exist
   - referenced graph schema must exist
 - name conflicts
   x global labels
   x graph types
   x graphs
   x local labels
   - node types??
   - rel types??

 x doubly mapped nodes/rels

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

    val globalLabelDefinitions: Map[String, LabelDefinition] = ddl.labelDefinitions
      .validateDistinctBy(_.name)(name => s"Duplicate label name $name")
      .keyBy(_.name)

    val graphTypes = ddl.schemaDefinitions
      .validateDistinctBy(_._1)(name => s"Duplicate graph type name $name")
      .keyBy(_._1).mapValues(_._2)
      .mapValues(schemaDefinition => toGraphType(globalLabelDefinitions, schemaDefinition))
      .view.force // mapValues creates a view, but we want validation now

    val inlineGraphTypes = ddl.graphDefinitions
      .keyBy(_.name)
      .mapValues(_.localSchemaDefinition)
      .mapValues(schemaDefinition => toGraphType(globalLabelDefinitions, schemaDefinition))

    val graphs = ddl.graphDefinitions
      .map(toGraph(inlineGraphTypes, graphTypes))
      .validateDistinctBy(_.name)(name => s"Duplicate graph name $name")
      .keyBy(_.name)

    GraphDdl(
      graphs = graphs
    )
  }

  private[ddl] def toGraphType(
    globalLabelDefinitions: Map[String, LabelDefinition],
    schemaDefinition: SchemaDefinition
  ): Schema = {
    val localLabelDefinitions = schemaDefinition.localLabelDefinitions
      .validateDistinctBy(_.name)(name => s"Duplicate label name $name")
      .keyBy(_.name)
    val labelDefinitions = globalLabelDefinitions ++ localLabelDefinitions

    def undefinedLabelException(label: String) = IllegalArgumentException(s"Defined label (one of: ${labelDefinitions.keys.show})", label)

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
    val graphType = graph.maybeSchemaName
      .map(schemaName => graphTypes.getOrFail(schemaName, "Unresolved schema name"))
      .getOrElse(inlineTypes.getOrFail(graph.name, "Unresolved schema name"))

    Graph(
      name = GraphName(graph.name),
      graphType = graphType,
      nodeToViewMappings = graph.nodeMappings
        .flatMap(nm => toNodeToViewMappings(nm, graphType))
        .validateDistinctBy(_.key)(key => s"Duplicate mapping for $key")
        .keyBy(_.key),
      edgeToViewMappings = graph.relationshipMappings
        .flatMap(em => toEdgeToViewMappings(em, graphType))
        .validateDistinctBy(_.key)(key => s"Duplicate mapping for $key")
        .keyBy(_.key)
    )
  }

  def toNodeToViewMappings(nmd: NodeMappingDefinition, graphType: GraphType): Seq[NodeToViewMapping] = {
    nmd.nodeToViewDefinitions.map { nvd =>
      NodeToViewMapping(
        environment = DbEnv(DataSourceConfig()),
        nodeType = nmd.labelNames,
        view = nvd.viewName,
        propertyMappings = toPropertyMappings(nmd.labelNames, graphType.nodePropertyKeys(nmd.labelNames).keySet, nvd.maybePropertyMapping)
      )
    }
  }

  def toEdgeToViewMappings(rmd: RelationshipMappingDefinition, graphType: GraphType): Seq[EdgeToViewMapping] = {
    rmd.relationshipToViewDefinitions.map { rvd =>
      EdgeToViewMapping(
        environment = DbEnv(DataSourceConfig()),
        edgeType = Set(rmd.relType),
        view = rvd.viewDefinition.name,
        startNode = StartNode(
          nodeViewKey = NodeViewKey(
            nodeType = rvd.startNodeToViewDefinition.labelSet,
            view = rvd.startNodeToViewDefinition.viewDefinition.name
          ),
          joinPredicates = rvd.startNodeToViewDefinition.joinOn.joinPredicates.map(toJoin(
            nodeAlias = rvd.startNodeToViewDefinition.viewDefinition.alias,
            edgeAlias = rvd.viewDefinition.alias
          ))
        ),
        endNode = EndNode(
          nodeViewKey = NodeViewKey(
            nodeType = rvd.endNodeToViewDefinition.labelSet,
            view = rvd.endNodeToViewDefinition.viewDefinition.name
          ),
          joinPredicates = rvd.endNodeToViewDefinition.joinOn.joinPredicates.map(toJoin(
            nodeAlias = rvd.endNodeToViewDefinition.viewDefinition.alias,
            edgeAlias = rvd.viewDefinition.alias
          ))
        ),
        propertyMappings = toPropertyMappings(Set(rmd.relType), graphType.relationshipPropertyKeys(rmd.relType).keySet, rvd.maybePropertyMapping)
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

  def toPropertyMappings(
    labels: Set[String],
    schemaPropertyKeys: Set[String],
    maybePropertyMapping: Option[PropertyToColumnMappingDefinition]
  ): PropertyMappings = {
    maybePropertyMapping match {
      case Some(propertyToColumnMappingDefinition) =>
        propertyToColumnMappingDefinition.keys.foreach { key =>
          if (!schemaPropertyKeys.contains(key)) {
            throw GraphDdlException(
              s"""Mapped property $key does not exist for element type ${elementType(labels)}.
                 |Expected one of ${stringList(schemaPropertyKeys)}.""".stripMargin)
          }
        }
        val remainingKeys = schemaPropertyKeys -- propertyToColumnMappingDefinition.keys
        propertyToColumnMappingDefinition ++ remainingKeys.map(key => key -> key)

      case None => schemaPropertyKeys.map(key => key -> key).toMap
    }
  }

  def notFound(msg: String, needle: Any, haystack: Traversable[Any]) =
    throw IllegalArgumentException(
      expected = if (haystack.nonEmpty) s"one of ${haystack.show}" else "",
      actual = needle
    )

  def failure(msg: String): Nothing = ???

  private def elementType(labels: Traversable[String]): String =
    labels.mkString("(", ", ", ")")

  private def stringList(elems: Traversable[Any]): String =
    elems.mkString("[", ",", "]")

  implicit class TraversableOps[T, C[X] <: Traversable[X]](elems: C[T]) {
    def show: String = elems.mkString("[", ",", "]")

    def keyBy[K](key: T => K): Map[K, T] =
      elems.map(t => key(t) -> t).toMap

    def   validateDistinctBy[K](key: T => K)(msg: K => String): C[T] = {
      elems.groupBy(key).foreach {
        case (k, vals) if vals.size > 1 => throw SchemaException(msg(k))
        case _ =>
      }
      elems
    }
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
  nodeToViewMappings: Map[NodeViewKey, NodeToViewMapping],
  edgeToViewMappings: Map[EdgeViewKey, EdgeToViewMapping]
)

sealed trait ElementToViewMapping

case class NodeToViewMapping(
  nodeType: NodeType,
  view: ViewId,
  propertyMappings: PropertyMappings,
  environment: DbEnv
) extends ElementToViewMapping {
  def key: NodeViewKey = NodeViewKey(nodeType, view)
}

case class EdgeToViewMapping(
  edgeType: EdgeType,
  view: ViewId,
  startNode: StartNode,
  endNode: EndNode,
  propertyMappings: PropertyMappings,
  environment: DbEnv
) extends ElementToViewMapping {
  def key: EdgeViewKey = EdgeViewKey(edgeType, view)
}

case class StartNode(
  nodeViewKey: NodeViewKey,
  joinPredicates: List[Join]
)

case class EndNode(
  nodeViewKey: NodeViewKey,
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


case class NodeViewKey(nodeType: Set[String], view: String) {
  override def toString: String = s"node type: ${nodeType.mkString(", ")}, view: $view"
}
case class EdgeViewKey(edgeType: Set[String], view: String) {
  override def toString: String = s"relationship type: ${edgeType.mkString(", ")}, view: $view"
}

case class GraphDdlException(msg: String) extends RuntimeException(msg)