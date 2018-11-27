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

import org.opencypher.graphddl.GraphDdl._
import org.opencypher.graphddl.GraphDdlAst.{ColumnIdentifier, PropertyToColumnMappingDefinition}
import org.opencypher.graphddl.GraphDdlException.{malformed, tryWithContext, unresolved}
import org.opencypher.graphddl.{GraphDdlException => Err}
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.schema.{PropertyKeys, Schema, SchemaPattern}

import scala.language.higherKinds

// TODO: validate that no duplicate node types / rel types are defined (not sure if necessary though)
object GraphDdl {

  type NodeType = Set[String]
  type EdgeType = Set[String]

  type ColumnId = String

  type PropertyMappings = Map[String, String]
  type LabelKey = (String, Set[String])

  case class GraphDefinitionWithContext(
    definition: GraphDefinition,
    maybeSetSchema: Option[SetSchemaDefinition] = None
  )

  def apply(ddl: String): GraphDdl = GraphDdl(GraphDdlParser.parse(ddl))

  def apply(ddl: DdlDefinition): GraphDdl = {

    case class DdlParts(
      maybeCurrentSetSchema: Option[SetSchemaDefinition] = None,
      elementTypeDefinitions: List[ElementTypeDefinition] = List.empty,
      graphTypeDefinitions: List[GraphTypeDefinition] = List.empty,
      graphDefinitions: List[GraphDefinitionWithContext] = List.empty
    )

    // split ddl statements into parts and embed any sequence-dependent context (SET SCHEMA)
    val ddlParts = ddl.statements.foldLeft(DdlParts()) {
      case (state, s: SetSchemaDefinition) =>
        state.copy(maybeCurrentSetSchema = Some(s))

      case (state, s: ElementTypeDefinition) =>
        state.copy(elementTypeDefinitions = state.elementTypeDefinitions :+ s)

      case (state, s: GraphTypeDefinition) =>
        state.copy(graphTypeDefinitions = state.graphTypeDefinitions :+ s)

      case (state, s: GraphDefinition) =>
        state.copy(graphDefinitions = state.graphDefinitions :+ GraphDefinitionWithContext(s, state.maybeCurrentSetSchema))
    }

    val globalLabelDefinitions: Map[String, ElementTypeDefinition] = ddlParts.elementTypeDefinitions
      .validateDistinctBy(_.name, "Duplicate label name")
      .keyBy(_.name)

    val graphTypes = ddlParts.graphTypeDefinitions
      .validateDistinctBy(_.name, "Duplicate graph type name")
      .keyBy(_.name).mapValues(_.graphTypeBody)
      .map { case (name, graphType) =>
        name -> tryWithContext(s"Error in graph type: $name")(toOkapiSchema(globalLabelDefinitions, graphType))
      }
      .view.force // mapValues creates a view, but we want validation now

    val inlineGraphTypes = ddlParts.graphDefinitions.map(_.definition)
      .keyBy(_.name)
      .mapValues(_.graphTypeBody)
      .map { case (name, graphType) =>
        name -> tryWithContext(s"Error in graph type of graph: $name")(toOkapiSchema(globalLabelDefinitions, graphType))
      }

    val graphs = ddlParts.graphDefinitions
      .map(graphDefinition => toGraph(inlineGraphTypes, graphTypes, graphDefinition))
      .validateDistinctBy(_.name, "Duplicate graph name")
      .keyBy(_.name)

    GraphDdl(
      graphs = graphs
    )
  }

  private[graphddl] case class TypeParts(
    labelDefinitions: List[ElementTypeDefinition] = List.empty,
    nodeDefinitions: List[NodeTypeDefinition] = List.empty,
    relDefinitions: List[RelationshipTypeDefinition] = List.empty,
    patternDefinitions: List[PatternDefinition] = List.empty
  )

  private[graphddl] def toTypeParts(
    typeDefinition: GraphTypeBody
  ) = typeDefinition.statements.foldLeft(TypeParts()) {
    case (parts, s: ElementTypeDefinition)      => parts.copy(labelDefinitions = parts.labelDefinitions :+ s)
    case (parts, s: NodeTypeDefinition)         => parts.copy(nodeDefinitions = parts.nodeDefinitions :+ s)
    case (parts, s: RelationshipTypeDefinition) => parts.copy(relDefinitions = parts.relDefinitions :+ s)
    case (parts, s: PatternDefinition)          => parts.copy(patternDefinitions = parts.patternDefinitions :+ s)
  }

  private[graphddl] def toOkapiSchema(
    globalLabelDefinitions: Map[String, ElementTypeDefinition],
    typeDefinition: GraphTypeBody
  ): Schema = {

    val parts = toTypeParts(typeDefinition)

    val localLabelDefinitions = parts.labelDefinitions
      .validateDistinctBy(_.name, "Duplicate label name")
      .keyBy(_.name)
    val labelDefinitions: Map[String, ElementTypeDefinition] = globalLabelDefinitions ++ localLabelDefinitions

    // Nodes

    val nodeDefinitionsFromPatterns = parts.patternDefinitions.flatMap(pattern =>
      pattern.sourceNodeTypes ++ pattern.targetNodeTypes)
    val allNodeDefinitions = parts.nodeDefinitions.map(_.elementTypes) ++ nodeDefinitionsFromPatterns
    val schemaWithNodes = schemaForNodeDefinitions(labelDefinitions, allNodeDefinitions.toSet)

    // Relationships

    val relDefinitionsFromPatterns = parts.patternDefinitions.flatMap(_.relTypes)
    val allRelDefinitions = relDefinitionsFromPatterns ++ parts.relDefinitions.map(_.elementType)

    val schemaWithRels = schemaForRelationshipDefinitions(labelDefinitions, allRelDefinitions.toSet)

    // Schema patterns

    schemaWithSchemaPatterns(parts.patternDefinitions.toSet, schemaWithNodes ++ schemaWithRels)
  }

  private def schemaForNodeDefinitions(
    elementTypeDefinitions: Map[String, ElementTypeDefinition],
    nodeDefinitions: Set[Set[String]]
  ): Schema = {
    val schemaWithNodes = nodeDefinitions.foldLeft(Schema.empty) {
      case (currentSchema, labelCombo) =>
        tryWithContext(s"Error for label combination (${labelCombo.mkString(",")})") {
          labelCombo
            .flatMap(label => elementTypeDefinitions.getOrFail(label, "Unresolved label").properties)
            .groupBy { case (key, _) => key }.mapValues(_.map { case (key, _) => key })

          val comboProperties = labelCombo.foldLeft(PropertyKeys.empty) { case (currProps, label) =>
            val labelProperties = elementTypeDefinitions.getOrFail(label, "Unresolved label").properties
            currProps.keySet.intersect(labelProperties.keySet).foreach { key =>
              if (currProps(key) != labelProperties(key)) {
                Err.incompatibleTypes(
                  s"""|Incompatible property types for property key: $key
                      |Conflicting types: ${currProps(key)} and ${labelProperties(key)}
                 """.stripMargin)
              }
            }
            currProps ++ labelProperties
          }
          currentSchema.withNodePropertyKeys(labelCombo, comboProperties)
        }
    }

    val usedLabels = nodeDefinitions.flatten
    usedLabels.foldLeft(schemaWithNodes) {
      case (currentSchema, label) =>
        elementTypeDefinitions(label).maybeKey match {
          case Some((_, keys)) => currentSchema.withNodeKey(label, keys)
          case None => currentSchema
        }
    }
  }

  private def schemaForRelationshipDefinitions(
    elementTypeDefinitions: Map[String, ElementTypeDefinition],
    relDefinitions: Set[String]
  ): Schema = {
    val schemaWithRels = relDefinitions.foldLeft(Schema.empty) {
      case (currentSchema, relType) =>
        currentSchema.withRelationshipPropertyKeys(relType, elementTypeDefinitions.getOrFail(relType, "Unresolved label").properties)
    }

    relDefinitions.foldLeft(schemaWithRels) {
      case (currentSchema, relType) =>
        elementTypeDefinitions(relType).maybeKey match {
          case Some((_, keys)) => currentSchema.withRelationshipKey(relType, keys)
          case None => currentSchema
        }
    }
  }

  private def schemaWithSchemaPatterns(
    patternDefinitions: Set[PatternDefinition],
    schemaWithNodesAndRels: Schema
  ): Schema = {
    patternDefinitions.foldLeft(schemaWithNodesAndRels) {
      // TODO: extend OKAPI schema with cardinality constraints
      case (currentSchema, PatternDefinition(sourceLabelCombinations, _, relTypes, _, targetLabelCombinations)) =>
        val expandedPatterns = for {
          sourceCombination <- sourceLabelCombinations
          relType <- relTypes
          targetLabelCombination <- targetLabelCombinations
        } yield SchemaPattern(sourceCombination, relType, targetLabelCombination)
        currentSchema.withSchemaPatterns(expandedPatterns.toSeq: _*)
    }
  }

  def toGraph(
    inlineTypes: Map[String, Schema],
    graphTypes: Map[String, Schema],
    graph: GraphDefinitionWithContext
  ): Graph = tryWithContext(s"Error in graph: ${graph.definition.name}") {
    val graphType = graph.definition.maybeGraphTypeName
      .map(name => graphTypes.getOrFail(name, "Unresolved graph type name"))
      .getOrElse(inlineTypes.getOrFail(graph.definition.name, "Unresolved graph type name"))

    Graph(
      name = GraphName(graph.definition.name),
      graphType = graphType,

      // TODO: allow multiple node mappings with same node type and view + remove the validate
      nodeToViewMappings = graph.definition.mappings
        .collect { case nm : NodeMappingDefinition => nm }
        .flatMap(nm => toNodeToViewMappings(graphType, graph.maybeSetSchema, nm))
        .validateDistinctBy(_.key, "Duplicate mapping")
        .keyBy(_.key),
      edgeToViewMappings = graph.definition.mappings
        .collect { case rm: RelationshipMappingDefinition => rm }
        .flatMap(em => toEdgeToViewMappings(graphType, graph.maybeSetSchema, em))
    )
  }

  def toNodeToViewMappings(
    graphType: Schema,
    maybeSetSchema: Option[SetSchemaDefinition],
    nmd: NodeMappingDefinition
  ): Seq[NodeToViewMapping] = {
    nmd.nodeToView.map { nvd =>
      tryWithContext(s"Error in node mapping for: ${nmd.nodeType.elementTypes.mkString(",")}") {
        val viewId = toQualifiedViewId(maybeSetSchema, nvd.viewId)
        val nodeKey = NodeViewKey(nmd.nodeType.elementTypes, viewId)
        tryWithContext(s"Error in node mapping for: $nodeKey") {
          NodeToViewMapping(
            nodeType = nodeKey.nodeType,
            view = nodeKey.qualifiedViewId,
            propertyMappings = toPropertyMappings(
              elementTypes = nmd.nodeType.elementTypes,
              graphTypePropertyKeys = graphType.nodePropertyKeys(nmd.nodeType.elementTypes).keySet,
              maybePropertyMapping = nvd.maybePropertyMapping
            )
          )
        }
      }
    }
  }

  def toEdgeToViewMappings(
    graphType: Schema,
    maybeSetSchema: Option[SetSchemaDefinition],
    rmd: RelationshipMappingDefinition
  ): Seq[EdgeToViewMapping] = {
    rmd.relTypeToView.map { rvd =>
      tryWithContext(s"Error in relationship mapping for: ${rmd.relType}") {
        val viewId = toQualifiedViewId(maybeSetSchema, rvd.viewDef.viewId)
        val edgeKey = EdgeViewKey(Set(rmd.relType.elementType), viewId)
        tryWithContext(s"Error in relationship mapping for: $edgeKey") {
          EdgeToViewMapping(
            edgeType = edgeKey.edgeType,
            view = edgeKey.qualifiedViewId,
            startNode = StartNode(
              nodeViewKey = NodeViewKey(
                nodeType = rvd.startNodeTypeToView.nodeType.elementTypes,
                qualifiedViewId = toQualifiedViewId(maybeSetSchema, rvd.startNodeTypeToView.viewDef.viewId)
              ),
              joinPredicates = rvd.startNodeTypeToView.joinOn.joinPredicates.map(toJoin(
                nodeAlias = rvd.startNodeTypeToView.viewDef.alias,
                edgeAlias = rvd.viewDef.alias
              ))
            ),
            endNode = EndNode(
              nodeViewKey = NodeViewKey(
                nodeType = rvd.endNodeTypeToView.nodeType.elementTypes,
                qualifiedViewId = toQualifiedViewId(maybeSetSchema, rvd.endNodeTypeToView.viewDef.viewId)
              ),
              joinPredicates = rvd.endNodeTypeToView.joinOn.joinPredicates.map(toJoin(
                nodeAlias = rvd.endNodeTypeToView.viewDef.alias,
                edgeAlias = rvd.viewDef.alias
              ))
            ),
            propertyMappings = toPropertyMappings(
              elementTypes = Set(rmd.relType.elementType),
              graphTypePropertyKeys = graphType.relationshipPropertyKeys(rmd.relType.elementType).keySet,
              maybePropertyMapping = rvd.maybePropertyMapping
            )
          )
        }
      }
    }
  }

  def toQualifiedViewId(
    maybeSetSchema: Option[SetSchemaDefinition],
    viewId: List[String]
  ): QualifiedViewId = (maybeSetSchema, viewId) match {
    case (_, dataSource :: schema :: view :: Nil) =>
      QualifiedViewId(dataSource, schema, view)
    case (Some(SetSchemaDefinition(dataSource, schema)), view :: Nil) =>
      QualifiedViewId(dataSource, schema, view)
    case (None, view) if view.size < 3 =>
      malformed("Relative view identifier requires a preceeding SET SCHEMA statement", view.mkString("."))
    case (Some(_), view) if view.size > 1 =>
      malformed("Relative view identifier must have exactly one segment", view.mkString("."))
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
        if (!aliases.contains(leftAlias)) unresolved("Unresolved alias", leftAlias, aliases)
        if (!aliases.contains(rightAlias)) unresolved("Unresolved alias", rightAlias, aliases)
        unresolved(s"Unable to resolve aliases", s"$leftAlias, $rightAlias", aliases)
    }
  }

  def toPropertyMappings(
    elementTypes: Set[String],
    graphTypePropertyKeys: Set[String],
    maybePropertyMapping: Option[PropertyToColumnMappingDefinition]
  ): PropertyMappings = {
    val mappings = maybePropertyMapping.getOrElse(Map.empty)
    mappings.keys
      .filterNot(graphTypePropertyKeys)
      .foreach(p => unresolved("Unresolved property name", p, graphTypePropertyKeys))

    graphTypePropertyKeys
      .keyBy(identity)
      .mapValues(prop => mappings.getOrElse(prop, prop))
  }

  implicit class TraversableOps[T, C[X] <: Traversable[X]](elems: C[T]) {
    def keyBy[K](key: T => K): Map[K, T] =
      elems.map(t => key(t) -> t).toMap

    def validateDistinctBy[K](key: T => K, msg: String): C[T] = {
      elems.groupBy(key).foreach {
        case (k, vals) if vals.size > 1 => Err.duplicate(msg, k)
        case _ =>
      }
      elems
    }
  }

  implicit class MapOps[K, V](map: Map[K, V]) {
    def getOrFail(key: K, msg: String): V = map.getOrElse(key, unresolved(msg, key, map.keySet))
  }
}

case class GraphDdl(
  graphs: Map[GraphName, Graph]
)

case class Graph(
  name: GraphName,
  graphType: Schema,
  nodeToViewMappings: Map[NodeViewKey, NodeToViewMapping],
  edgeToViewMappings: List[EdgeToViewMapping]
) {

  // TODO: validate (during GraphDdl construction) if the user always uses the same join columns for the node views
  def nodeIdColumnsFor(nodeViewKey: NodeViewKey): Option[List[String]] = edgeToViewMappings.collectFirst {
    case evm: EdgeToViewMapping if evm.startNode.nodeViewKey == nodeViewKey =>
      evm.startNode.joinPredicates.map(_.nodeColumn)

    case evm: EdgeToViewMapping if evm.endNode.nodeViewKey == nodeViewKey =>
      evm.endNode.joinPredicates.map(_.nodeColumn)
  }
}

object QualifiedViewId {
  def apply(qualifiedViewId: String): QualifiedViewId = qualifiedViewId.split("\\.").toList match {
    case dataSource :: schema :: view :: Nil => QualifiedViewId(dataSource, schema, view)
    case _ => malformed("Qualified view id did not match pattern dataSource.schema.view", qualifiedViewId)
  }
}

case class QualifiedViewId(
  dataSource: String,
  schema: String,
  view: String
) {
  override def toString: String = s"$dataSource.$schema.$view"
}

sealed trait ElementToViewMapping

case class NodeToViewMapping(
  nodeType: NodeType,
  view: QualifiedViewId,
  propertyMappings: PropertyMappings
) extends ElementToViewMapping {
  def key: NodeViewKey = NodeViewKey(nodeType, view)
}

case class EdgeToViewMapping(
  edgeType: EdgeType,
  view: QualifiedViewId,
  startNode: StartNode,
  endNode: EndNode,
  propertyMappings: PropertyMappings
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

trait ElementViewKey {
  def elementType: Set[String]
  def qualifiedViewId: QualifiedViewId
}

case class NodeViewKey(nodeType: Set[String], qualifiedViewId: QualifiedViewId) extends ElementViewKey {
  override val elementType: Set[String] = nodeType

  override def toString: String = s"node type: ${nodeType.mkString(", ")}, view: $qualifiedViewId"
}

case class EdgeViewKey(edgeType: Set[String], qualifiedViewId: QualifiedViewId) extends ElementViewKey {
  override val elementType: Set[String] = edgeType

  override def toString: String = s"relationship type: ${edgeType.mkString(", ")}, view: $qualifiedViewId"
}
