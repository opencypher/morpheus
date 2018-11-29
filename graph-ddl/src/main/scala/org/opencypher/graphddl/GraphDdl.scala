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

  type PropertyMappings = Map[String, String]

  private[graphddl] case class DdlParts(
    maybeSetSchema: Option[SetSchemaDefinition] = None,
    elementTypes: List[ElementTypeDefinition] = List.empty,
    graphTypes: List[GraphTypeDefinition] = List.empty,
    graphs: List[GraphDefinitionWithContext] = List.empty
  )

  private[graphddl] case class TypeParts(
    elementTypes: List[ElementTypeDefinition] = List.empty,
    nodeTypes: List[NodeTypeDefinition] = List.empty,
    relTypes: List[RelationshipTypeDefinition] = List.empty,
    patterns: List[PatternDefinition] = List.empty
  ) {

    def validateDistinct: TypeParts = {
      elementTypes.validateDistinctBy(_.name, "Duplicate element types")
      nodeTypes.validateDistinctBy(_.elementTypes, "Duplicate node types")
      relTypes.validateDistinctBy(_.elementType, "Duplicate relationship types")
      this
    }

    def allStatements: List[GraphTypeStatement] =
      elementTypes ++ nodeTypes ++ relTypes ++ patterns
  }

  private[graphddl] case class GraphParts(
    graphTypeStatements: List[GraphTypeStatement] = List.empty,
    nodeMappings: List[NodeMappingDefinition] = List.empty,
    relMappings: List[RelationshipMappingDefinition] = List.empty
  )

  case class GraphDefinitionWithContext(
    definition: GraphDefinition,
    maybeSetSchema: Option[SetSchemaDefinition] = None
  )

  def apply(ddl: String): GraphDdl = GraphDdl(GraphDdlParser.parse(ddl))

  def apply(ddl: DdlDefinition): GraphDdl = {

    val ddlParts = toDdlParts(ddl.statements)

    val elementTypeDefinitions: Map[String, ElementTypeDefinition] = ddlParts.elementTypes
      .validateDistinctBy(_.name, "Duplicate element type")
      .keyBy(_.name)

    val graphTypeDefinitions = ddlParts.graphTypes
      .validateDistinctBy(_.name, "Duplicate graph type name")
      .map { graphTypeDefinition =>
        tryWithContext(s"Error in graph type ${graphTypeDefinition.name}") {
          toTypeParts(graphTypeDefinition.statements).validateDistinct
        }
        graphTypeDefinition
      }.keyBy(_.name)

    val graphs = ddlParts.graphs
      .validateDistinctBy(_.definition.name, "Duplicate graph name")
      .map(graphDefinition => toGraph(graphTypeDefinitions, elementTypeDefinitions, graphDefinition))
      .keyBy(_.name)

    GraphDdl(
      graphs = graphs
    )
  }

  private[graphddl] def toDdlParts(statements: List[DdlStatement]) = statements.foldLeft(DdlParts()) {
    case (parts, s: SetSchemaDefinition)   => parts.copy(maybeSetSchema = Some(s))
    case (parts, s: ElementTypeDefinition) => parts.copy(elementTypes = parts.elementTypes :+ s)
    case (parts, s: GraphTypeDefinition)   => parts.copy(graphTypes = parts.graphTypes :+ s)
    case (parts, s: GraphDefinition)       => parts.copy(graphs = parts.graphs :+ GraphDefinitionWithContext(s, parts.maybeSetSchema))
  }

  private[graphddl] def toTypeParts(statements: List[GraphTypeStatement]) = statements.foldLeft(TypeParts()) {
    case (parts, s: ElementTypeDefinition)      => parts.copy(elementTypes = parts.elementTypes :+ s)
    case (parts, s: NodeTypeDefinition)         => parts.copy(nodeTypes = parts.nodeTypes :+ s)
    case (parts, s: RelationshipTypeDefinition) => parts.copy(relTypes = parts.relTypes :+ s)
    case (parts, s: PatternDefinition)          => parts.copy(patterns = parts.patterns :+ s)
  }

  private[graphddl] def toGraphParts(mappings: List[GraphStatement]) = mappings.foldLeft(GraphParts()) {
    case (parts, s: GraphTypeStatement)            => parts.copy(graphTypeStatements = parts.graphTypeStatements :+ s)
    case (parts, s: NodeMappingDefinition)         => parts.copy(nodeMappings = parts.nodeMappings :+ s)
    case (parts, s: RelationshipMappingDefinition) => parts.copy(relMappings = parts.relMappings :+ s)
  }

  private[graphddl] def toOkapiSchema(
    elementTypeDefinitions: Map[String, ElementTypeDefinition],
    graphTypeStatements: List[GraphTypeStatement]
  ): Schema = {

    val parts = toTypeParts(graphTypeStatements)

    val localElementTypes = parts.elementTypes
      .validateDistinctBy(_.name, "Duplicate element type")
      .keyBy(_.name)
    val labelDefinitions: Map[String, ElementTypeDefinition] = elementTypeDefinitions ++ localElementTypes

    // Nodes

    val nodeDefinitionsFromPatterns = parts.patterns.flatMap(pattern =>
      pattern.sourceNodeTypes ++ pattern.targetNodeTypes)

    val allNodeDefinitions = parts.nodeTypes.map(_.elementTypes) ++ nodeDefinitionsFromPatterns
    val schemaWithNodes = schemaForNodeDefinitions(labelDefinitions, allNodeDefinitions.toSet)

    // Relationships

    val relDefinitionsFromPatterns = parts.patterns.flatMap(_.relTypes)
    val allRelDefinitions = relDefinitionsFromPatterns ++ parts.relTypes.map(_.elementType)

    val schemaWithRels = schemaForRelationshipDefinitions(labelDefinitions, allRelDefinitions.toSet)

    // Schema patterns

    schemaWithSchemaPatterns(parts.patterns.toSet, schemaWithNodes ++ schemaWithRels)
  }

  private def schemaForNodeDefinitions(
    elementTypeDefinitions: Map[String, ElementTypeDefinition],
    nodeDefinitions: Set[Set[String]]
  ): Schema = {
    val schemaWithNodes = nodeDefinitions.foldLeft(Schema.empty) {
      case (currentSchema, labelCombo) =>
        tryWithContext(s"Error for label combination (${labelCombo.mkString(",")})") {
          labelCombo
            .flatMap(label => elementTypeDefinitions.getOrFail(label, "Unresolved element type").properties)
            .groupBy { case (key, _) => key }.mapValues(_.map { case (key, _) => key })

          val comboProperties = labelCombo.foldLeft(PropertyKeys.empty) { case (currProps, label) =>
            val labelProperties = elementTypeDefinitions.getOrFail(label, "Unresolved element type").properties
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
    val schemaWithRelationships = relDefinitions.foldLeft(Schema.empty) {
      case (currentSchema, relType) =>
        currentSchema.withRelationshipPropertyKeys(
          typ = relType,
          keys = elementTypeDefinitions.getOrFail(relType, "Unresolved element type").properties)
    }

    relDefinitions.foldLeft(schemaWithRelationships) {
      case (currentSchema, relType) =>
        elementTypeDefinitions(relType).maybeKey match {
          case Some((_, keys)) => currentSchema.withRelationshipKey(relType, keys)
          case None => currentSchema
        }
    }
  }

  private def schemaWithSchemaPatterns(
    patternDefinitions: Set[PatternDefinition],
    schemaWithNodesAndRelationships: Schema
  ): Schema = {
    patternDefinitions.foldLeft(schemaWithNodesAndRelationships) {
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

  private def toGraph(
    graphTypeDefinitions: Map[String, GraphTypeDefinition],
    elementTypeDefinitions: Map[String, ElementTypeDefinition],
    graph: GraphDefinitionWithContext): Graph =

    tryWithContext(s"Error in graph: ${graph.definition.name}") {
      // Merges type definitions from a graph type with the type definitions of a graph instance
      def normalizeGraphDefinition(
        graphDefinition: GraphDefinition,
        graphTypes: Map[String, GraphTypeDefinition]
      ): GraphParts = {
        val maybeGraphType = graphDefinition.maybeGraphTypeName.map(name => graphTypes.getOrFail(name, "Unresolved graph type name"))

        val initialGraphParts = toGraphParts(graphDefinition.statements)
        val typePartsFromGraph = toTypeParts(initialGraphParts.graphTypeStatements).validateDistinct
        val typePartsFromGraphType = toTypeParts(maybeGraphType.toList.flatMap(_.statements)).validateDistinct

        // Shadowing
        val allElementTypeStatements = elementTypeDefinitions ++ typePartsFromGraphType.elementTypes.keyBy(_.name) ++ typePartsFromGraph.elementTypes.keyBy(_.name)
        val allNodeTypeStatements = typePartsFromGraphType.nodeTypes.keyBy(_.elementTypes) ++ typePartsFromGraph.nodeTypes.keyBy(_.elementTypes) ++ initialGraphParts.nodeMappings.map(_.nodeType).keyBy(_.elementTypes)
        val allRelTypeStatements = typePartsFromGraphType.relTypes.keyBy(_.elementType) ++ typePartsFromGraph.relTypes.keyBy(_.elementType) ++ initialGraphParts.relMappings.map(_.relType).keyBy(_.elementType)
        val allPatternStatements = typePartsFromGraph.patterns ++ typePartsFromGraphType.patterns

        val allGraphTypeStatements =
          allElementTypeStatements.values ++
          allNodeTypeStatements.values ++
          allRelTypeStatements.values ++
          allPatternStatements

        initialGraphParts.copy(graphTypeStatements = allGraphTypeStatements.toList)
      }

      val graphParts = normalizeGraphDefinition(graph.definition, graphTypeDefinitions)
      val okapiSchema = toOkapiSchema(elementTypeDefinitions, graphParts.graphTypeStatements)

      Graph(
        name = GraphName(graph.definition.name),
        graphType = okapiSchema,
        // TODO: allow multiple node mappings with same node type and view + remove the validate
        nodeToViewMappings = graphParts.nodeMappings
          .flatMap(nm => toNodeToViewMappings(okapiSchema, graph.maybeSetSchema, nm))
          .validateDistinctBy(_.key, "Duplicate mapping")
          .keyBy(_.key),
        edgeToViewMappings = graphParts.relMappings
          .flatMap(em => toEdgeToViewMappings(okapiSchema, graph.maybeSetSchema, em))
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
