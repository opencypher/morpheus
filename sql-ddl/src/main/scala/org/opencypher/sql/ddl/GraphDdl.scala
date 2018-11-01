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
import org.opencypher.sql.ddl.{GraphDdlException => Err}
import org.opencypher.sql.ddl.GraphDdl._
import org.opencypher.sql.ddl.GraphDdlAst.{ColumnIdentifier, PropertyToColumnMappingDefinition}

import scala.language.higherKinds

/*
TODO:
 == validate
 - name resolution
   x schema names
   x alias names
   x property names in mappings must exist
   x referenced graph schema must exist
 - name conflicts
   x global labels
   x graph types
   x graphs
   x local labels
   - node types??
   - rel types??
 - mappings
   x duplicate node/rel mappings
   - duplicate property mappings?

 == other
 x construct all property mappings (even if mapping to same name)

  */

object GraphDdl {

  type GraphType = Schema

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
      labelDefinitions: List[LabelDefinition] = List.empty,
      globalSchemaDefinitions: List[GlobalSchemaDefinition] = List.empty,
      graphDefinitions: List[GraphDefinitionWithContext] = List.empty
    )

    // split ddl statements into parts and embed any sequence-dependent context (SET SCHEMA)
    val ddlParts = ddl.statements.foldLeft(DdlParts()) {
      case (state, ssd: SetSchemaDefinition) =>
        state.copy(maybeCurrentSetSchema = Some(ssd))

      case (state, ld: LabelDefinition) =>
        state.copy(labelDefinitions = state.labelDefinitions :+ ld)

      case (state, gsd: GlobalSchemaDefinition) =>
        state.copy(globalSchemaDefinitions = state.globalSchemaDefinitions :+ gsd)

      case (state, gsd: GraphDefinition) =>
        state.copy(graphDefinitions = state.graphDefinitions :+ GraphDefinitionWithContext(gsd, state.maybeCurrentSetSchema))
    }


    val globalLabelDefinitions: Map[String, LabelDefinition] = ddlParts.labelDefinitions
      .validateDistinctBy(_.name, "Duplicate label name")
      .keyBy(_.name)

    val graphTypes = ddlParts.globalSchemaDefinitions
      .validateDistinctBy(_.name, "Duplicate graph type name")
      .keyBy(_.name).mapValues(_.schemaDefinition)
      .map { case (name, schema) =>
        name -> Err.contextualize(s"Error in graph type: $name")(toGraphType(globalLabelDefinitions, schema)) }
      .view.force // mapValues creates a view, but we want validation now

    val inlineGraphTypes = ddlParts.graphDefinitions.map(_.definition)
      .keyBy(_.name)
      .mapValues(_.localSchemaDefinition)
      .map { case (name, schema) =>
        name -> Err.contextualize(s"Error in graph type of graph: $name")(toGraphType(globalLabelDefinitions, schema)) }

    val graphs = ddlParts.graphDefinitions
      .map(graphDefinition => toGraph(inlineGraphTypes, graphTypes, graphDefinition))
      .validateDistinctBy(_.name, "Duplicate graph name")
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
      .validateDistinctBy(_.name, "Duplicate label name")
      .keyBy(_.name)
    val labelDefinitions = globalLabelDefinitions ++ localLabelDefinitions

    // track all node / rel definitions (e.g. explicit ones and implicit ones from schema pattern definitions)
    val nodeDefinitionsFromPatterns = schemaDefinition.schemaPatternDefinitions.flatMap(schemaDef =>
      schemaDef.sourceLabelCombinations ++ schemaDef.targetLabelCombinations)
    val relDefinitionsFromPatterns = schemaDefinition.schemaPatternDefinitions.flatMap(_.relTypes)

    // Nodes

    val schemaWithNodes = (nodeDefinitionsFromPatterns ++ schemaDefinition.nodeDefinitions).foldLeft(Schema.empty) {
      case (currentSchema, labelCombo) =>
        Err.contextualize(s"Error for label combination (${labelCombo.mkString(",")})") {
          labelCombo
            .flatMap(label => labelDefinitions.getOrFail(label, "Unresolved label").properties)
            .groupBy { case (key, tpe) => key }.mapValues(_.map { case (key, tpe) => key } )

          val comboProperties = labelCombo.foldLeft(PropertyKeys.empty) { case (currProps, label) =>
            val labelProperties = labelDefinitions.getOrFail(label, "Unresolved label").properties
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
        currentSchema.withRelationshipPropertyKeys(relType, labelDefinitions.getOrFail(relType, "Unresolved label").properties)
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


  def toGraph(
    inlineTypes: Map[String, GraphType],
    graphTypes: Map[String, GraphType],
    graph: GraphDefinitionWithContext
  ): Graph = Err.contextualize(s"Error in graph: ${graph.definition.name}") {
    val graphType = graph.definition.maybeSchemaName
      .map(schemaName => graphTypes.getOrFail(schemaName, "Unresolved schema name"))
      .getOrElse(inlineTypes.getOrFail(graph.definition.name, "Unresolved schema name"))

    Graph(
      name = GraphName(graph.definition.name),
      graphType = graphType,
      nodeToViewMappings = graph.definition.nodeMappings
        .flatMap(nm => toNodeToViewMappings(graphType, graph.maybeSetSchema, nm))
        .validateDistinctBy(_.key, "Duplicate mapping")
        .keyBy(_.key),
      edgeToViewMappings = graph.definition.relationshipMappings
        .flatMap(em => toEdgeToViewMappings(graphType, graph.maybeSetSchema, em))
//        .validateDistinctBy(_.key, "Duplicate mapping")
//        .keyBy(_.key)
    )
  }

  def toNodeToViewMappings(
    graphType: GraphType,
    maybeSetSchema: Option[SetSchemaDefinition],
    nmd: NodeMappingDefinition
  ): Seq[NodeToViewMapping] = {
    nmd.nodeToViewDefinitions.map { nvd =>
      Err.contextualize(s"Error in node mapping for: ${nmd.labelNames.mkString(",")}") {
        val viewId = toQualifiedViewId(maybeSetSchema, nvd.viewId)
        val nodeKey = NodeViewKey(nmd.labelNames, viewId)
        Err.contextualize(s"Error in node mapping for: $nodeKey") {
          NodeToViewMapping(
            nodeType = nodeKey.nodeType,
            view = nodeKey.qualifiedViewId,
            propertyMappings = toPropertyMappings(nmd.labelNames, graphType.nodePropertyKeys(nmd.labelNames).keySet, nvd.maybePropertyMapping)
          )
        }
      }
    }
  }

  def toEdgeToViewMappings(
    graphType: GraphType,
    maybeSetSchema: Option[SetSchemaDefinition],
    rmd: RelationshipMappingDefinition
  ): Seq[EdgeToViewMapping] = {
    rmd.relationshipToViewDefinitions.map { rvd =>
      Err.contextualize(s"Error in relationship mapping for: ${rmd.relType}") {
        val viewId = toQualifiedViewId(maybeSetSchema, rvd.viewDefinition.viewId)
        val edgeKey = EdgeViewKey(Set(rmd.relType), viewId)
        Err.contextualize(s"Error in relationship mapping for: $edgeKey") {
          EdgeToViewMapping(
            edgeType = edgeKey.edgeType,
            view = edgeKey.qualifiedViewId,
            startNode = StartNode(
              nodeViewKey = NodeViewKey(
                nodeType = rvd.startNodeToViewDefinition.labelSet,
                qualifiedViewId = toQualifiedViewId(maybeSetSchema, rvd.startNodeToViewDefinition.viewDefinition.viewId)
              ),
              joinPredicates = rvd.startNodeToViewDefinition.joinOn.joinPredicates.map(toJoin(
                nodeAlias = rvd.startNodeToViewDefinition.viewDefinition.alias,
                edgeAlias = rvd.viewDefinition.alias
              ))
            ),
            endNode = EndNode(
              nodeViewKey = NodeViewKey(
                nodeType = rvd.endNodeToViewDefinition.labelSet,
                qualifiedViewId = toQualifiedViewId(maybeSetSchema, rvd.endNodeToViewDefinition.viewDefinition.viewId)
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
    }
  }

  def toQualifiedViewId(
    maybeSetSchema: Option[SetSchemaDefinition],
    viewId: List[String]
  ): QualifiedViewId = (maybeSetSchema, viewId) match {
    case (None, dataSource :: schema :: view :: Nil) =>
      QualifiedViewId(dataSource, schema, view)
    case (Some(SetSchemaDefinition(dataSource, schema)), view :: Nil) =>
      QualifiedViewId(dataSource, schema, view)
    case (None, view)    if view.size < 3 =>
      Err.malformed("Relative view identifier requires a preceeding SET SCHEMA statement", view.mkString("."))
    case (Some(_), view) if view.size > 1 =>
      Err.malformed("Relative view identifier must have exactly one segment", view.mkString("."))
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
        if (!aliases.contains(leftAlias))  Err.unresolved("Unresolved alias", leftAlias, aliases)
        if (!aliases.contains(rightAlias)) Err.unresolved("Unresolved alias", rightAlias, aliases)
        failure(s"Unable to resolve aliases: $leftAlias, $rightAlias")
    }
  }

  def toPropertyMappings(
    labels: Set[String],
    schemaPropertyKeys: Set[String],
    maybePropertyMapping: Option[PropertyToColumnMappingDefinition]
  ): PropertyMappings = {
    val mappings = maybePropertyMapping.getOrElse(Map.empty)
    mappings.keys
      .filterNot(schemaPropertyKeys)
      .foreach(p => Err.unresolved("Unresolved property name", p, schemaPropertyKeys))

    schemaPropertyKeys
      .keyBy(identity)
      .mapValues(prop => mappings.getOrElse(prop, prop))

  }

  def failure(msg: String): Nothing = ???

  implicit class TraversableOps[T, C[X] <: Traversable[X]](elems: C[T]) {
    def keyBy[K](key: T => K): Map[K, T] =
      elems.map(t => key(t) -> t).toMap

    def validateDistinctBy[K](key: T => K, msg: String): C[T] = {
      elems.groupBy(key).foreach {
        case (k, vals) if vals.size > 1 => Err.duplicate(msg, k)
        case _                          =>
      }
      elems
    }
  }

  implicit class MapOps[K, V](map: Map[K, V]) {
    def getOrFail(key: K, msg: String): V = map.getOrElse(key, Err.unresolved(msg, key, map.keySet))
  }
}

case class GraphDdl(
  graphs: Map[GraphName, Graph]
)

case class Graph(
  name: GraphName,
  graphType: GraphType,
  nodeToViewMappings: Map[NodeViewKey, NodeToViewMapping],
  edgeToViewMappings: List[EdgeToViewMapping]
)

object QualifiedViewId {
  def apply(qualifiedViewId: String): QualifiedViewId = qualifiedViewId.split("\\.").toList match {
    case dataSource :: schema :: view :: Nil => QualifiedViewId(dataSource, schema, view)
    case _ => GraphDdlException.malformed("Qualified view id did not match pattern dataSource.schema.view", qualifiedViewId)
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

case class NodeViewKey(nodeType: Set[String], qualifiedViewId: QualifiedViewId) {
  override def toString: String = s"node type: ${nodeType.mkString(", ")}, view: $qualifiedViewId"
}

case class EdgeViewKey(edgeType: Set[String], qualifiedViewId: QualifiedViewId) {
  override def toString: String = s"relationship type: ${edgeType.mkString(", ")}, view: $qualifiedViewId"
}
