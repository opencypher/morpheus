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
import org.opencypher.graphddl.GraphDdlException._
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.schema.{PropertyKeys, Schema, SchemaPattern}
import org.opencypher.okapi.api.schema.PropertyKeys.PropertyKeys

import scala.language.higherKinds

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

  private[graphddl] object DdlParts {
    def apply(statements: List[DdlStatement]): DdlParts = {
      val result = statements.foldLeft(DdlParts()) {
        case (parts, s: SetSchemaDefinition)   => parts.copy(maybeSetSchema = Some(s))
        case (parts, s: ElementTypeDefinition) => parts.copy(elementTypes = parts.elementTypes :+ s)
        case (parts, s: GraphTypeDefinition)   => parts.copy(graphTypes = parts.graphTypes :+ s)
        case (parts, s: GraphDefinition)       => parts.copy(graphs = parts.graphs :+ GraphDefinitionWithContext(s, parts.maybeSetSchema))
      }
      result.elementTypes.validateDistinctBy(_.name, "Duplicate element type")
      result.graphTypes.validateDistinctBy(_.name, "Duplicate graph type")
      result.graphs.validateDistinctBy(_.definition.name, "Duplicate graph")
      result
    }
  }
  private[graphddl] case class TypeParts(
    elementTypes: List[ElementTypeDefinition] = List.empty,
    nodeTypes: List[NodeTypeDefinition] = List.empty,
    relTypes: List[RelationshipTypeDefinition] = List.empty,
    patterns: List[PatternDefinition] = List.empty
  )

  private[graphddl] object TypeParts {
    def apply(statements: List[GraphTypeStatement]): TypeParts = {
      val result = statements.foldLeft(TypeParts()) {
        case (parts, s: ElementTypeDefinition)      => parts.copy(elementTypes = parts.elementTypes :+ s)
        case (parts, s: NodeTypeDefinition)         => parts.copy(nodeTypes = parts.nodeTypes :+ s)
        case (parts, s: RelationshipTypeDefinition) => parts.copy(relTypes = parts.relTypes :+ s)
        case (parts, s: PatternDefinition)          => parts.copy(patterns = parts.patterns :+ s)
      }
      result.elementTypes.validateDistinctBy(_.name, "Duplicate element type")
      result.nodeTypes.validateDistinctBy(_.elementTypes, "Duplicate node type")
      result.relTypes.validateDistinctBy(_.elementType, "Duplicate relationship type")
      result
    }
  }

  private[graphddl] case class GraphParts(
    graphTypeStatements: List[GraphTypeStatement] = List.empty,
    nodeMappings: List[NodeMappingDefinition] = List.empty,
    relMappings: List[RelationshipMappingDefinition] = List.empty
  )

  private[graphddl] object GraphParts {
    def apply(mappings: List[GraphStatement]): GraphParts =
      mappings.foldLeft(GraphParts()) {
        case (parts, s: GraphTypeStatement)            => parts.copy(graphTypeStatements = parts.graphTypeStatements :+ s)
        case (parts, s: NodeMappingDefinition)         => parts.copy(nodeMappings = parts.nodeMappings :+ s)
        case (parts, s: RelationshipMappingDefinition) => parts.copy(relMappings = parts.relMappings :+ s)
      }
  }

  private[graphddl] case class GraphDefinitionWithContext(
    definition: GraphDefinition,
    maybeSetSchema: Option[SetSchemaDefinition] = None
  )

  def apply(ddl: String): GraphDdl =
    GraphDdl(GraphDdlParser.parse(ddl))

  def apply(ddl: DdlDefinition): GraphDdl = {

    val ddlParts = DdlParts(ddl.statements)

    val global = GraphType().push(ddlParts.elementTypes)

    val graphTypes = ddlParts.graphTypes
      .keyBy(_.name)
      .mapValues { graphType => tryWithGraphType(graphType.name) {
        global.push(graphType.statements)
      }}
      .view.force

    val graphs = ddlParts.graphs
      .map { graph => tryWithGraph(graph.definition.name) {
        val graphType = graph.definition.maybeGraphTypeName
          .map(name => graphTypes.getOrFail(name, "Unresolved graph type"))
          .getOrElse(global)
        toGraph(graphType, graph)
      }}
      .keyBy(_.name)

    GraphDdl(
      graphs = graphs
    )
  }
  def tryWithGraphType[T](name: String)(block: => T): T =
    tryWithContext(s"Error in graph type: $name")(block)

  def tryWithGraph[T](name: String)(block: => T): T =
    tryWithContext(s"Error in graph: $name")(block)

  private[graphddl] case class GraphType(
    parent: Option[GraphType] = None,
    elementTypes: Map[String, ElementTypeDefinition] = Map(),
    nodeTypes: Map[Set[String], PropertyKeys] = Map(),
    edgeTypes: Map[String, PropertyKeys] = Map(),
    patterns: Set[SchemaPattern] = Set()
  ) {
    lazy val allElementTypes: Map[String, ElementTypeDefinition] =
      parent.map(_.allElementTypes).getOrElse(Map()) ++ elementTypes

    lazy val allNodeTypes: Map[Set[String], PropertyKeys] =
      parent.map(_.allNodeTypes).getOrElse(Map()) ++ nodeTypes

    lazy val allEdgeTypes: Map[String, PropertyKeys] =
      parent.map(_.allEdgeTypes).getOrElse(Map()) ++ edgeTypes

    lazy val allPatterns: Set[SchemaPattern] =
      parent.map(_.allPatterns).getOrElse(Set()) ++ patterns

    lazy val asOkapiSchema: Schema = {
      Schema.empty
        .foldLeftOver(allNodeTypes) { case (schema, (labels, properties)) =>
          schema.withNodePropertyKeys(labels, properties)
        }
        .foldLeftOver(allNodeTypes.keySet.flatten.map(resolveElementType)) { case (schema, eType) =>
          eType.maybeKey.fold(schema)(key => schema.withNodeKey(eType.name, key._2))
        }
        .foldLeftOver(allEdgeTypes) { case (schema, (label, properties)) =>
          schema.withRelationshipPropertyKeys(label, properties)
        }
        .foldLeftOver(allEdgeTypes.keySet.map(resolveElementType)) { case (schema, eType) =>
          eType.maybeKey.fold(schema)(key => schema.withNodeKey(eType.name, key._2))
        }
        .withSchemaPatterns(allPatterns.toSeq: _*)
    }

    /** Validates, resolves and pushes the statements to form a child GraphType */
    def push(statements: List[GraphTypeStatement]): GraphType = {
      val parts = TypeParts(statements)

      val local = GraphType(Some(this), parts.elementTypes.keyBy(_.name))

      val patterns = for {
        pattern <- parts.patterns
        source <- pattern.sourceNodeTypes
        edge <- pattern.relTypes
        target <- pattern.targetNodeTypes
      } yield SchemaPattern(source, edge, target)

      GraphType(
        parent = Some(this),

        elementTypes = local.elementTypes,

        nodeTypes = Set(
          parts.nodeTypes.map(_.elementTypes),
          patterns.map(_.sourceLabelCombination),
          patterns.map(_.targetLabelCombination)
        ).flatten.map(labels => labels -> tryWithNode(labels)(
          mergeProperties(labels.map(local.resolveElementType))
        )).toMap,

        edgeTypes = Set(
          parts.relTypes.map(_.elementType),
          patterns.map(_.relType)
        ).flatten.map(label => label -> tryWithRel(label)(
          mergeProperties(Set(local.resolveElementType(label)))
        )).toMap,

        patterns =
          patterns.toSet
      )
    }

    private def tryWithNode[T](labels: Set[String])(block: => T): T =
      tryWithContext(s"Error in node type: (${labels.mkString(",")})")(block)

    private def tryWithRel[T](label: String)(block: => T): T =
      tryWithContext(s"Error in relationship type: [$label]")(block)

    private def mergeProperties(elementTypes: Set[ElementTypeDefinition]): PropertyKeys = {
      elementTypes
        .flatMap(_.properties)
        .foldLeft(PropertyKeys.empty) { case (props, (name, ctype)) =>
          props.get(name).filter(_ != ctype) match {
            case Some(t) => incompatibleTypes(name, ctype, t)
            case None    => props.updated(name, ctype)
          }
        }
    }

    private def resolveElementType(name: String): ElementTypeDefinition =
      allElementTypes.getOrElse(name, unresolved(s"Unresolved element type", name))
  }

  private def toGraph(
    parent: GraphType,
    graph: GraphDefinitionWithContext
  ): Graph = {
    val parts = GraphParts(graph.definition.statements)
    val graphType = parent
      .push(parts.graphTypeStatements)
      .push(parts.nodeMappings.map(_.nodeType) ++ parts.relMappings.map(_.relType))

    Graph(
      name = GraphName(graph.definition.name),
      graphType = graphType.asOkapiSchema,
      nodeToViewMappings = parts.nodeMappings
        .flatMap(nm => toNodeToViewMappings(graphType.asOkapiSchema, graph.maybeSetSchema, nm))
        .validateDistinctBy(_.key, "Duplicate mapping")
        .keyBy(_.key),
      edgeToViewMappings = parts.relMappings
        // TODO: Validate distinct edge mappings
        .flatMap(em => toEdgeToViewMappings(graphType.asOkapiSchema, graph.maybeSetSchema, em))
    )
  }

  private def toNodeToViewMappings(
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

  private def toEdgeToViewMappings(
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

  private def toQualifiedViewId(
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

  private def toJoin(nodeAlias: String, edgeAlias: String)(join: (ColumnIdentifier, ColumnIdentifier)): Join = {
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

  private def toPropertyMappings(
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

  // Helper extension methods

  private implicit class TraversableOps[T, C[X] <: Traversable[X]](elems: C[T]) {
    def keyBy[K](key: T => K): Map[K, T] =
      elems.map(t => key(t) -> t).toMap

    def validateDistinctBy[K](key: T => K, msg: String): C[T] = {
      elems.groupBy(key).foreach {
        case (k, values) if values.size > 1 => duplicate(msg, k)
        case _                              =>
      }
      elems
    }
  }

  private implicit class MapOps[K, V](map: Map[K, V]) {
    def getOrFail(key: K, msg: String): V = map.getOrElse(key, unresolved(msg, key, map.keySet))
  }

  private implicit class SchemaOps(schema: Schema) {
    def foldLeftOver[T](trav: TraversableOnce[T])(op: (Schema, T) => Schema): Schema =
      trav.foldLeft(schema)(op)
  }
}

// Result types

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
