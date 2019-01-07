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

import org.opencypher.graphddl.GraphDdl._
import org.opencypher.graphddl.GraphDdlAst.{ColumnIdentifier, PropertyToColumnMappingDefinition}
import org.opencypher.graphddl.GraphDdlException._
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.okapi.api.schema.{PropertyKeys, Schema, SchemaPattern}
import org.opencypher.okapi.impl.util.ScalaUtils._

import scala.language.higherKinds

object GraphDdl {

  type PropertyMappings = Map[String, String]

  def apply(ddl: String): GraphDdl =
    GraphDdl(GraphDdlParser.parse(ddl))

  def apply(ddl: DdlDefinition): GraphDdl = {

    val ddlParts = DdlParts(ddl.statements)

    val global = GraphType.empty.push(ddlParts.elementTypes)

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

  private[graphddl] object DdlParts {

    val empty: DdlParts = DdlParts()

    def apply(statements: List[DdlStatement]): DdlParts = {
      val result = statements.foldLeft(DdlParts.empty) {
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

  private[graphddl] case class DdlParts(
    maybeSetSchema: Option[SetSchemaDefinition] = None,
    elementTypes: List[ElementTypeDefinition] = List.empty,
    graphTypes: List[GraphTypeDefinition] = List.empty,
    graphs: List[GraphDefinitionWithContext] = List.empty
  )

  private[graphddl] object GraphTypeParts {

    val empty: GraphTypeParts = GraphTypeParts()

    def apply(statements: List[GraphTypeStatement]): GraphTypeParts = {
      val result = statements.foldLeft(GraphTypeParts.empty) {
        case (parts, s: ElementTypeDefinition)      => parts.copy(elementTypes = parts.elementTypes :+ s)
        case (parts, s: NodeTypeDefinition)         => parts.copy(nodeTypes = parts.nodeTypes :+ s)
        case (parts, s: RelationshipTypeDefinition) => parts.copy(relTypes = parts.relTypes :+ s)
      }
      result.elementTypes.validateDistinctBy(_.name, "Duplicate element type")
      result.nodeTypes.validateDistinctBy(identity, "Duplicate node type")
      result.relTypes.validateDistinctBy(identity, "Duplicate relationship type")
      result
    }
  }

  private[graphddl] case class GraphTypeParts(
    elementTypes: List[ElementTypeDefinition] = List.empty,
    nodeTypes: List[NodeTypeDefinition] = List.empty,
    relTypes: List[RelationshipTypeDefinition] = List.empty
  )

  private[graphddl] object GraphParts {

    val empty: GraphParts = GraphParts()

    def apply(mappings: List[GraphStatement]): GraphParts =
      mappings.foldLeft(GraphParts.empty) {
        case (parts, s: GraphTypeStatement)            => parts.copy(graphTypeStatements = parts.graphTypeStatements :+ s)
        case (parts, s: NodeMappingDefinition)         => parts.copy(nodeMappings = parts.nodeMappings :+ s)
        case (parts, s: RelationshipMappingDefinition) => parts.copy(relMappings = parts.relMappings :+ s)
      }
  }

  private[graphddl] case class GraphParts(
    graphTypeStatements: List[GraphTypeStatement] = List.empty,
    nodeMappings: List[NodeMappingDefinition] = List.empty,
    relMappings: List[RelationshipMappingDefinition] = List.empty
  )

  private[graphddl] case class GraphDefinitionWithContext(
    definition: GraphDefinition,
    maybeSetSchema: Option[SetSchemaDefinition] = None
  )

  object GraphType {
    val empty: GraphType = GraphType()
  }

  private[graphddl] case class GraphType(
    parent: Option[GraphType] = None,
    elementTypes: Map[String, ElementTypeDefinition] = Map.empty,
    nodeTypes: Map[Set[String], PropertyKeys] = Map.empty,
    edgeTypes: Map[String, PropertyKeys] = Map.empty,
    patterns: Set[SchemaPattern] = Set.empty
  ) {
    lazy val allElementTypes: Map[String, ElementTypeDefinition] =
      parent.map(_.allElementTypes).getOrElse(Map.empty) ++ elementTypes

    lazy val allNodeTypes: Map[Set[String], PropertyKeys] =
      parent.map(_.allNodeTypes).getOrElse(Map.empty) ++ nodeTypes

    lazy val allEdgeTypes: Map[String, PropertyKeys] =
      parent.map(_.allEdgeTypes).getOrElse(Map.empty) ++ edgeTypes

    lazy val allPatterns: Set[SchemaPattern] =
      parent.map(_.allPatterns).getOrElse(Set.empty) ++ patterns

    lazy val asOkapiSchema: Schema = Schema.empty
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

    /** Validates, resolves and pushes the statements to form a child GraphType */
    def push(statements: List[GraphTypeStatement]): GraphType = {
      val parts = GraphTypeParts(statements)

      val local = GraphType(Some(this), parts.elementTypes.keyBy(_.name))

      GraphType(
        parent = Some(this),

        elementTypes = local.elementTypes,

        nodeTypes = Set(
          parts.nodeTypes.map(_.elementTypes),
          parts.relTypes.map(_.sourceNodeType.elementTypes),
          parts.relTypes.map(_.targetNodeType.elementTypes)
        ).flatten.map(labels => labels -> tryWithNode(labels)(
          mergeProperties(labels.map(local.resolveElementType))
        )).toMap,

        edgeTypes = Set(
          parts.relTypes.map(_.elementType)
        ).flatten.map(label => label -> tryWithRel(label)(
          mergeProperties(Set(local.resolveElementType(label)))
        )).toMap,

        patterns = parts.relTypes.map(relType => SchemaPattern(
          relType.sourceNodeType.elementTypes,
          relType.elementType,
          relType.targetNodeType.elementTypes
        )).toSet
      )
    }

    private def mergeProperties(elementTypes: Set[ElementTypeDefinition]): PropertyKeys = {
      elementTypes
        .flatMap(_.properties)
        .foldLeft(PropertyKeys.empty) { case (props, (name, cypherType)) =>
          props.get(name).filter(_ != cypherType) match {
            case Some(t) => incompatibleTypes(name, cypherType, t)
            case None    => props.updated(name, cypherType)
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
        .validateDistinctBy(_.key, "Duplicate node mapping")
        .keyBy(_.key),
      edgeToViewMappings = parts.relMappings
        .flatMap(em => toEdgeToViewMappings(graphType.asOkapiSchema, graph.maybeSetSchema, em))
        .validateDistinctBy(_.key, "Duplicate relationship mapping")
    )
  }

  private def toNodeToViewMappings(
    graphType: Schema,
    maybeSetSchema: Option[SetSchemaDefinition],
    nmd: NodeMappingDefinition
  ): Seq[NodeToViewMapping] = {
    nmd.nodeToView.map { nvd =>
      tryWithContext(s"Error in node mapping for: ${nmd.nodeType.elementTypes.mkString(",")}") {

        val nodeKey = NodeViewKey(toNodeType(nmd.nodeType), toViewId(maybeSetSchema, nvd.viewId))

        tryWithContext(s"Error in node mapping for: $nodeKey") {
          NodeToViewMapping(
            nodeType = nodeKey.nodeType,
            view = toViewId(maybeSetSchema, nvd.viewId),
            propertyMappings = toPropertyMappings(
              elementTypes = nodeKey.nodeType.elementTypes,
              graphTypePropertyKeys = graphType.nodePropertyKeys(nodeKey.nodeType.elementTypes).keySet,
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

        val edgeKey = EdgeViewKey(toRelType(rmd.relType), toViewId(maybeSetSchema, rvd.viewDef.viewId))

        tryWithContext(s"Error in relationship mapping for: $edgeKey") {
          EdgeToViewMapping(
            relType = edgeKey.relType,
            view = edgeKey.viewId,
            startNode = StartNode(
              nodeViewKey = NodeViewKey(
                nodeType = edgeKey.relType.startNodeType,
                viewId = toViewId(maybeSetSchema, rvd.startNodeTypeToView.viewDef.viewId)
              ),
              joinPredicates = rvd.startNodeTypeToView.joinOn.joinPredicates.map(toJoin(
                nodeAlias = rvd.startNodeTypeToView.viewDef.alias,
                edgeAlias = rvd.viewDef.alias
              ))
            ),
            endNode = EndNode(
              nodeViewKey = NodeViewKey(
                nodeType = edgeKey.relType.endNodeType,
                viewId = toViewId(maybeSetSchema, rvd.endNodeTypeToView.viewDef.viewId)
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

  private def toViewId(
    maybeSetSchema: Option[SetSchemaDefinition],
    viewId: List[String]
  ): ViewId = ViewId(maybeSetSchema, viewId)

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

  private def toNodeType(nodeTypeDefinition: NodeTypeDefinition): NodeType =
    NodeType(nodeTypeDefinition.elementTypes)

  private def toRelType(relTypeDefinition: RelationshipTypeDefinition): RelationshipType =
    RelationshipType(
      startNodeType = toNodeType(relTypeDefinition.sourceNodeType),
      elementType = relTypeDefinition.elementType,
      endNodeType = toNodeType(relTypeDefinition.targetNodeType))

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

  private def tryWithGraphType[T](name: String)(block: => T): T =
    tryWithContext(s"Error in graph type: $name")(block)

  private def tryWithGraph[T](name: String)(block: => T): T =
    tryWithContext(s"Error in graph: $name")(block)

  private def tryWithNode[T](labels: Set[String])(block: => T): T =
    tryWithContext(s"Error in node type: (${labels.mkString(",")})")(block)

  private def tryWithRel[T](label: String)(block: => T): T =
    tryWithContext(s"Error in relationship type: [$label]")(block)

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

case class ViewId(maybeSetSchema: Option[SetSchemaDefinition], parts: List[String]) {
  lazy val dataSource: String = {
    (maybeSetSchema, parts) match {
      case (_, dataSource :: schema :: view :: Nil) => dataSource
      case (None, dataSource :: tail) if tail.nonEmpty => dataSource
      case (Some(setSchema), _) => setSchema.dataSource
      case _ => malformed("Relative view identifier requires a preceeding SET SCHEMA statement", parts.mkString("."))
    }
  }
}

sealed trait ElementToViewMapping

case class NodeToViewMapping(
  nodeType: NodeType,
  view: ViewId,
  propertyMappings: PropertyMappings
) extends ElementToViewMapping {
  def key: NodeViewKey = NodeViewKey(nodeType, view)
}

case class EdgeToViewMapping(
  relType: RelationshipType,
  view: ViewId,
  startNode: StartNode,
  endNode: EndNode,
  propertyMappings: PropertyMappings
) extends ElementToViewMapping {
  def key: EdgeViewKey = EdgeViewKey(relType, view)
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

object NodeType {
  def apply(elementTypes: String*): NodeType = NodeType(elementTypes.toSet)
}

case class NodeType(elementTypes: Set[String]) {
  override def toString: String = s"(${elementTypes.mkString(", ")})"
}

object RelationshipType {
  def apply(startNodeElementType: String, elementType: String, endNodeElementType: String): RelationshipType =
    RelationshipType(NodeType(startNodeElementType), elementType, NodeType(endNodeElementType))
}

case class RelationshipType(startNodeType: NodeType, elementType: String, endNodeType: NodeType) {
  override def toString: String = s"$startNodeType-[$elementType]->$endNodeType"

}

trait ElementViewKey {
  def elementType: Set[String]
  def viewId: ViewId
}

case class NodeViewKey(nodeType: NodeType, viewId: ViewId) extends ElementViewKey {
  override val elementType: Set[String] = nodeType.elementTypes

  override def toString: String = s"node type: $nodeType, view: $viewId"
}

case class EdgeViewKey(relType: RelationshipType, viewId: ViewId) extends ElementViewKey {
  override val elementType: Set[String] = Set(relType.elementType)

  override def toString: String = s"relationship type: $relType, view: $viewId"
}
