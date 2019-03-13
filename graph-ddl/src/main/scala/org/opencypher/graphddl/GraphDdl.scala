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

import cats.instances.map._
import cats.syntax.semigroup._
import org.opencypher.graphddl.GraphDdl._
import org.opencypher.graphddl.GraphDdlAst.{ColumnIdentifier, KeyDefinition, PropertyToColumnMappingDefinition}
import org.opencypher.graphddl.GraphDdlException._
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.schema.PropertyKeys
import org.opencypher.okapi.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.okapi.api.types.CypherType

import scala.language.higherKinds

object GraphDdl {

  type PropertyMappings = Map[String, String]

  def apply(ddl: String): GraphDdl =
    GraphDdl(GraphDdlParser.parseDdl(ddl))

  def apply(ddl: DdlDefinition): GraphDdl = {

    val ddlParts = DdlParts(ddl.statements)

    val global = PartialGraphType.empty.push(name = "global", statements = ddlParts.elementTypes)

    val graphTypes = ddlParts.graphTypes
      .keyBy(_.name)
      .mapValues { graphType =>
        tryWithGraphType(graphType.name) {
          global.push(graphType.name, graphType.statements)
        }
      }
      .view.force

    val graphs = ddlParts.graphs
      .map { graph =>
        tryWithGraph(graph.definition.name) {
          val graphType = graph.definition.maybeGraphTypeName
            .map(name => graphTypes.getOrFail(name, "Unresolved graph type"))
            .getOrElse(global)
          toGraph(graphType, graph)
        }
      }
      .keyBy(_.name)

    GraphDdl(
      graphs = graphs
    )
  }

  private[graphddl] object DdlParts {

    val empty: DdlParts = DdlParts()

    def apply(statements: List[DdlStatement]): DdlParts = {
      val result = statements.foldLeft(DdlParts.empty) {
        case (parts, s: SetSchemaDefinition) => parts.copy(maybeSetSchema = Some(s))
        case (parts, s: ElementTypeDefinition) => parts.copy(elementTypes = parts.elementTypes :+ s)
        case (parts, s: GraphTypeDefinition) => parts.copy(graphTypes = parts.graphTypes :+ s)
        case (parts, s: GraphDefinition) => parts.copy(graphs = parts.graphs :+ GraphDefinitionWithContext(s, parts.maybeSetSchema))
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
        case (parts, s: ElementTypeDefinition) => parts.copy(elementTypes = parts.elementTypes :+ s)
        case (parts, s: NodeTypeDefinition) => parts.copy(nodeTypes = parts.nodeTypes :+ s)
        case (parts, s: RelationshipTypeDefinition) => parts.copy(relTypes = parts.relTypes :+ s)
      }
      result.elementTypes.validateDistinctBy(_.name, "Duplicate element type")
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
        case (parts, s: GraphTypeStatement) => parts.copy(graphTypeStatements = parts.graphTypeStatements :+ s)
        case (parts, s: NodeMappingDefinition) => parts.copy(nodeMappings = parts.nodeMappings :+ s)
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

  object PartialGraphType {
    val empty: PartialGraphType = PartialGraphType(name = "empty")
  }

  private[graphddl] case class PartialGraphType(
    parent: Option[PartialGraphType] = None,
    name: String,
    elementTypes: Map[String, ElementTypeDefinition] = Map.empty,
    nodeTypes: Map[NodeTypeDefinition, PropertyKeys] = Map.empty,
    edgeTypes: Map[RelationshipTypeDefinition, PropertyKeys] = Map.empty
  ) {
    lazy val allElementTypes: Map[String, ElementTypeDefinition] =
      parent.map(_.allElementTypes).getOrElse(Map.empty) ++ elementTypes

    lazy val allNodeTypes: Map[NodeTypeDefinition, PropertyKeys] =
      parent.map(_.allNodeTypes).getOrElse(Map.empty) ++ nodeTypes

    lazy val allEdgeTypes: Map[RelationshipTypeDefinition, PropertyKeys] =
      parent.map(_.allEdgeTypes).getOrElse(Map.empty) ++ edgeTypes

    /** Validates, resolves and pushes the statements to form a child GraphType */
    def push(name: String, statements: List[GraphTypeStatement]): PartialGraphType = {
      val parts = GraphTypeParts(statements)

      val local = PartialGraphType(Some(this), name, parts.elementTypes.keyBy(_.name))

      PartialGraphType(
        parent = Some(this),
        name,
        elementTypes = local.elementTypes,
        nodeTypes = resolveNodeTypes(parts, local),
        edgeTypes = resolveRelationshipTypes(parts, local)
      )
    }

    def toGraphType: GraphType = GraphType(name,
      allElementTypes.values.toSet.map { elementTypeDef: ElementTypeDefinition => toElementType(elementTypeDef) },
      allNodeTypes.map { case (nodeTypeDef, _) => toNodeType(nodeTypeDef) }.toSet,
      allEdgeTypes.map { case (relTypeDef, _) => toRelType(relTypeDef) }.toSet)

    def toElementType(elementTypeDefinition: ElementTypeDefinition): ElementType = {
      val parentTypes = elementTypeDefinition.parents.map(resolveElementType).map(toElementType).map(_.name)
      ElementType(elementTypeDefinition.name, parentTypes, elementTypeDefinition.properties, elementTypeDefinition.maybeKey)
    }

    def toNodeType(nodeTypeDefinition: NodeTypeDefinition): NodeType =
      NodeType(resolveNodeLabels(nodeTypeDefinition))

    def toRelType(relationshipTypeDefinition: RelationshipTypeDefinition): RelationshipType =
      RelationshipType(
        startNodeType = toNodeType(relationshipTypeDefinition.startNodeType),
        labels = resolveRelationshipLabel(relationshipTypeDefinition),
        endNodeType = toNodeType(relationshipTypeDefinition.endNodeType))

    private def resolveNodeTypes(
      parts: GraphTypeParts,
      local: PartialGraphType
    ): Map[NodeTypeDefinition, PropertyKeys] = {
      parts.nodeTypes
        .map(nodeType => nodeType.copy(elementTypes = local.resolveNodeLabels(nodeType)))
        .validateDistinctBy(identity, "Duplicate node type")
        .map(nodeType => nodeType -> tryWithNode(nodeType)(mergeProperties(nodeType.elementTypes.flatMap(local.resolveElementTypes))))
        .toMap
    }

    private def resolveRelationshipTypes(
      parts: GraphTypeParts,
      local: PartialGraphType
    ): Map[RelationshipTypeDefinition, PropertyKeys] = {
      parts.relTypes
        .map(relType => relType.copy(
          startNodeType = relType.startNodeType.copy(elementTypes = local.resolveNodeLabels(relType.startNodeType)),
          elementTypes = local.resolveRelationshipLabel(relType),
          endNodeType = relType.endNodeType.copy(elementTypes = local.resolveNodeLabels(relType.endNodeType))))
        .validateDistinctBy(identity, "Duplicate relationship type")
        .map(relType => relType -> tryWithRel(relType)(mergeProperties(relType.elementTypes.flatMap(local.resolveElementTypes))))
        .toMap
    }

    private def resolveNodeLabels(nodeTypeDefinition: NodeTypeDefinition): Set[String] =
      tryWithNode(nodeTypeDefinition)(nodeTypeDefinition.elementTypes.flatMap(resolveElementTypes).map(_.name))

    private def resolveRelationshipLabel(relTypeDefinition: RelationshipTypeDefinition): Set[String] =
      tryWithRel(relTypeDefinition)(relTypeDefinition.elementTypes.flatMap(resolveElementTypes).map(_.name))

    private def mergeProperties(elementTypes: Set[ElementTypeDefinition]): PropertyKeys = {
      elementTypes
        .flatMap(_.properties)
        .foldLeft(PropertyKeys.empty) { case (props, (name, cypherType)) =>
          props.get(name).filter(_ != cypherType) match {
            case Some(t) => incompatibleTypes(name, cypherType, t)
            case None => props.updated(name, cypherType)
          }
        }
    }

    private def resolveElementTypes(name: String): Set[ElementTypeDefinition] = {
      val elementType = resolveElementType(name)
      detectCircularDependency(elementType)
      resolveParents(elementType)
    }

    private def resolveElementType(name: String): ElementTypeDefinition =
      allElementTypes.getOrElse(name, unresolved(s"Unresolved element type", name))

    private def resolveParents(node: ElementTypeDefinition): Set[ElementTypeDefinition] =
      node.parents.map(resolveElementType).flatMap(resolveParents) + node

    private def detectCircularDependency(node: ElementTypeDefinition): Unit = {
      def traverse(node: ElementTypeDefinition, path: List[ElementTypeDefinition]): Unit = {
        node.parents.foreach { p =>
          val parentElementType = allElementTypes.getOrElse(p, unresolved(s"Unresolved element type", p))
          if (path.contains(parentElementType)) {
            illegalInheritance("Circular dependency detected", (path.map(_.name) :+ p).mkString(" -> "))
          }
          traverse(parentElementType, path :+ parentElementType)
        }
      }

      traverse(node, List(node))
    }
  }

  private def toGraph(
    parent: PartialGraphType,
    graph: GraphDefinitionWithContext
  ): Graph = {
    val parts = GraphParts(graph.definition.statements)
    val graphTypeName = graph.definition.maybeGraphTypeName.getOrElse(graph.definition.name)
    val partialGraphType = parent
      .push(graphTypeName, parts.graphTypeStatements)
      .push(graphTypeName, parts.nodeMappings.map(_.nodeType) ++ parts.relMappings.map(_.relType))
    val graphType = partialGraphType.toGraphType

    val g = Graph(
      name = GraphName(graph.definition.name),
      graphType = graphType,
      nodeToViewMappings = parts.nodeMappings
        .flatMap(nmd => toNodeToViewMappings(partialGraphType.toNodeType(nmd.nodeType), graphType, graph.maybeSetSchema, nmd))
        .validateDistinctBy(_.key, "Duplicate node mapping")
        .keyBy(_.key),
      edgeToViewMappings = parts.relMappings
        .flatMap(rmd => toEdgeToViewMappings(partialGraphType.toRelType(rmd.relType), graphType, graph.maybeSetSchema, rmd))
        .validateDistinctBy(_.key, "Duplicate relationship mapping")
    )

    g.edgeToViewMappings.flatMap(evm => Seq(
      evm.startNode.nodeViewKey -> evm.startNodeJoinColumns.toSet,
      evm.endNode.nodeViewKey -> evm.endNodeJoinColumns.toSet
    )).distinct
      .validateDistinctBy({ case (nvk, _) => nvk }, msg = "Inconsistent join column definition", illegalConstraint)
    g
  }

  private def toNodeToViewMappings(
    nodeType: NodeType,
    graphType: GraphType,
    maybeSetSchema: Option[SetSchemaDefinition],
    nmd: NodeMappingDefinition
  ): Seq[NodeToViewMapping] = {
    nmd.nodeToView.map { nvd =>
      tryWithContext(s"Error in node mapping for: ${nmd.nodeType.elementTypes.mkString(",")}") {

        val nodeKey = NodeViewKey(nodeType, toViewId(maybeSetSchema, nvd.viewId))

        tryWithContext(s"Error in node mapping for: $nodeKey") {
          NodeToViewMapping(
            nodeType = nodeKey.nodeType,
            view = toViewId(maybeSetSchema, nvd.viewId),
            propertyMappings = toPropertyMappings(
              elementTypes = nodeKey.nodeType.labels,
              graphTypePropertyKeys = graphType.nodePropertyKeys(nodeKey.nodeType).keySet,
              maybePropertyMapping = nvd.maybePropertyMapping
            )
          )
        }
      }
    }
  }

  private def toEdgeToViewMappings(
    relType: RelationshipType,
    graphType: GraphType,
    maybeSetSchema: Option[SetSchemaDefinition],
    rmd: RelationshipMappingDefinition
  ): Seq[EdgeToViewMapping] = {
    rmd.relTypeToView.map { rvd =>
      tryWithContext(s"Error in relationship mapping for: ${rmd.relType}") {

        val edgeKey = EdgeViewKey(relType, toViewId(maybeSetSchema, rvd.viewDef.viewId))

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
              elementTypes = edgeKey.relType.labels,
              graphTypePropertyKeys = graphType.relationshipPropertyKeys(edgeKey.relType).keySet,
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

  private def tryWithNode[T](nodeTypeDefinition: NodeTypeDefinition)(block: => T): T =
    tryWithContext(s"Error in node type: $nodeTypeDefinition")(block)

  private def tryWithRel[T](relationshipTypeDefinition: RelationshipTypeDefinition)(block: => T): T =
    tryWithContext(s"Error in relationship type: $relationshipTypeDefinition")(block)

  private implicit class TraversableOps[T, C[X] <: Traversable[X]](elems: C[T]) {
    def keyBy[K](key: T => K): Map[K, T] =
      elems.map(t => key(t) -> t).toMap

    def validateDistinctBy[K](key: T => K, msg: String, error: (String, Any) => Nothing = duplicate): C[T] = {
      elems.groupBy(key).foreach {
        case (k, values) if values.size > 1 => error(msg, k)
        case _ =>
      }
      elems
    }
  }

  private implicit class MapOps[K, V](map: Map[K, V]) {
    def getOrFail(key: K, msg: String): V = map.getOrElse(key, unresolved(msg, key, map.keySet))
  }
}

// Result types

case class GraphDdl(graphs: Map[GraphName, Graph]) {
  def ++(other: GraphDdl): GraphDdl = GraphDdl(graphs ++ other.graphs)
}

case class Graph(
  name: GraphName,
  graphType: GraphType,
  nodeToViewMappings: Map[NodeViewKey, NodeToViewMapping],
  edgeToViewMappings: List[EdgeToViewMapping]
) {

  def nodeIdColumnsFor(nodeViewKey: NodeViewKey): Option[List[String]] = edgeToViewMappings.collectFirst {
    case evm: EdgeToViewMapping if evm.startNode.nodeViewKey == nodeViewKey => evm.startNodeJoinColumns
    case evm: EdgeToViewMapping if evm.endNode.nodeViewKey == nodeViewKey => evm.endNodeJoinColumns
  }
}

object GraphType {
  val empty = GraphType("empty")
}

case class GraphType(
  name: String,
  elementTypes: Set[ElementType] = Set.empty,
  nodeTypes: Set[NodeType] = Set.empty,
  relTypes: Set[RelationshipType] = Set.empty
) {

  private lazy val elementTypesByName = elementTypes.map(et => et.name -> et).toMap

  def nodeElementTypes: Set[ElementType] = nodeTypes.flatMap(_.labels).map(elementTypesByName)

  def relElementTypes: Set[ElementType] = relTypes.flatMap(_.labels).map(elementTypesByName)

  def nodePropertyKeys(labels: String*): PropertyKeys =
    nodePropertyKeys(NodeType(labels.toSet))

  def nodePropertyKeys(nodeType: NodeType): PropertyKeys =
    getPropertyKeys(nodeType.labels)

  def relationshipPropertyKeys(startLabel: String, label: String, endLabel: String): PropertyKeys =
    relationshipPropertyKeys(RelationshipType(startLabel, label, endLabel))

  def relationshipPropertyKeys(startLabels: Set[String], labels: Set[String], endLabels: Set[String]): PropertyKeys =
    relationshipPropertyKeys(RelationshipType(NodeType(startLabels), labels, NodeType(endLabels)))

  def relationshipPropertyKeys(relationshipType: RelationshipType): PropertyKeys =
    getPropertyKeys(relationshipType.labels)

  def withName(name: String): GraphType =
    copy(name = name)

  def withElementType(label: String, propertyKeys: (String, CypherType)*): GraphType =
    withElementType(ElementType(name = label, properties = propertyKeys.toMap))

  def withElementType(label: String, parents: Set[String], propertyKeys: (String, CypherType)*): GraphType =
    withElementType(ElementType(name = label, parents = parents, properties = propertyKeys.toMap))

  def withElementType(elementType: ElementType): GraphType = {
    validateElementType(elementType)
    copy(elementTypes = elementTypes + elementType)
  }

  def withNodeType(labels: String*): GraphType = withNodeType(NodeType(labels: _*))

  def withNodeType(nodeType: NodeType): GraphType = {
    validateNodeType(nodeType)
    copy(nodeTypes = nodeTypes + expandNodeType(nodeType))
  }

  def withRelationshipType(startLabel: String, label: String, endLabel: String): GraphType =
    withRelationshipType(Set(startLabel), Set(label), Set(endLabel))

  def withRelationshipType(startLabels: Set[String], labels: Set[String], endLabels: Set[String]): GraphType =
    withRelationshipType(RelationshipType(NodeType(startLabels), labels, NodeType(endLabels)))

  def withRelationshipType(relationshipType: RelationshipType): GraphType = {
    validateRelType(relationshipType)
    copy(relTypes = relTypes + expandRelType(relationshipType))
  }

  private def validateElementType(elementType: ElementType): Unit = tryWithContext(elementType.toString) {
    if (elementTypes.contains(elementType)) {
      duplicate("Element type already exists", elementType)
    }
    elementType.parents.foreach(validateElementTypeHierarchy)
  }

  private def validateElementTypeHierarchy(parent: String): Unit = {
    if (elementTypes.exists(_.name == parent)) {
      elementTypes.collectFirst { case et if et.name == parent => et }.get.parents.foreach(validateElementTypeHierarchy)
    } else {
      illegalInheritance("Element type not found", parent)
    }
  }

  private def expandNodeType(nodeType: NodeType): NodeType =
    nodeType.copy(labels = nodeType.labels.flatMap(getElementTypes).map(_.name))

  private def expandRelType(relType: RelationshipType): RelationshipType =
    relType.copy(
      startNodeType = expandNodeType(relType.startNodeType),
      labels = relType.labels.flatMap(getElementTypes).map(_.name),
      endNodeType = expandNodeType(relType.endNodeType))

  private def validateNodeType(nodeType: NodeType): Unit = tryWithContext(nodeType.toString) {
    nodeType.labels.foreach(validateElementTypeHierarchy)
  }

  private def validateRelType(relType: RelationshipType): Unit = tryWithContext(relType.toString) {
    validateNodeType(relType.startNodeType)
    validateNodeType(relType.endNodeType)
    relType.labels.foreach(validateElementTypeHierarchy)
  }

  private def getElementTypes(label: String): Set[ElementType] = {
    val children = elementTypes.filter(_.name == label)
    children ++ children.flatMap(_.parents.flatMap(getElementTypes))
  }

  private def getPropertyKeys(labels: Set[String]): PropertyKeys =
    labels.flatMap(getElementTypes).map(_.properties).reduce(_ |+| _)
}

case class ViewId(maybeSetSchema: Option[SetSchemaDefinition], parts: List[String]) {
  lazy val dataSource: String = {
    (maybeSetSchema, parts) match {
      case (_, ds :: _ :: _ :: Nil) => ds
      case (None, ds :: tail) if tail.nonEmpty => ds
      case (Some(setSchema), _) => setSchema.dataSource
      case _ => malformed("Relative view identifier requires a preceding SET SCHEMA statement", parts.mkString("."))
    }
  }

  lazy val tableName: String = (maybeSetSchema, parts) match {
    case (_, _ :: schema :: view :: Nil) => s"$schema.$view"
    case (Some(SetSchemaDefinition(_, schema)), view :: Nil) => s"$schema.$view"
    case (None, view) if view.size < 3 =>
      malformed("Relative view identifier requires a preceding SET SCHEMA statement", view.mkString("."))
    case (Some(_), view) if view.size > 1 =>
      malformed("Relative view identifier must have exactly one segment", view.mkString("."))
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

  def startNodeJoinColumns: List[String] = startNode.joinPredicates.map(_.nodeColumn)

  def endNodeJoinColumns: List[String] = endNode.joinPredicates.map(_.nodeColumn)
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

case class ElementType(
  name: String,
  parents: Set[String] = Set.empty,
  properties: Map[String, CypherType] = Map.empty,
  maybeKey: Option[KeyDefinition] = None
)

object NodeType {
  def apply(labels: String*): NodeType = NodeType(labels.toSet)
}

case class NodeType(labels: Set[String]) {
  override def toString: String = s"(${labels.mkString(",")})"
}

object RelationshipType {
  def apply(startNodeElementType: String, label: String, endNodeElementType: String): RelationshipType =
    RelationshipType(NodeType(startNodeElementType), Set(label), NodeType(endNodeElementType))
}

case class RelationshipType(startNodeType: NodeType, labels: Set[String], endNodeType: NodeType) {
  override def toString: String = s"$startNodeType-[${labels.mkString(",")}]->$endNodeType"
}

trait ElementViewKey {
  def elementType: Set[String]

  def viewId: ViewId
}

case class NodeViewKey(nodeType: NodeType, viewId: ViewId) extends ElementViewKey {
  override val elementType: Set[String] = nodeType.labels

  override def toString: String = s"node type: $nodeType, view: $viewId"
}

case class EdgeViewKey(relType: RelationshipType, viewId: ViewId) extends ElementViewKey {
  override val elementType: Set[String] = relType.labels

  override def toString: String = s"relationship type: $relType, view: $viewId"
}
