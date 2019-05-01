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
package org.opencypher.okapi.relational.impl.planning

import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.api.schema.PropertyGraphSchema
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.okapi.impl.util.ScalaUtils._
import org.opencypher.okapi.ir.api.expr.PrefixId.GraphIdPrefix
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.set.{SetLabelItem, SetPropertyItem}
import org.opencypher.okapi.ir.api.{Label, PropertyKey, RelType, expr}
import org.opencypher.okapi.logical.impl._
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.relational.api.io.ElementTable
import org.opencypher.okapi.relational.api.planning.RelationalRuntimeContext
import org.opencypher.okapi.relational.api.schema.RelationalPropertyGraphSchema._
import org.opencypher.okapi.relational.api.table.Table
import org.opencypher.okapi.relational.impl.operators.{ConstructGraph, RelationalOperator}
import org.opencypher.okapi.relational.impl.planning.RelationalPlanner.RelationalOperatorOps
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.okapi.relational.impl.{operators => relational}
import org.opencypher.okapi.impl.types.CypherTypeUtils._

import scala.reflect.runtime.universe.TypeTag

object ConstructGraphPlanner {

  def planConstructGraph[T <: Table[T] : TypeTag](inputTablePlan: RelationalOperator[T], construct: LogicalPatternGraph)
    (implicit context: RelationalRuntimeContext[T]): RelationalOperator[T] = {

    val prefixes = computePrefixes(construct)

    val onGraphs = construct.onGraphs.map { qgn =>
      val start = relational.Start[T](qgn)
      prefixes.get(qgn).map(p => relational.PrefixGraph(start, p)).getOrElse(start).graph
    }

    val constructTable = initConstructTable(inputTablePlan, prefixes, construct)

    val allGraphs = onGraphs ++ {
      if (construct.clones.nonEmpty || construct.newElements.nonEmpty) {
        Some(extractScanGraph(construct, constructTable))
      } else {
        None
      }
    }

    val constructedGraph = allGraphs match {
      case Nil => context.session.graphs.empty
      case head :: Nil => head
      case several => context.session.graphs.unionGraph(several: _*)
    }

    val constructOp = ConstructGraph(constructTable, constructedGraph, construct, context)

    context.queryLocalCatalog += (construct.qualifiedGraphName -> constructOp.graph)

    constructOp
  }

  private def computePrefixes(construct: LogicalPatternGraph): Map[QualifiedGraphName, GraphIdPrefix] = {
    val onGraphQgns = construct.onGraphs
    val cloneGraphQgns = construct.clones.values.flatMap(_.cypherType.graph).toList
    val createGraphQgns = if (construct.newElements.nonEmpty) List(construct.qualifiedGraphName) else Nil
    val graphQgns = (onGraphQgns ++ cloneGraphQgns ++ createGraphQgns).distinct
    if (graphQgns.size <= 1) {
      Map.empty // No prefixes needed when there is at most one graph QGN
    } else {
      graphQgns.zipWithIndex.map { case (qgn, i) =>
        // Assign GraphIdPrefix `11111111` to created nodes and relationships
        qgn -> (if (qgn == construct.qualifiedGraphName) (-1).toByte else i.toByte)
      }.toMap
    }
  }

  private def initConstructTable[T <: Table[T] : TypeTag](
    inputTablePlan: RelationalOperator[T],
    unionPrefixStrategy: Map[QualifiedGraphName, GraphIdPrefix],
    construct: LogicalPatternGraph
  ): RelationalOperator[T] = {
    val LogicalPatternGraph(_, clonedVarsToInputVars, createdElements, sets, _, _) = construct

    // Apply aliases in CLONE to input table in order to create the base table, on which CONSTRUCT happens
    val aliasClones = clonedVarsToInputVars
      .filter { case (alias, original) => alias != original }
      .map(_.swap)

    val aliasOp: RelationalOperator[T] = if (aliasClones.isEmpty) {
      inputTablePlan
    } else {
      relational.Alias(inputTablePlan, aliasClones.map { case (expr, alias) => expr as alias }.toSeq)
    }

    val prefixedBaseTableOp = clonedVarsToInputVars.foldLeft(aliasOp) {
      case (op, (alias, original)) =>
        unionPrefixStrategy.get(original.cypherType.graph.get) match {
          case Some(prefix) => op.prefixVariableId(alias, prefix)
          case None => op
        }
    }

    // Construct CREATEd elements
    val constructedElementsOp = {
      if (createdElements.isEmpty) {
        prefixedBaseTableOp
      } else {
        val maybeCreatedElementPrefix = unionPrefixStrategy.get(construct.qualifiedGraphName)
        val elementsOp = planConstructElements(prefixedBaseTableOp, createdElements, maybeCreatedElementPrefix)

        val setValueForExprTuples = sets.flatMap {
          case SetPropertyItem(propertyKey, v, valueExpr) =>
            List(valueExpr -> ElementProperty(v, PropertyKey(propertyKey))(valueExpr.cypherType))
          case SetLabelItem(v, labels) =>
            labels.toList.map { label =>
              v.cypherType.material match {
                case _: CTNode => TrueLit -> expr.HasLabel(v, Label(label))
                case other => throw UnsupportedOperationException(s"Cannot set a label on $other")
              }
            }
        }
        elementsOp.addInto(setValueForExprTuples: _*)
      }
    }

    // Remove all vars that were part the original pattern graph DF, except variables that were CLONEd without an alias
    val allInputVars = aliasOp.header.vars
    val originalVarsToKeep = clonedVarsToInputVars.keySet -- aliasClones.keySet
    val varsToRemoveFromTable = allInputVars -- originalVarsToKeep

    constructedElementsOp.dropExprSet(varsToRemoveFromTable)
  }

  def planConstructElements[T <: Table[T] : TypeTag](
    inOp: RelationalOperator[T],
    toCreate: Set[ConstructedElement],
    maybeCreatedElementIdPrefix: Option[GraphIdPrefix]
  ): RelationalOperator[T] = {

    // Construct nodes before relationships, as relationships might depend on nodes
    val nodes: Set[ConstructedNode] = toCreate.collect {
      case c: ConstructedNode if !inOp.header.vars.contains(c.v) => c
    }
    val rels: Set[ConstructedRelationship] = toCreate.collect {
      case r: ConstructedRelationship if !inOp.header.vars.contains(r.v) => r
    }

    val (_, nodesToCreate) = nodes.foldLeft(0 -> Seq.empty[(Expr, Expr)]) {
      case ((nextColumnPartitionId, nodeProjections), nextNodeToConstruct) =>
        (nextColumnPartitionId + 1) -> (nodeProjections ++ computeNodeProjections(inOp, maybeCreatedElementIdPrefix, nextColumnPartitionId, nodes.size, nextNodeToConstruct))
    }

    val createdNodesOp = inOp.addInto(nodesToCreate.map(_.swap): _*)

    val (_, relsToCreate) = rels.foldLeft(0 -> Seq.empty[(Expr, Expr)]) {
      case ((nextColumnPartitionId, relProjections), nextRelToConstruct) =>
        (nextColumnPartitionId + 1) ->
          (relProjections ++ computeRelationshipProjections(createdNodesOp, maybeCreatedElementIdPrefix, nextColumnPartitionId, rels.size, nextRelToConstruct))
    }

    createdNodesOp.addInto(relsToCreate.map { case (into, value) => value -> into }: _*)
  }

  def computeNodeProjections[T <: Table[T]](
    inOp: RelationalOperator[T],
    maybeCreatedElementIdPrefix: Option[GraphIdPrefix],
    columnIdPartition: Int,
    numberOfColumnPartitions: Int,
    node: ConstructedNode
  ): Map[Expr, Expr] = {

    val idTuple = node.v ->
      prefixId(generateId(columnIdPartition, numberOfColumnPartitions), maybeCreatedElementIdPrefix)

    val copiedLabelTuples = node.baseElement match {
      case Some(origNode) => copyExpressions(inOp, node.v)(_.labelsFor(origNode))
      case None => Map.empty
    }

    val createdLabelTuples = node.labels.map {
      label => HasLabel(node.v, label) -> TrueLit
    }.toMap

    val propertyTuples = node.baseElement match {
      case Some(origNode) => copyExpressions(inOp, node.v)(_.propertiesFor(origNode))
      case None => Map.empty
    }

    createdLabelTuples ++
      copiedLabelTuples ++
      propertyTuples +
      idTuple
  }

  def computeRelationshipProjections[T <: Table[T]](
    inOp: RelationalOperator[T],
    maybeCreatedElementIdPrefix: Option[GraphIdPrefix],
    columnIdPartition: Int,
    numberOfColumnPartitions: Int,
    toConstruct: ConstructedRelationship
  ): Map[Expr, Expr] = {
    val ConstructedRelationship(rel, source, target, typOpt, baseRelOpt) = toConstruct

    // id needs to be generated
    val idTuple: (Var, Expr) = rel ->
      prefixId(generateId(columnIdPartition, numberOfColumnPartitions), maybeCreatedElementIdPrefix)

    // source and target are present: just copy
    val sourceTuple = {
      StartNode(rel)(CTInteger) -> source
    }
    val targetTuple = {
      EndNode(rel)(CTInteger) -> target
    }

    val typeTuple: Map[Expr, Expr] = {
      typOpt match {
        // type is set
        case Some(t) =>
          Map(HasType(rel, RelType(t)) -> TrueLit)
        case None =>
          // When no type is present, it needs to be a copy of a base relationship
          copyExpressions(inOp, rel)(_.typesFor(baseRelOpt.get))
      }
    }

    val propertyTuples: Map[Expr, Expr] = baseRelOpt match {
      case Some(baseRel) =>
        copyExpressions(inOp, rel)(_.propertiesFor(baseRel))
      case None => Map.empty
    }

    propertyTuples ++ typeTuple + idTuple + sourceTuple + targetTuple
  }

  def copyExpressions[E <: Expr, T <: Table[T]](inOp: RelationalOperator[T], targetVar: Var)
    (extractor: RecordHeader => Set[E]): Map[Expr, Expr] = {
      extractor(inOp.header)
        .map(o => o.withOwner(targetVar) -> o)
        .toMap
  }

  def prefixId(id: Expr, maybeCreatedElementIdPrefix: Option[GraphIdPrefix]): Expr = {
    maybeCreatedElementIdPrefix.map(PrefixId(id, _)).getOrElse(id)
  }

  // TODO: improve documentation and add specific tests
  def generateId(columnIdPartition: Int, numberOfColumnPartitions: Int): Expr = {
    val columnPartitionBits = math.log(numberOfColumnPartitions).floor.toInt + 1
    val totalIdSpaceBits = 33
    val columnIdShift = totalIdSpaceBits - columnPartitionBits

    // id needs to be generated
    // Limits the system to 500 mn partitions
    // The first half of the id space is protected
    val columnPartitionOffset = columnIdPartition.toLong << columnIdShift

    ToId(Add(MonotonicallyIncreasingId(), IntegerLit(columnPartitionOffset)))
  }

  /**
    * Given the construction table this method extracts all possible scans over that table
    * and compiles them to a ScanGraph. Note that cloned elements that are also present in an `ON GRAPH` are not
    * included in the ScanGraph to avoid duplication. This improves scan performance as fewer DISTINCTs are needed.
    */
  private def extractScanGraph[T <: Table[T] : TypeTag](
    logicalPatternGraph: LogicalPatternGraph,
    inputOp: RelationalOperator[T]
  )(implicit context: RelationalRuntimeContext[T]): RelationalCypherGraph[T] = {

    val schema = logicalPatternGraph.schema

    // extract label sets
    val setLabelItems = logicalPatternGraph.sets.collect {
      case SetLabelItem(variable, labels) => variable -> labels
    }.groupBy {
      case (variable, _) => variable
    }.map {
      case (variable, list) => variable -> list.flatMap(_._2).toSet
    }

    // Compute Scans from constructed and cloned elements
    val createdElementScanTypes = scanTypesFromCreatedElements(logicalPatternGraph, setLabelItems)
    val clonedElementScanTypes = scanTypesFromClonedElements(logicalPatternGraph, setLabelItems)

    val scansForCreated = createScans(createdElementScanTypes, inputOp, schema)
    val scansForCloned = createScans(clonedElementScanTypes, inputOp, schema, distinct = true)

    val scanGraphSchema: PropertyGraphSchema = computeScanGraphSchema(schema, createdElementScanTypes, clonedElementScanTypes)

    // Construct the scan graph
    context.session.graphs.create(Some(scanGraphSchema), scansForCreated ++ scansForCloned: _*)
  }

  /**
    * Computes all scan types that can be created from created elements.
    */
  private def scanTypesFromCreatedElements[T <: Table[T] : TypeTag](
    logicalPatternGraph: LogicalPatternGraph,
    setLabels: Map[Var, Set[String]]
  )(implicit context: RelationalRuntimeContext[T]): Set[(Var, CypherType)] = {

    logicalPatternGraph.newElements.flatMap {
      case c: ConstructedNode if c.baseElement.isEmpty =>
        val allLabels = c.labels.map(_.name) ++ setLabels.getOrElse(c.v, Set.empty)
        Seq(c.v -> CTNode(allLabels))

      case c: ConstructedNode =>
        c.baseElement.get.cypherType match {
          case CTNode(baseLabels, Some(sourceGraph)) =>
            val sourceSchema = context.resolveGraph(sourceGraph).schema
            val allLabels = c.labels.map(_.name) ++ baseLabels ++ setLabels.getOrElse(c.v, Set.empty)
            sourceSchema.forNode(allLabels).allCombinations.map(c.v -> CTNode(_)).toSeq
          case other => throw UnsupportedOperationException(s"Cannot construct node scan from $other")
        }

      case c: ConstructedRelationship if c.baseElement.isEmpty =>
        Seq(c.v -> c.v.cypherType)

      case c: ConstructedRelationship =>
        c.baseElement.get.cypherType match {
          case CTRelationship(baseTypes, Some(sourceGraph)) =>
            val sourceSchema = context.resolveGraph(sourceGraph).schema
            val possibleTypes = c.typ match {
              case Some(t) => Set(t)
              case _ => baseTypes
            }
            sourceSchema.forRelationship(CTRelationship(possibleTypes)).relationshipTypes.map(c.v -> CTRelationship(_)).toSeq
          case other => throw UnsupportedOperationException(s"Cannot construct relationship scan from $other")
        }
    }
  }

  /**
    * Computes all scan types that can be created from cloned elements
    */
  private def scanTypesFromClonedElements[T <: Table[T] : TypeTag](
    logicalPatternGraph: LogicalPatternGraph,
    setLabels: Map[Var, Set[String]]
  )(implicit context: RelationalRuntimeContext[T]): Set[(Var, CypherType)] = {

    val clonedElementsToKeep = logicalPatternGraph.clones.filterNot {
      case (_, base) => logicalPatternGraph.onGraphs.contains(base.cypherType.graph.get)
    }.mapValues(_.cypherType)

    clonedElementsToKeep.toSeq.flatMap {
      case (v, CTNode(labels, Some(sourceGraph))) =>
        val sourceSchema = context.resolveGraph(sourceGraph).schema
        val allLabels = labels ++ setLabels.getOrElse(v, Set.empty)
        sourceSchema.forNode(allLabels).allCombinations.map(v -> CTNode(_))

      case (v, r@CTRelationship(_, Some(sourceGraph))) =>
        val sourceSchema = context.resolveGraph(sourceGraph).schema
        sourceSchema.forRelationship(r.toCTRelationship).relationshipTypes.map(v -> CTRelationship(_)).toSeq

      case other => throw UnsupportedOperationException(s"Cannot construct scan from $other")
    }.toSet
  }

  /**
    * Creates the scans for the given scan types
    */
  private def createScans[T <: Table[T] : TypeTag](
    scanElements: Set[(Var, CypherType)],
    inputOp: RelationalOperator[T],
    schema: PropertyGraphSchema,
    distinct: Boolean = false
  )(implicit context: RelationalRuntimeContext[T]): Seq[ElementTable[T]] = {
    val groupedScanElements = scanElements.map {
      case (v, CTNode(labels, g)) =>
        v -> CTNode(labels, g)
      case other => other
    }.groupBy {
      case (_, cypherType) => cypherType
    }.map {
      case (ct, list) => ct -> list.map(_._1).toSeq
    }

    groupedScanElements.map { case (ct, vars) => scansForType(ct, vars, inputOp, schema, distinct) }.toSeq
  }

  def scansForType[T <: Table[T] : TypeTag](
    ct: CypherType,
    vars: Seq[Var],
    inputOp: RelationalOperator[T],
    schema: PropertyGraphSchema,
    distinct: Boolean = false
  ): ElementTable[T] = {

    val scans = vars.map { v =>
      val scanOp = scanForElementAndType(v, ct, inputOp, schema)
      val element = scanOp.singleElement

      // we need to get rid of the label and rel type columns
      val dropExprs = ct match {
        case _: CTRelationship => scanOp.header.typesFor(element)
        case _: CTNode => scanOp.header.labelsFor(element)
        case other => throw UnsupportedOperationException(s"Cannot create scan for $other")
      }

      scanOp.dropExpressions(dropExprs.toSeq: _*)
    }

    val combinedScans = scans.toList match {
      case head :: Nil => head

      case head :: tail =>
        val targetHeader = head.header
        val targetElement = head.singleElement
        tail.map { op =>
          op.alignWith(op.singleElement, targetElement, targetHeader)
        }.foldLeft(head)(_ unionAll _)

      case _ => throw IllegalArgumentException(
        expected = "Non-empty list of scans",
        actual = "Empty list",
        explanation = "This should never happen, possible planning bug.")
    }

    val distinctScans = if (distinct) relational.Distinct(combinedScans, combinedScans.header.vars) else combinedScans

    distinctScans.elementTable
  }

  private def scanForElementAndType[T <: Table[T] : TypeTag](
    extractionVar: Var,
    elementType: CypherType,
    op: RelationalOperator[T],
    schema: PropertyGraphSchema
  ): RelationalOperator[T] = {
    val targetElement = Var.unnamed(elementType)
    val targetElementHeader = schema.headerForElement(targetElement, exactLabelMatch = true)

    val labelOrTypePredicate = elementType match {
      case CTNode(labels, _) =>
        val labelFilters = op.header.labelsFor(extractionVar).map {
          case expr@HasLabel(_, Label(label)) if labels.contains(label) => Equals(expr, TrueLit)
          case expr: HasLabel => Equals(expr, FalseLit)
        }

        Ands(labelFilters)

      case CTRelationship(relTypes, _) =>
        val relTypeExprs: Set[Expr] = relTypes.map(relType => HasType(extractionVar, RelType(relType)))
        val physicalExprs = relTypeExprs intersect op.header.expressionsFor(extractionVar)
        Ors(physicalExprs.map(expr => Equals(expr, TrueLit)))

      case other => throw IllegalArgumentException("CTNode or CTRelationship", other)
    }

    val selected = op.select(extractionVar)
    val idExprs = op.header.idExpressions(extractionVar).toSeq

    val validElementPredicate = Ands(idExprs.map(idExpr => IsNotNull(idExpr)) :+ labelOrTypePredicate: _*)
    val filtered = relational.Filter(selected, validElementPredicate)

    val inputElement = filtered.singleElement
    val alignedScan = filtered.alignWith(inputElement, targetElement, targetElementHeader)

    alignedScan
  }

  private def computeScanGraphSchema[T <: Table[T]](
    baseSchema: PropertyGraphSchema,
    createdElementScanTypes: Set[(Var, CypherType)],
    clonedElementScanTypes: Set[(Var, CypherType)]
  ): PropertyGraphSchema = {
    val (nodeTypes, relTypes) = (createdElementScanTypes ++ clonedElementScanTypes).partition {
      case (_, _: CTNode) => true
      case _ => false
    }

    val scanGraphNodeLabelCombos = nodeTypes.collect {
      case (_, CTNode(labels, _)) => labels
    }

    val scanGraphRelTypes = relTypes.collect {
      case (_, CTRelationship(types, _)) => types
    }.flatten

    PropertyGraphSchema.empty
      .foldLeftOver(scanGraphNodeLabelCombos) {
        case (acc, labelCombo) => acc.withNodePropertyKeys(labelCombo, baseSchema.nodePropertyKeys(labelCombo))
      }
      .foldLeftOver(scanGraphRelTypes) {
        case (acc, typ) => acc.withRelationshipPropertyKeys(typ, baseSchema.relationshipPropertyKeys(typ))
      }
  }
}
