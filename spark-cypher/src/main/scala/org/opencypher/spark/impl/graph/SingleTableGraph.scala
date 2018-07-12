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
package org.opencypher.spark.impl.graph

import org.opencypher.okapi.api.graph.PropertyGraph
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.api.graph.{RelationalCypherGraph, RelationalCypherSession}
import org.opencypher.okapi.relational.api.physical.RelationalRuntimeContext
import org.opencypher.okapi.relational.api.schema.RelationalSchema._
import org.opencypher.okapi.relational.api.table.{FlatRelationalTable, RelationalCypherRecords}
import org.opencypher.okapi.relational.impl.operators.{ExtractEntities, Start}

/**
  * A single table graph represents the result of CONSTRUCT clause. It contains all entities from the outer scope that 
  * the clause constructs. The initial schema of that graph is the union of all graph schemata the CONSTRUCT clause refers
  * to, including their corresponding graph tags. Note, that the initial schema does not include the graph tag used for
  * the constructed entities.
  */
case class SingleTableGraph[T <: FlatRelationalTable[T]](
  baseTable: RelationalCypherRecords[T],
  override val schema: Schema,
  override val tags: Set[Int]
)(implicit val session: RelationalCypherSession[T], context: RelationalRuntimeContext[T])
  extends RelationalCypherGraph[T] {

  override type Graph = SingleTableGraph[T]

  private val header = baseTable.header

  def show(): Unit = baseTable.show

  override def cache(): SingleTableGraph[T] = map(_.cache())

  private def map(f: RelationalCypherRecords[T] => RelationalCypherRecords[T]) =
    SingleTableGraph(f(baseTable), schema, tags)

  override def nodes(name: String, nodeCypherType: CTNode): RelationalCypherRecords[T] = {
    val targetNode = Var(name)(nodeCypherType)
    val nodeSchema = schema.forNode(nodeCypherType.labels)
    val targetNodeHeader = nodeSchema.headerForNode(targetNode)
    val extractionNodes: Set[Var] = header.nodesForType(nodeCypherType)
    val extractionOp = ExtractEntities(Start(baseTable), targetNodeHeader, extractionNodes)

    session.records.from(extractionOp.header, extractionOp.table)
  }

  override def relationships(name: String, relCypherType: CTRelationship): RelationalCypherRecords[T] = {
    val targetRel = Var(name)(relCypherType)
    val relSchema = schema.forRelationship(relCypherType)
    val targetRelHeader = relSchema.headerForRelationship(targetRel)
    val extractionRels: Set[Var] = header.relationshipsForType(relCypherType)
    val extractionOp = ExtractEntities(Start(baseTable), targetRelHeader, extractionRels)

    session.records.from(extractionOp.header, extractionOp.table)
  }

  // TODO: move UnionGraph to relational
  override def unionAll(others: PropertyGraph*): PropertyGraph = ???

  //  def nodesWithExactLabels(name: String, labels: Set[String]): RelationalCypherRecords[T] = {
  //    val nodeType = CTNode(labels)
  //    val nodeVar = Var(name)(nodeType)
  //    val records = nodes(name, nodeType)
  //
  //    val header = records.header
  //
  //    // compute slot contents to keep
  //    val labelExprs = header.labelsFor(nodeVar)
  //
  //    // need to iterate the slots to maintain the correct order
  //    val propertyExprs = schema.nodeKeys(labels).flatMap {
  //      case (key, cypherType) => Property(nodeVar, PropertyKey(key))(cypherType)
  //    }.toSet
  //    val headerPropertyExprs = header.propertiesFor(nodeVar).filter(propertyExprs.contains)
  //
  //    val keepExprs: Seq[Expr] = Seq(nodeVar) ++ labelExprs ++ headerPropertyExprs
  //
  //    val keepColumns = keepExprs.map(header.column)
  //
  //    // we only keep rows where all "other" labels are false
  //    val predicate = labelExprs
  //      .filterNot(l => labels.contains(l.label.name))
  //
  //      .map(header.column)
  //      .map(records.df.col(_) === false)
  //      .reduceOption(_ && _)
  //
  //    // filter rows and select only necessary columns
  //    val updatedData = predicate match {
  //
  //      case Some(filter) =>
  //        records.df
  //          .filter(filter)
  //          .select(keepColumns.head, keepColumns.tail: _*)
  //
  //      case None =>
  //        records.df.select(keepColumns.head, keepColumns.tail: _*)
  //    }
  //
  //    val updatedHeader = RecordHeader.from(keepExprs)
  //
  //    CAPSRecords(updatedHeader, updatedData)
  //  }
}
