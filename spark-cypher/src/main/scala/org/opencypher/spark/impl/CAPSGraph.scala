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
package org.opencypher.spark.impl

import org.apache.spark.storage.StorageLevel
import org.opencypher.okapi.api.graph.{GraphOperations, PropertyGraph}
import org.opencypher.okapi.api.schema._
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.PropertyKey
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.impl.table.{ColumnName, OpaqueField, RecordHeader}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.{CAPSEntityTable, CAPSNodeTable}
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.schema.CAPSSchema
import org.opencypher.spark.schema.CAPSSchema._

trait CAPSGraph extends PropertyGraph with GraphOperations with Serializable {

  def tags: Set[Int]

  implicit def session: CAPSSession

  override def nodes(name: String, nodeCypherType: CTNode = CTNode): CAPSRecords

  override def relationships(name: String, relCypherType: CTRelationship = CTRelationship): CAPSRecords

  override def unionAll(others: PropertyGraph*): CAPSGraph = {
    CAPSUnionGraph(this :: others.map(_.asCaps).toList: _*)
  }

  override def schema: CAPSSchema

  def cache(): CAPSGraph

  def persist(): CAPSGraph

  def persist(storageLevel: StorageLevel): CAPSGraph

  def unpersist(): CAPSGraph

  def unpersist(blocking: Boolean): CAPSGraph

  def nodesWithExactLabels(name: String, labels: Set[String]): CAPSRecords = {
    val nodeType = CTNode(labels)
    val nodeVar = Var(name)(nodeType)
    val records = nodes(name, nodeType)

    // compute slot contents to keep
    val idSlot = records.header.slotFor(nodeVar)

    val labelSlots = records.header.labelSlots(nodeVar)
      .filter(slot => labels.contains(slot._1.label.name))
      .values

    // need to iterate the slots to maintain the correct order
    val propertyExprs = schema.nodeKeys(labels).flatMap {
      case (key, cypherType) => Property(nodeVar, PropertyKey(key))(cypherType)
    }.toSet
    val propertySlots = records.header.slotsFor(nodeVar).filter(sc => propertyExprs.contains(sc.content.key))

    val keepSlots = (Seq(idSlot) ++ labelSlots ++ propertySlots).map(_.content)
    val keepCols = keepSlots.map(ColumnName.of)

    // we only keep rows where all "other" labels are false
    val predicate = records.header.labelSlots(nodeVar)
      .filterNot(slot => labels.contains(slot._1.label.name))
      .values
      .map(ColumnName.of)
      .map(records.data.col(_) === false)
      .reduceOption(_ && _)

    // filter rows and select only necessary columns
    val updatedData = predicate match {

      case Some(filter) =>
        records.data
          .filter(filter)
          .select(keepCols.head, keepCols.tail: _*)

      case None =>
        records.data.select(keepCols.head, keepCols.tail: _*)
    }

    val updatedHeader = RecordHeader.from(keepSlots: _*)

    CAPSRecords.verifyAndCreate(updatedHeader, updatedData)(session)
  }

}

object CAPSGraph {

  def empty(implicit caps: CAPSSession): CAPSGraph =
    new EmptyGraph() {

      override def session: CAPSSession = caps

      override def cache(): CAPSGraph = this

      override def persist(): CAPSGraph = this

      override def persist(storageLevel: StorageLevel): CAPSGraph = this

      override def unpersist(): CAPSGraph = this

      override def unpersist(blocking: Boolean): CAPSGraph = this

      override def tags: Set[Int] = Set.empty
    }

  def create(nodeTable: CAPSNodeTable, entityTables: CAPSEntityTable*)(implicit caps: CAPSSession): CAPSGraph = {
    val allTables = nodeTable +: entityTables
    val schema = allTables.map(_.schema).reduce[Schema](_ ++ _).asCaps
    new CAPSScanGraph(allTables, schema, Set(0))
  }

  def create(records: CypherRecords, schema: CAPSSchema, tags: Set[Int] = Set(0))(implicit caps: CAPSSession): CAPSGraph = {
    val capsRecords = records.asCaps
    new CAPSPatternGraph(capsRecords, schema, tags)
  }

  def createLazy(theSchema: CAPSSchema, loadGraph: => CAPSGraph)(implicit caps: CAPSSession): CAPSGraph =
    new LazyGraph(theSchema, loadGraph) {}

  sealed abstract class LazyGraph(override val schema: CAPSSchema, loadGraph: => CAPSGraph)(implicit caps: CAPSSession)
    extends CAPSGraph {
    protected lazy val lazyGraph: CAPSGraph = {
      val g = loadGraph
      if (g.schema == schema) g else throw IllegalArgumentException(s"a graph with schema $schema", g.schema)
    }

    override def tags: Set[Int] = lazyGraph.tags

    override def session: CAPSSession = caps

    override def nodes(name: String, nodeCypherType: CTNode): CAPSRecords =
      lazyGraph.nodes(name, nodeCypherType)

    override def relationships(name: String, relCypherType: CTRelationship): CAPSRecords =
      lazyGraph.relationships(name, relCypherType)

    override def cache(): CAPSGraph = {
      lazyGraph.cache()
      this
    }

    override def persist(): CAPSGraph = {
      lazyGraph.persist()
      this
    }

    override def persist(storageLevel: StorageLevel): CAPSGraph = {
      lazyGraph.persist(storageLevel)
      this
    }

    override def unpersist(): CAPSGraph = {
      lazyGraph.unpersist()
      this
    }

    override def unpersist(blocking: Boolean): CAPSGraph = {
      lazyGraph.unpersist(blocking)
      this
    }
  }

  sealed abstract class EmptyGraph(implicit val caps: CAPSSession) extends CAPSGraph {

    override val schema: CAPSSchema = CAPSSchema.empty

    override def nodes(name: String, cypherType: CTNode): CAPSRecords =
      CAPSRecords.empty(RecordHeader.from(OpaqueField(Var(name)(cypherType))))

    override def relationships(name: String, cypherType: CTRelationship): CAPSRecords =
      CAPSRecords.empty(RecordHeader.from(OpaqueField(Var(name)(cypherType))))
  }

}
