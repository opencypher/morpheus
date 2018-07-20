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
package org.opencypher.okapi.procedures

import java.util
import java.util.concurrent._
import java.util.stream.Stream

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.internal.kernel.api._
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import org.opencypher.okapi.api.types.CTNull
import org.opencypher.okapi.api.types.CypherType._
import org.opencypher.okapi.api.value.CypherValue

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class SchemaCalculator(db: GraphDatabaseService, api: GraphDatabaseAPI, tx: KernelTransaction, log: Log) {
  /**
    * Computes the schema of the Neo4j graph as used by Okapi
    *
    * @return
    */
  def constructOkapiSchemaInfo(): Stream[OkapiSchemaInfo] = {

    val nodeSchemaFutures = computerEntitySchema(Node)
    val relationshipSchemaFutures = computerEntitySchema(Relationship)

    val nodesSchema = nodeSchemaFutures
      .map(Await.ready(_, Duration.apply(20, TimeUnit.SECONDS)))
      .map(_.value.get.get)
      .foldLeft(new LabelPropertyKeyMap)(_ ++ _)

    val relationshipsSchema = relationshipSchemaFutures
      .map(Await.ready(_, Duration.apply(20, TimeUnit.SECONDS)))
      .map(_.value.get.get)
      .foldLeft(new LabelPropertyKeyMap)(_ ++ _)

    val nodeStream = getOkapiSchemaInfo(Node, nodesSchema)

    val relStream = getOkapiSchemaInfo(Relationship, relationshipsSchema)

    (nodeStream ++ relStream).asJavaCollection.stream()
  }

  /**
    * Computes the entity schema for the given entities by computing the schema for each individual entity and then
    * combining them. Uses batching to parallelize the computation
    */
  private def computerEntitySchema[T <: WrappedCursor](typ: EntityType): Seq[Future[LabelPropertyKeyMap]] = {
    val threads = Runtime.getRuntime.availableProcessors
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(threads))

    val maxId = getHighestIdInUseForStore(typ)
    val batchSize = 100000
    val batches = (maxId / batchSize.toFloat).ceil.toInt

    (1 to batches)
      .map { batch => Future {
        val upper = batch * batchSize - 1
        val lower = upper - batchSize
        val extractor = typ match  {
          case Node => NodeExtractor(db, api)
          case Relationship => RelExtractor(db, api)
        }
        extractor(lower, upper)
    }}
  }

  /**
    * Generates the OkapiSchemaInfo entries for a given label combination / relationship type
    */
  private def getOkapiSchemaInfo(
    typ: EntityType,
    map: LabelPropertyKeyMap
  ): Seq[OkapiSchemaInfo] = {
    map.data.flatMap {
      case (labelPointers, propertyMap) =>
        val labels = labelPointers.map(getLabelName(typ, _))

        if (propertyMap.isEmpty) {
          Seq(new OkapiSchemaInfo(typ.name, labels.toSeq.asJava, "", new util.ArrayList(0)))
        } else {
          propertyMap.map {
            case (propertyId, cypherTypes) =>
              new OkapiSchemaInfo(typ.name, labels.toSeq.asJava, getPropertyName(propertyId), cypherTypes.toSeq.asJava)
          }
        }
    }.toSeq
  }

  private def getLabelName(typ: EntityType, id: Int): String = typ match {
    case Node => tx.token().nodeLabelName(id)
    case Relationship => tx.token().relationshipTypeName(id)
  }

  private def getPropertyName(id: Int): String = tx.token().propertyKeyName(id)

  private def getHighestIdInUseForStore(typ: EntityType) = {
    val neoStores = api.getDependencyResolver.resolveDependency(classOf[RecordStorageEngine]).testAccessNeoStores
    val store = typ match {
      case Node => neoStores.getNodeStore
      case Relationship =>neoStores.getRelationshipStore
      case _ => throw new IllegalArgumentException("invalid type " + typ)
    }
    store.getHighId
  }
}

trait EntityType {
  def name: String
}

case object Node extends EntityType {
  override val name: String = "Node"
}

case object Relationship extends EntityType {
  override val name: String = "Relationship"
}

class LabelPropertyKeyMap {
  val data: mutable.Map[Set[Int], mutable.Map[Int, mutable.Set[String]]] =
    new mutable.HashMap().withDefault(_ => new mutable.HashMap().withDefault(_ => mutable.Set()))

  def add(labels: Set[Int], propertyMap: Map[Int, String]): LabelPropertyKeyMap = {
    val labelData = data.getOrElseUpdate(labels, data.default(labels))

    val nullProperties = labelData.keySet -- propertyMap.keySet
    nullProperties.foreach(labelData(_).add(CTNull.name))

    propertyMap.foreach {
      case (property, typ) => labelData.getOrElseUpdate(property, labelData.default(property)).add(typ)
    }

    this
  }

  def ++(other: LabelPropertyKeyMap): LabelPropertyKeyMap = {
    val overlap = data.keySet intersect other.data.keySet
    overlap.foreach { labels =>
      val lData = data(labels)
      val rData = other.data(labels)

      val nullProperties = lData.keySet ++ rData.keySet -- (lData.keySet intersect rData.keySet)
      nullProperties.foreach(lData(_).add(CTNull.name))

      rData.foreach {
        case (property, types) => lData.getOrElseUpdate(property, lData.default(property)) ++= types
      }
    }
    (other.data.keySet -- overlap).foreach(x => data.update(x, other.data(x)))
    this
  }
}

trait WrappedCursor {
  def getNodeCursor: Option[NodeCursor]
  def getRelCursor: Option[RelationshipScanCursor]

  def close(): Unit
}

case class WrappedNodeCursor(cursor: NodeCursor) extends WrappedCursor {
  override def getNodeCursor: Option[NodeCursor] = Some(cursor)
  override def getRelCursor: Option[RelationshipScanCursor] = None
  override def close(): Unit = cursor.close()
}

case class WrappedRelationshipCursor(cursor: RelationshipScanCursor) extends WrappedCursor {
  override def getNodeCursor: Option[NodeCursor] = None
  override def getRelCursor: Option[RelationshipScanCursor] = Some(cursor)
  override def close(): Unit = cursor.close()
}

trait Extractor[T <: WrappedCursor] {
  def labelPropertyMap: LabelPropertyKeyMap
  def reuseMap: util.HashMap[Int, String]

  def db: GraphDatabaseService
  def api: GraphDatabaseAPI

  def apply(lower: Long, upper: Long): LabelPropertyKeyMap = withTransaction {
    val ctx = api.getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge])
    val kTx = ctx.getKernelTransactionBoundToThisThread(true)
    val cursors = kTx.cursors
    val read = kTx.dataRead

    val wrappedCursor = getCursor(cursors)
    val propertyCursor = cursors.allocatePropertyCursor()

    (lower to upper).foreach(iterate(_, wrappedCursor, propertyCursor, read))

    propertyCursor.close()
    wrappedCursor.close()

    labelPropertyMap
  }

  def getCursor(cursors: CursorFactory): T

  def iterate(i: Long, wrappedCursor: T, propertyCursor: PropertyCursor, read: Read): Unit

  /**
    * Runs the given function wrapped in a Neo4j transaction and returns the result
    *
    * @param function code that will be run inside the transaction
    * @tparam A return type of the function
    * @return
    */
  private def withTransaction[A](function: => A): A = {
    val tx = db.beginTx()
    val res = function
    tx.success()
    res
  }

  protected def extractPropertyTypes(propertyCursor: PropertyCursor): mutable.Map[Int, String] = {
    reuseMap.clear()
    while(propertyCursor.next()) {
      val value = propertyCursor.propertyValue().asObject()

      val typ = CypherValue.get(value).map(_.cypherType) match {
        case Some(cypherType) => cypherType.name
        case None => value.getClass.getSimpleName
      }

      reuseMap.put(propertyCursor.propertyKey(), typ)
      //map.put(propertyCursor.propertyKey(), propertyCursor.propertyType().name())
    }
    reuseMap.asScala
  }
}

case class NodeExtractor (
  override val db: GraphDatabaseService,
  override val api: GraphDatabaseAPI
) extends Extractor[WrappedNodeCursor] {

  override val labelPropertyMap: LabelPropertyKeyMap = new LabelPropertyKeyMap
  override val reuseMap: util.HashMap[Int, String] = new util.HashMap[Int, String]()

  override def iterate(i: Long, wrappedCursor: WrappedNodeCursor, propertyCursor: PropertyCursor, read: Read): Unit = {
    val nodeCursor = wrappedCursor.cursor
    read.singleNode(i, nodeCursor)
    if(nodeCursor.next()) {
      nodeCursor.properties(propertyCursor)
      val labels = nodeCursor.labels().all()
      val propertyTypes = extractPropertyTypes(propertyCursor)
      labelPropertyMap.add(labels.map(_.toInt).toSet, propertyTypes.toMap)
    }
  }

  override def getCursor(cursors: CursorFactory): WrappedNodeCursor = WrappedNodeCursor(cursors.allocateNodeCursor)
}

case class RelExtractor(
  override val db: GraphDatabaseService,
  override val api: GraphDatabaseAPI
) extends Extractor[WrappedRelationshipCursor] {

  override val labelPropertyMap: LabelPropertyKeyMap = new LabelPropertyKeyMap
  override val reuseMap: util.HashMap[Int, String] = new util.HashMap[Int, String]()

  override def iterate(i: Long, wrappedCursor: WrappedRelationshipCursor, propertyCursor: PropertyCursor, read: Read): Unit = {
    val relCursor = wrappedCursor.cursor

    read.singleRelationship(i, relCursor)
    if(relCursor.next()) {
      relCursor.properties(propertyCursor)
      val relType = relCursor.`type`()
      val propertyTypes = extractPropertyTypes(propertyCursor)
      labelPropertyMap.add(Set(relType), propertyTypes.toMap)
    }
  }

  override def getCursor(cursors: CursorFactory): WrappedRelationshipCursor =
    WrappedRelationshipCursor(cursors.allocateRelationshipScanCursor())
}
