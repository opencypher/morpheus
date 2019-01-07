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
package org.opencypher.okapi.procedures

import java.util
import java.util.concurrent._
import java.util.stream.Stream

import org.neo4j.internal.kernel.api._
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import org.neo4j.values.storable.ValueGroup
import org.opencypher.okapi.api.graph.{GraphEntityType, Node, Relationship}
import org.opencypher.okapi.api.types.CypherType._
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.procedures.LabelPropertyKeyMap._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Success, Try}

class SchemaCalculator(api: GraphDatabaseAPI, tx: KernelTransaction, log: Log) {
  val ctx: ThreadToStatementContextBridge =  api.getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge])

  /**
    * Computes the schema of the Neo4j graph as used by Okapi
    */
  def constructOkapiSchemaInfo(): Stream[OkapiSchemaInfo] = {

    val nodeSchemaFutures = computeEntitySchema(Node)
    val relationshipSchemaFutures = computeEntitySchema(Relationship)

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

    Stream.concat(nodeStream, relStream)
  }

  /**
    * Computes the entity schema for the given entities by computing the schema for each individual entity and then
    * combining them. Uses batching to parallel the computation
    */
  private def computeEntitySchema[T <: WrappedCursor](typ: GraphEntityType): Seq[Future[LabelPropertyKeyMap]] = {
    val maxId = getHighestIdInUseForStore(typ)
    val batchSize = 100000
    val batches = (maxId / batchSize.toFloat).ceil.toInt

    (1 to batches)
      .map { batch =>
        Future {
          val upper = batch * batchSize - 1
          val lower = upper - batchSize
          val extractor = typ match {
            case Node => NodeExtractor(api, ctx)
            case Relationship => RelExtractor(api, ctx)
          }
          extractor(lower, upper)
        }
      }
  }

  /**
    * Generates the OkapiSchemaInfo entries for a given label combination / relationship type
    */
  private def getOkapiSchemaInfo(
    typ: GraphEntityType,
    map: LabelPropertyKeyMap
  ): Stream[OkapiSchemaInfo] = map.data.asScala.flatMap {
    case (labelPointers, propertyMap) =>
      val labels = labelPointers.map(l => getLabelName(typ, l.toInt))

      if (propertyMap.isEmpty) {
        Seq(new OkapiSchemaInfo(typ.name, labels.toSeq.asJava, "", new util.ArrayList(0)))
      } else {
        propertyMap.asScala.map {
          case (propertyId, cypherTypes) =>
            new OkapiSchemaInfo(
              typ.name,
              labels.toSeq.asJava,
              getPropertyName(propertyId),
              cypherTypes.toList.asJava
            )
        }
      }
  }.asJavaCollection.stream()

  /**
    * Translates integers representing labels into the correct label name
    */
  private def getLabelName(typ: GraphEntityType, id: Int): String = typ match {
    case Node => tx.token().nodeLabelName(id)
    case Relationship => tx.token().relationshipTypeName(id)
  }

  /**
    * Translates integers representing property names into the correct property name string
    */
  private def getPropertyName(id  : Int): String = tx.token().propertyKeyName(id)

  private def getHighestIdInUseForStore(typ: GraphEntityType) = {
    val neoStores = api.getDependencyResolver.resolveDependency(classOf[RecordStorageEngine]).testAccessNeoStores
    val store = typ match {
      case Node => neoStores.getNodeStore
      case Relationship =>neoStores.getRelationshipStore
      case _ => throw new IllegalArgumentException("invalid type " + typ)
    }
    store.getHighId
  }
}

object LabelPropertyKeyMap {
  val ctNullString: String = CTNull.name
}

/**
  * Stores the gatheredd information about label, property, type combinations.
  * @note This implementation is mutable and does inplace updates
  */
class LabelPropertyKeyMap {
  val data: java.util.Map[Set[Long], java.util.Map[Int, Array[String]]] = new java.util.HashMap()

  /**
    * Given a label combination and a property cursor, this computes the new schema that results from including this
    * new information
    */
  final def add(labels: Set[Long], propertyCursor: PropertyCursor): LabelPropertyKeyMap = {
    var labelData = data.get(labels)
    val isExistingLabel = labelData != null
    if(!isExistingLabel) {
      labelData = new java.util.HashMap()
      data.put(labels, labelData )
    }

    val remainingProperties = new util.HashSet(labelData.keySet())

    while(propertyCursor.next()) {
      val property = propertyCursor.propertyKey()
      remainingProperties.remove(property)

      val typ = getCypherType(propertyCursor)

      var knownTypes = labelData.get(property)
      val existingTypes = knownTypes != null
      if (knownTypes == null) {
        knownTypes = Array()
        labelData.put(property, knownTypes)
      }

      // we have seen this label combination before but not the property, so make the property nullable
      if(isExistingLabel && !existingTypes && !knownTypes.contains(ctNullString)) {
        knownTypes = knownTypes :+  ctNullString
        labelData.put(property, knownTypes)
      }

      if(!knownTypes.contains(typ)){
        knownTypes = knownTypes :+ typ
        labelData.put(property, knownTypes)
      }
    }

    // if remaining properties is not empty, then we have not seen these properties for the current element,
    // thus we have to make it nullable.
    remainingProperties.foreach { property =>
      val knownTypes = labelData.get(property)
      if(!knownTypes.contains(ctNullString)) {
        labelData.put(property, knownTypes :+ ctNullString)
      }
    }

    this
  }

  def getCypherType(cursor: PropertyCursor): String = {
    Try(cursor.propertyType()) match {
      case Success(typ) => typ match {
        case ValueGroup.UNKNOWN => CTVoid.name
        case ValueGroup.TEXT_ARRAY => CTList(CTString.nullable).name
        case ValueGroup.BOOLEAN_ARRAY => CTList(CTBoolean.nullable).name
        case ValueGroup.TEXT => CTString.name
        case ValueGroup.BOOLEAN => CTBoolean.name
        case ValueGroup.NO_VALUE => ctNullString
        case ValueGroup.NUMBER | ValueGroup.NUMBER_ARRAY =>
          CypherValue.get(cursor.propertyValue().asObject()).map(_.cypherType.name).get
        case other =>
          other.toString
      }

      case _ =>
        CypherValue.get(cursor.propertyValue().asObject()).map(_.cypherType.name).getOrElse(
          cursor.propertyValue().asObject().getClass.getSimpleName
        )
    }
  }

  def ++(other: LabelPropertyKeyMap): LabelPropertyKeyMap = {
    other.data.keySet.foreach { labels =>
      if(data.containsKey(labels)) {
        val lData = data.get(labels)
        val rData = other.data.get(labels)

        val remainingProperties = new util.HashSet(lData.keySet())

        for(property <- rData.keySet()) {
          remainingProperties.remove(property)

          val existingTypes = lData.putIfAbsent(property, Array())
          var knownTypes = lData.get(property)

          // we have seen this label combination before but not the property, so make the property nullable
          if(existingTypes == null && !knownTypes.contains(ctNullString)) {
            knownTypes = knownTypes :+ ctNullString
            lData.put(property, knownTypes)
          }

          val toAdd = rData.get(property).filterNot(knownTypes.contains)
          lData.put(property, knownTypes ++ toAdd)
        }

        remainingProperties.foreach { property =>
          val knownTypes = lData.get(property)
          if(!knownTypes.contains(ctNullString)) {
            lData.put(property, knownTypes :+ ctNullString)
          }
        }

      } else {
        data.put(labels, other.data.get(labels))
      }
    }
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

  def api: GraphDatabaseAPI
  def ctx: ThreadToStatementContextBridge

  def apply(lower: Long, upper: Long): LabelPropertyKeyMap = withTransaction {
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
    val tx = api.beginTx()
    val res = function
    tx.success()
    res
  }
}

case class NodeExtractor (
  override val api: GraphDatabaseAPI,
  override val ctx: ThreadToStatementContextBridge
) extends Extractor[WrappedNodeCursor] {

  override val labelPropertyMap: LabelPropertyKeyMap = new LabelPropertyKeyMap

  final override def iterate(i: Long, wrappedCursor: WrappedNodeCursor, propertyCursor: PropertyCursor, read: Read): Unit = {
    val nodeCursor = wrappedCursor.cursor
    read.singleNode(i, nodeCursor)
    if(nodeCursor.next()) {
      nodeCursor.properties(propertyCursor)
      val labels = nodeCursor.labels().all()
      labelPropertyMap.add(labels.toSet, propertyCursor)
    }
  }

  override def getCursor(cursors: CursorFactory): WrappedNodeCursor = WrappedNodeCursor(cursors.allocateNodeCursor)
}

case class RelExtractor(
  override val api: GraphDatabaseAPI,
  override val ctx: ThreadToStatementContextBridge
) extends Extractor[WrappedRelationshipCursor] {

  override val labelPropertyMap: LabelPropertyKeyMap = new LabelPropertyKeyMap

  final override def iterate(i: Long, wrappedCursor: WrappedRelationshipCursor, propertyCursor: PropertyCursor, read: Read): Unit = {
    val relCursor = wrappedCursor.cursor

    read.singleRelationship(i, relCursor)
    if(relCursor.next()) {
      relCursor.properties(propertyCursor)
      val relType = relCursor.`type`()
      labelPropertyMap.add(Set(relType), propertyCursor)
    }
  }

  override def getCursor(cursors: CursorFactory): WrappedRelationshipCursor =
    WrappedRelationshipCursor(cursors.allocateRelationshipScanCursor())
}