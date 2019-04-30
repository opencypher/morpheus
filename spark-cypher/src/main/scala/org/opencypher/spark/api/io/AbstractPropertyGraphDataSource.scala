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
package org.opencypher.spark.api.io

import java.util.concurrent.Executors

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.schema.PropertyGraphSchema
import org.opencypher.okapi.api.types.{CTIdentity, CypherType}
import org.opencypher.okapi.impl.exception.GraphNotFoundException
import org.opencypher.okapi.impl.util.StringEncodingUtilities._
import org.opencypher.spark.api.MorpheusSession
import org.opencypher.spark.api.io.metadata.MorpheusGraphMetaData
import org.opencypher.spark.api.io.util.MorpheusGraphExport._
import org.opencypher.spark.impl.MorpheusConverters._
import org.opencypher.spark.impl.io.MorpheusPropertyGraphDataSource
import org.opencypher.spark.schema.MorpheusSchema
import org.opencypher.spark.schema.MorpheusSchema._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.{Failure, Success}

object AbstractPropertyGraphDataSource {

  def nodeColsWithCypherType(schema: PropertyGraphSchema, labelCombination: Set[String]): Map[String, CypherType] = {
    val propertyColsWithCypherType = schema.nodePropertyKeysForCombinations(Set(labelCombination)).map {
      case (key, cypherType) => key.toPropertyColumnName -> cypherType
    }
    propertyColsWithCypherType + (GraphElement.sourceIdKey -> CTIdentity)
  }

  def relColsWithCypherType(schema: PropertyGraphSchema, relType: String): Map[String, CypherType] = {
    val propertyColsWithCypherType = schema.relationshipPropertyKeys(relType).map {
      case (key, cypherType) => key.toPropertyColumnName -> cypherType
    }
    propertyColsWithCypherType ++ Relationship.nonPropertyAttributes.map(_ -> CTIdentity)
  }
}

/**
  * Abstract data source implementation that takes care of caching graph names and schemas.
  *
  * It automatically creates initializes a ScanGraphs an only requires the implementor to provider simpler methods for
  * reading/writing files and tables.
  */
abstract class AbstractPropertyGraphDataSource extends MorpheusPropertyGraphDataSource {

  implicit val morpheus: MorpheusSession

  def tableStorageFormat: StorageFormat

  protected var schemaCache: Map[GraphName, MorpheusSchema] = Map.empty

  protected var graphNameCache: Set[GraphName] = listGraphNames.map(GraphName).toSet

  protected def listGraphNames: List[String]

  protected def deleteGraph(graphName: GraphName): Unit

  protected[io] def readSchema(graphName: GraphName): MorpheusSchema

  protected def writeSchema(graphName: GraphName, schema: MorpheusSchema): Unit

  protected def readMorpheusGraphMetaData(graphName: GraphName): MorpheusGraphMetaData

  protected def writeMorpheusGraphMetaData(graphName: GraphName, morpheusGraphMetaData: MorpheusGraphMetaData): Unit

  protected def readNodeTable(graphName: GraphName, labelCombination: Set[String], sparkSchema: StructType): DataFrame

  protected def writeNodeTable(graphName: GraphName, labelCombination: Set[String], table: DataFrame): Unit

  protected def readRelationshipTable(graphName: GraphName, relKey: String, sparkSchema: StructType): DataFrame

  protected def writeRelationshipTable(graphName: GraphName, relKey: String, table: DataFrame): Unit

  override def graphNames: Set[GraphName] = graphNameCache

  override def hasGraph(graphName: GraphName): Boolean = graphNameCache.contains(graphName)

  override def delete(graphName: GraphName): Unit = {
    deleteGraph(graphName)
    schemaCache -= graphName
    graphNameCache -= graphName
  }

  override def graph(name: GraphName): PropertyGraph = {
    if (!hasGraph(name)) {
      throw GraphNotFoundException(s"Graph with name '$name'")
    } else {
      val morpheusSchema: MorpheusSchema = schema(name).get
      val nodeTables = morpheusSchema.allCombinations.map { combo =>
        val df = readNodeTable(name, combo, morpheusSchema.canonicalNodeStructType(combo))
        MorpheusNodeTable(combo, df)
      }
      val relTables = morpheusSchema.relationshipTypes.map { relType =>
        val df = readRelationshipTable(name, relType, morpheusSchema.canonicalRelStructType(relType))
        MorpheusRelationshipTable(relType, df)
      }
      if (nodeTables.isEmpty) {
        morpheus.graphs.empty
      } else {
        morpheus.graphs.create(Some(morpheusSchema), (nodeTables ++ relTables).toSeq: _*)
      }
    }
  }

  override def schema(graphName: GraphName): Option[MorpheusSchema] = {
    if (schemaCache.contains(graphName)) {
      schemaCache.get(graphName)
    } else {
      val s = readSchema(graphName)
      schemaCache += graphName -> s
      Some(s)
    }
  }


  override def store(graphName: GraphName, graph: PropertyGraph): Unit = {
    checkStorable(graphName)

    val poolSize = morpheus.sparkSession.sparkContext.statusTracker.getExecutorInfos.length

    implicit val executionContext: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(poolSize))

    try {
      val relationalGraph = graph.asMorpheus

      val schema = relationalGraph.schema.asMorpheus
      schemaCache += graphName -> schema
      graphNameCache += graphName
      writeMorpheusGraphMetaData(graphName, MorpheusGraphMetaData(tableStorageFormat.name))
      writeSchema(graphName, schema)

      val nodeWrites = schema.labelCombinations.combos.map { combo =>
        Future {
          writeNodeTable(graphName, combo, relationalGraph.canonicalNodeTable(combo))
        }
      }

      val relWrites = schema.relationshipTypes.map { relType =>
        Future {
          writeRelationshipTable(graphName, relType, relationalGraph.canonicalRelationshipTable(relType))
        }
      }

      waitForWriteCompletion(nodeWrites)
      waitForWriteCompletion(relWrites)
    } finally {
      executionContext.shutdown()
    }
  }

  override def reset(): Unit = {
    schemaCache = Map.empty
    graphNameCache = listGraphNames.map(GraphName).toSet
  }

  protected def waitForWriteCompletion(writeFutures: Set[Future[Unit]])(implicit ec: ExecutionContext): Unit = {
    writeFutures.foreach { writeFuture =>
      Await.ready(writeFuture, Duration.Inf)
      writeFuture.onComplete {
        case Success(_) =>
        case Failure(e) => throw e
      }
    }
  }
}
