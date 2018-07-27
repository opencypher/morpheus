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
package org.opencypher.spark.api.io

import java.util.concurrent.Executors

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.types.CTInteger
import org.opencypher.okapi.impl.exception.GraphNotFoundException
import org.opencypher.okapi.impl.util.StringEncodingUtilities._
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.metadata.CAPSGraphMetaData
import org.opencypher.spark.api.io.util.CAPSGraphExport._
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.impl.CAPSGraph
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.impl.io.CAPSPropertyGraphDataSource
import org.opencypher.spark.schema.CAPSSchema

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.{Failure, Success}

/**
  * Abstract data source implementation that takes care of caching graph names and schemas.
  *
  * It automatically creates initializes a ScanGraphs an only requires the implementor to provider simpler methods for
  * reading/writing files and tables.
  */
abstract class AbstractPropertyGraphDataSource(implicit val session: CAPSSession) extends CAPSPropertyGraphDataSource {

  def tableStorageFormat: String

  protected var schemaCache: Map[GraphName, CAPSSchema] = Map.empty

  protected var graphNameCache: Set[GraphName] = listGraphNames.map(GraphName).toSet

  protected def listGraphNames: List[String]

  protected def deleteGraph(graphName: GraphName): Unit

  protected def readSchema(graphName: GraphName): CAPSSchema

  protected def writeSchema(graphName: GraphName, schema: CAPSSchema): Unit

  protected def readCAPSGraphMetaData(graphName: GraphName): CAPSGraphMetaData

  protected def writeCAPSGraphMetaData(graphName: GraphName, capsGraphMetaData: CAPSGraphMetaData): Unit

  protected def readNodeTable(graphName: GraphName, labels: Set[String], sparkSchema: StructType): DataFrame

  protected def writeNodeTable(graphName: GraphName, labels: Set[String], table: DataFrame): Unit

  protected def readRelationshipTable(graphName: GraphName, relKey: String, sparkSchema: StructType): DataFrame

  protected def writeRelationshipTable(graphName: GraphName, relKey: String, table: DataFrame): Unit

  override def graphNames: Set[GraphName] = graphNameCache

  override def hasGraph(graphName: GraphName): Boolean = graphNameCache.contains(graphName)

  override def delete(graphName: GraphName): Unit = {
    schemaCache -= graphName
    graphNameCache -= graphName
    deleteGraph(graphName)
  }

  override def graph(graphName: GraphName): PropertyGraph = {
    if (!hasGraph(graphName)) {
      throw GraphNotFoundException(s"Graph with name '$graphName'")
    } else {
      val capsSchema: CAPSSchema = schema(graphName).get
      val capsMetaData: CAPSGraphMetaData = readCAPSGraphMetaData(graphName)
      val nodeTables = capsSchema.allLabelCombinations.map { combo =>
        val propertyColsWithCypherType = capsSchema.keysFor(Set(combo)).map {
          case (key, cypherType) => key.toPropertyColumnName -> cypherType
        }

        val columnsWithCypherType = propertyColsWithCypherType + (GraphEntity.sourceIdKey -> CTInteger)
        val df = readNodeTable(graphName, combo, capsSchema.canonicalNodeStructType(combo))
        CAPSNodeTable(combo, df.setNullability(columnsWithCypherType))
      }

      val relTables = capsSchema.relationshipTypes.map { relType =>
        val propertyColsWithCypherType = capsSchema.relationshipKeys(relType).map {
          case (key, cypherType) => key.toPropertyColumnName -> cypherType
        }

        val columnsWithCypherType = propertyColsWithCypherType ++ Relationship.nonPropertyAttributes.map(_ -> CTInteger)
        val df = readRelationshipTable(graphName, relType, capsSchema.canonicalRelStructType(relType))
        CAPSRelationshipTable(relType, df.setNullability(columnsWithCypherType))
      }
      CAPSGraph.create(capsMetaData.tags, Some(capsSchema), nodeTables.head, (nodeTables.tail ++ relTables).toSeq: _*)
    }
  }

  override def schema(graphName: GraphName): Option[CAPSSchema] = {
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

    val poolSize = session.sparkSession.conf.getOption("spark.executor.instances").map(_.toInt).getOrElse(2)

    implicit val executionContext: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(poolSize))

    val capsGraph = graph.asCaps
    val schema = capsGraph.schema
    schemaCache += graphName -> schema
    graphNameCache += graphName
    writeCAPSGraphMetaData(graphName, CAPSGraphMetaData(tableStorageFormat, capsGraph.tags))
    writeSchema(graphName, schema)

    val nodeWrites = schema.labelCombinations.combos.map { combo =>
      Future {
        writeNodeTable(graphName, combo, capsGraph.canonicalNodeTable(combo))
      }
    }

    val relWrites = schema.relationshipTypes.map { relType =>
      Future {
        writeRelationshipTable(graphName, relType, capsGraph.canonicalRelationshipTable(relType))
      }
    }

    waitForWriteCompletion(nodeWrites)
    waitForWriteCompletion(relWrites)
  }

  private def waitForWriteCompletion(writeFutures: Set[Future[Unit]])(implicit ec: ExecutionContext) = {
    writeFutures.foreach { writeFuture =>
      Await.ready(writeFuture, Duration.Inf)
      writeFuture.onComplete {
        case Success(_) =>
        case Failure(e) => throw e
      }
    }
  }
}
