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

import java.util.concurrent._
import java.util.stream.Stream

import org.neo4j.graphdb._
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.logging.Log
import org.opencypher.okapi.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.api.types.CypherType._
import org.opencypher.okapi.api.value.CypherValue

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class SchemaCalculator(db: GraphDatabaseService, tx: KernelTransaction, log: Log) {

  /**
    * Computes the schema of the Neo4j graph as used by Okapi
    *
    * @return
    */
  def constructOkapiSchemaInfo(): Stream[OkapiSchemaInfo] = {

    val nodes: Iterator[Node] = db.getAllNodes.iterator().asScala
    val nodesSchema = computerEntitySchema(nodes){ node =>
        val propertyTypes = extractPropertyTypes(node.getAllProperties.asScala)
        val labelSet = node.getLabels.iterator().asScala.map(_.name).toSet
        val schema = Schema.empty.withNodePropertyKeys(labelSet.toSeq: _*)(propertyTypes.toSeq: _*)
        schema
    }

    val relationships = db.getAllRelationships.iterator().asScala
    val relationshipsSchema = computerEntitySchema(relationships){ relationship =>
        val propertyTypes = extractPropertyTypes(relationship.getAllProperties.asScala)
        val relType = relationship.getType.name
        Schema.empty.withRelationshipPropertyKeys(relType)(propertyTypes.toSeq: _*)
    }

    val nodeStream = nodesSchema.labelPropertyMap.map.flatMap {
      case (labels, properties) => getOkapiSchemaInfo("Node", labels.toSeq, properties)
    }

    val relStream = relationshipsSchema.relTypePropertyMap.map.flatMap {
      case (relType, properties) => getOkapiSchemaInfo("Relationship", Seq(relType), properties)
    }

    (nodeStream ++ relStream).asJavaCollection.stream()
  }

  /**
    * Computes the entity schema for the given entities by computing the schema for each individual entity and then
    * combining them. Uses batching to parallelize the computation
    *
    * @param entities entities for which to calculate the schema
    * @param extractor function that computes the schema for a given entity
    * @tparam T entity type
    * @return
    */
  private def computerEntitySchema[T <: Entity](entities: Iterator[T])(extractor: T => Schema): Schema = {
    val threads = Runtime.getRuntime.availableProcessors * 2
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(threads))
    entities
      .grouped(1000)
      .map { batch => Future { withTransaction{ batch.map(extractor).reduce(_ ++ _) }}}
      .map(Await.ready(_, Duration.apply(20, TimeUnit.SECONDS)))
      .map(_.value.get.get)
      .foldLeft(Schema.empty)(_ ++ _)
  }

  /**
    * Extracts the property types from the properties of Node/Relationship
    *
    * @param allProperties property map of a Node/Relationship
    * @return
    */
  private def extractPropertyTypes(allProperties: mutable.Map[String, AnyRef]): mutable.Map[String, CypherType] = {
    allProperties.flatMap {
      case (key, value) =>
        CypherValue.get(value).map(_.cypherType) match {
          case Some(cypherType) =>
            Some(key -> cypherType)

          case None =>
            log.warn(s"Schema procedure does not support type for property $key = $value of type ${value.getClass.getSimpleName}")
            None
        }
    }
  }

  /**
    * Generates the OkapiSchemaInfo entries for a given label combination / relationship type
    *
    * @param typ identifies the created entries (Label or Relationship)
    * @param labels label combination / relationship type for which the property keys are computed
    * @param propertyKeys propertyKeys for the given labels/ relationship type
    * @return
    */
  private def getOkapiSchemaInfo(
    typ: String,
    labels: Seq[String],
    propertyKeys: PropertyKeys
  ): Seq[OkapiSchemaInfo] = {
    if (propertyKeys.isEmpty) {
      Seq( new OkapiSchemaInfo(typ, labels.asJava, "", "" ) )
    } else {
      propertyKeys.map {
        case (property, cypherType) => new OkapiSchemaInfo(typ, labels.asJava, property, cypherType.toString())
      }
    }.toSeq
  }

  /**
    * Runs the given function wrapped in a Neo4j transaction and returns the result
    *
    * @param function code that will be run inside the transaction
    * @tparam T return type of the function
    * @return
    */
  private def withTransaction[T](function: => T): T = {
    val tx = db.beginTx()
    val res = function
    tx.success()
    res
  }
}
