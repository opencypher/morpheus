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
package org.opencypher.spark.api.io.neo4j

import org.apache.log4j.LogManager
import org.neo4j.driver.v1.Value
import org.neo4j.driver.v1.util.Function
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{CTVoid, CypherType}
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.spark.impl.io.neo4j.Neo4jHelpers._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object SchemaFromProcedure {

  private val log = LogManager.getLogger(getClass)

  val schemaProcedureName = "org.opencypher.okapi.procedures.schema"

  def apply(config: Neo4jConfig, omitImportFailures: Boolean): Option[Schema] = {
    Try {
      config.cypher("CALL dbms.procedures() YIELD name AS name").exists { map =>
        map("name").value == schemaProcedureName
      }
    } match {
      case Success(true) =>
        schemaFromProcedure(config, omitImportFailures)
      case Success(false) =>
        System.err.println("Neo4j schema procedure not activated. Consider activating the procedure `" + schemaProcedureName + "`.")
        None
      case Failure(error) =>
        System.err.println(s"Retrieving the procedure list from the Neo4j database failed: $error")
        None
    }
  }

  private[neo4j] def schemaFromProcedure(config: Neo4jConfig, omitImportFailures: Boolean): Option[Schema] = {
    Try {
      val rows = config.withSession { session =>
        val result = session.run("CALL " + schemaProcedureName)

        result.list().asScala.flatMap { row =>
          val extractString = new Function[Value, String] {
            override def apply(v1: Value): String = v1.asString()
          }

          val typ = row.get("type").asString()
          val labels = row.get("nodeLabelsOrRelType").asList(extractString).asScala.toList

          row.get("property").asString() match {
            case "" => // this label/type has no properties
              Seq((typ, labels, None, None))
            case property =>
              val typeStrings = row.get("cypherTypes").asList(extractString).asScala.toList
              val cypherType: CypherType = typeStrings
                .flatMap { s => CypherType.fromName(s) match {
                  case v@ Some(_) => v
                  case None if omitImportFailures  =>
                    log.warn(s"At least one $typ with labels ${labels.mkString(",")} has unsupported property type $s for property $property")
                    None
                  case None => throw UnsupportedOperationException(
                    s"At least one $typ with labels ${labels.mkString(",")} has unsupported property type $s for property $property"
                  )
                }}
                .foldLeft(CTVoid: CypherType)(_ join _)

              Seq((typ, labels, Some(property), Some(cypherType)))
          }
        }
      }

      rows.groupBy(row => row._1 -> row._2).map {
        case ((typ, labels), tuples) =>
          val properties = tuples.collect {
            case (_, _, p, t) if p.nonEmpty => p.get -> t.get
          }

          typ match {
            case "Node" =>
              Schema.empty.withNodePropertyKeys(labels: _*)(properties: _*)
            case "Relationship" =>
              Schema.empty.withRelationshipPropertyKeys(labels.headOption.getOrElse(""))(properties: _*)
          }
      }.foldLeft(Schema.empty)(_ ++ _)
    } match {
      case Success(schema) => Some(schema)
      case Failure(error) =>
        System.err.println(s"Could not load schema from Neo4j: ${error.getMessage}")
        error.printStackTrace()
        None
    }
  }

}
