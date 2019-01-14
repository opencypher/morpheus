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
package org.opencypher.okapi.neo4j.io

import org.apache.logging.log4j.scala.Logging
import org.opencypher.okapi.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.okapi.api.schema.{PropertyKeys, Schema}
import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.api.value.CypherValue.{CypherBoolean, CypherList, CypherString, CypherValue}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, SchemaException}
import org.opencypher.okapi.neo4j.io.Neo4jHelpers._

import scala.util.{Failure, Success, Try}

object SchemaFromProcedure extends Logging {

  /**
    * Returns the schema for a Neo4j graph. When the schema contains a property that is incompatible with CAPS,
    * then an exception is thrown. Please set `omitImportFailures` in order to omit such properties from the schema
    * instead.
    *
    * This method relies on the built-in Neo4j schema procedures, which were introduced in Neo4j versions 3.3.9,
    * 3.4.10, and 3.5.0.
    *
    * @param neo4j              configuration for the Neo4j instance
    * @param omitImportFailures when set, incompatible properties are omitted from the schema and a warning is logged
    * @return schema for the Neo4j graph
    */
  def apply(neo4j: Neo4jConfig, omitImportFailures: Boolean): Schema = {
    neo4j.withSession { implicit session =>

      // Checks if the Neo4j instance supports the required schema procedures
      def areSchemaProceduresSupported: Boolean = {
        val schemaProcedures = Set(nodeSchemaProcedure, relSchemaProcedure)
        val supportedProcedures = cypher("CALL dbms.procedures() YIELD name AS name")
          .map(procedure => procedure("name").value.toString)
          .toSet
        schemaProcedures.subsetOf(supportedProcedures)
      }

      // Aggregates all property keys contained in a list of rows
      def propertyKeysForRows(rows: List[Row]): PropertyKeys = {
        rows.foldLeft(PropertyKeys.empty) { case (currentPropertyKeys, nextRow) =>
          currentPropertyKeys ++ nextRow.propertyKeys(omitImportFailures)
        }
      }

      Try {
        areSchemaProceduresSupported
      } match {
        case Success(true) =>
          val nodeRows = cypher(s"CALL $nodeSchemaProcedure")
          val nodeRowsGroupedByCombo = nodeRows.groupBy(_.labels)
          val nodeSchema = nodeRowsGroupedByCombo.foldLeft(Schema.empty) { case (currentSchema, (combo, rows)) =>
            currentSchema.withNodePropertyKeys(combo, propertyKeysForRows(rows))
          }
          val relRows = cypher(s"CALL $relSchemaProcedure")
          val relRowsGroupedByType = relRows.groupBy(_.relType)
          val relSchema = relRowsGroupedByType.foldLeft(Schema.empty) { case (currentSchema, (tpe, rows)) =>
            currentSchema.withRelationshipPropertyKeys(tpe, propertyKeysForRows(rows))
          }
          nodeSchema ++ relSchema
        case Success(false) => throw SchemaException(
          s"""|Your version of Neo4j does not support `$nodeSchemaProcedure` and `$relSchemaProcedure`.
              |Schema procedure support was added by Neo4j versions 3.3.9, 3.4.10, and 3.5.0.
           """.stripMargin)
        case Failure(error) =>
          throw SchemaException(s"Could not retrieve the procedure list from the Neo4j database", Some(error))
      }

    }
  }

  // Neo4j schema procedure rows are represented as a map from column name strings to Cypher values
  type Row = Map[String, CypherValue]

  /**
    * Functions to extract values from a Neo4j schema procedure row.
    */
  implicit private class Neo4jSchemaRow(row: Row) {

    def propertyKeys(omitImportFailures: Boolean): PropertyKeys = {
      propertyName match {
        case Some(name) =>
          maybePropertyType(omitImportFailures)
            .map(ct => PropertyKeys(name -> ct))
            .getOrElse(PropertyKeys.empty)
        case None =>
          PropertyKeys.empty
      }
    }

    def neo4jPropertyTypeStrings: List[String] = getValueAsStrings("propertyTypes")

    def isRelationshipSchema: Boolean = row.contains("relType")

    def relType: String = {
      val relString = row.get("relType") match {
        case Some(CypherString(s)) => s
        case _ => throw IllegalArgumentException("a valid Neo4j relationship schema row with a relationship type", row)
      }
      if (relString.length > 3 && relString.take(2) == ":`" && relString.takeRight(1) == "`") {
        relString.drop(2).dropRight(1)
      } else {
        throw IllegalArgumentException(s"a Neo4j schema `relType` with format :`REL_TYPE_NAME`", relString)
      }
    }

    def labels: Set[String] = getValueAsStrings("nodeLabels").toSet

    def propertyName: Option[String] = row.get("propertyName").collect { case CypherString(s) => s }

    def isMandatory: Boolean = row.get("mandatory").collect { case CypherBoolean(b) => b }.getOrElse(false)

    /**
      * Returns the Cypher type from a Neo4j schema row.
      *
      * CAPS can have at most one property type for a given property on a given label combination.
      * If different property types are present on the same property with multiple nodes that have the same
      * label combination, then by default a schema exception is thrown. If `omitImportFailures` is set, then the
      * problematic property is instead excluded from the schema and a warning is logged.
      *
      * @param omitImportFailures when set, incompatible properties are omitted from the schema and a warning is logged
      * @return joined Cypher type of a property on a label combination
      */
    private def maybePropertyType(omitImportFailures: Boolean): Option[CypherType] = {
      def wasNotAdded = "The property was not added to the schema due to the set `omitImportFailures` flag."
      def setFlag = "Set the `omitImportFailures` flag to compute the Neo4j schema without this property."
      def rowTypeDescription = if (isRelationshipSchema) s"relationship type [:$relType]" else s"node label combination [:${labels.mkString(", ")}]"
      def multipleTypes = s"The Neo4j property `${propertyName.getOrElse("")}` on $rowTypeDescription has multiple property types: [${neo4jPropertyTypeStrings.mkString(", ")}]."

      neo4jPropertyTypeStrings match {
        case neo4jTypeString :: Nil =>
          def unsupportedPropertyType = s"The Neo4j property `${propertyName.getOrElse("")}` on $rowTypeDescription has unsupported property type `$neo4jTypeString`."
          neo4jTypeString.toCypherType match {
            case None if omitImportFailures =>
              logger.warn(s"$unsupportedPropertyType $wasNotAdded")
              None
            case None =>
              throw SchemaException(
                s"$unsupportedPropertyType $setFlag")
            case Some(tpe) =>
              if (isMandatory) Some(tpe) else Some(tpe.nullable)
          }
        case Nil =>
          None
        case _ if omitImportFailures =>
          logger.warn(s"$multipleTypes\n$wasNotAdded")
          None
        case _ =>
          throw SchemaException(s"$multipleTypes\n$setFlag")
      }
    }

    private def getValueAsStrings(key: String): List[String] = {
      row.getOrElse[CypherValue](key, CypherList.empty) match {
        case CypherList(l) => l.collect { case CypherString(s) => s }
        case nonList => throw IllegalArgumentException("A Cypher list of strings", nonList)
      }
    }

  }

  /**
    * String replacement to convert Neo4j type string representations to ones that are compatible with CAPS.
    */
  implicit class Neo4jTypeString(val s: String) extends AnyVal {

    private def toCapsTypeString: String = neo4jTypeMapping.foldLeft(s) { case (currentTypeString, (neo4jType, capsType)) =>
      currentTypeString.replaceAll(neo4jType, capsType)
    }

    def toCypherType: Option[CypherType] = CypherType.fromName(toCapsTypeString)

  }

  private[neo4j] val neo4jTypeMapping: Map[String, String] = Map(
    "(.+)Array" -> "List($1)",
    "Double" -> "Float",
    "Long" -> "Integer"
  )

  private[neo4j] val nodeSchemaProcedure = "db.schema.nodeTypeProperties"
  private[neo4j] val relSchemaProcedure = "db.schema.relTypeProperties"

}
