package org.opencypher.spark.api.io

import cats.syntax.either._
import io.circe.Decoder
import org.apache.spark.sql.types._
import io.circe.generic.auto._
import org.opencypher.spark.api.util.JsonUtils

abstract class CsvSchema {
  def idField: CsvField
  def propertyFields: List[CsvField]

  def toStructType: StructType
}

case class CsvField(name: String, column: Int, valueType: String) {
  def getType: DataType = valueType.toLowerCase match {
    case "string" => StringType
    case "integer" => IntegerType
    case "long" => LongType
    case "boolean" => BooleanType
    case x => throw new RuntimeException(s"Unknown type $x")
  }

  def toStructField: StructField = StructField(name, getType, nullable = true)
}

case class CsvNodeSchema(idField: CsvField,
                         implicitLabels: List[String],
                         optionalLabels: List[CsvField],
                         propertyFields: List[CsvField])
                         extends CsvSchema {

  def toStructType: StructType = {
    StructType(
      (List(idField) ++ optionalLabels ++ propertyFields)
        .sortBy(_.column)
        .map(_.toStructField)
    )
  }
}

/**
  * Reads the schema of a node csv file. The schema file is in JSON format and has the following structure:
  * {
  *   "idField": {
  *     "name": "id",
  *     "column": 0,
  *     "valueType": "LONG"
  *   },
  *   "implicitLabels": ["Person","Employee"],
  *   "optionalLabels": [
  *     {
  *       "name": "Swede",
  *       "column": 3,
  *       "valueType": "BOOLEAN"
  *     },
  *     {
  *       "name": "German",
  *       "column": 4,
  *       "valueType": "BOOLEAN"
  *     }
  *   ],
  *   "propertyFields": [
  *     {
  *       "name": "name",
  *       "column": 1,
  *       "valueType": "STRING"
  *     },
  *     {
  *       "name": "luckyNumber",
  *       "column": 2,
  *       "valueType": "INTEGER"
  *     }
  *   ]
  * }
  */
object CsvNodeSchema {
  implicit val decodeNodeCsvSchema: Decoder[CsvNodeSchema] = for {
    idField <- Decoder.instance(_.get[CsvField]("idField"))
    implicitLabels <- Decoder.instance(_.get[List[String]]("implicitLabels"))
    optionalLabels <- Decoder.instance(_.getOrElse[List[CsvField]]("optionalLabels")(List()))
    propertyFields <- Decoder.instance(_.getOrElse[List[CsvField]]("propertyFields")(List()))
  } yield new CsvNodeSchema(idField, implicitLabels, optionalLabels, propertyFields)

  def apply(schemaJson: String): CsvNodeSchema = {
    JsonUtils.parseJson(schemaJson)
  }
}

case class CsvRelSchema(idField: CsvField,
                        startIdField: CsvField,
                        endIdField: CsvField,
                        relType: String,
                        propertyFields: List[CsvField])
                        extends CsvSchema {

  def toStructType: StructType = {
    StructType(
      (List(idField, startIdField, endIdField) ++ propertyFields)
          .sortBy(_.column)
        .map(_.toStructField)
    )
  }
}

/**
  * Reads the schema of a relationship csv file. The schema file is in JSON format and has the following structure:
  * {
  *   "idField": {
  *     "name": "id",
  *     "column": 0,
  *     "valueType": "LONG"
  *   },
  *   "startIdField": {
  *     "name": "start",
  *     "column": 1,
  *     "valueType": "LONG"
  *   },
  *   "endIdField": {
  *     "name": "end",
  *     "column": 2,
  *     "valueType": "LONG"
  *   },
  *   "relationshipType": "KNOWS",
  *   "propertyFields": [
  *     {
  *       "name": "since",
  *       "column": 3,
  *       "valueType": "INTEGER"
  *     }
  *   ]
  * }
  */
object CsvRelSchema {
  implicit val decodeRelCsvSchema: Decoder[CsvRelSchema] = for {
    id <- Decoder.instance(_.get[CsvField]("idField"))
    startIdField <- Decoder.instance(_.get[CsvField]("startIdField"))
    endIdField <- Decoder.instance(_.get[CsvField]("endIdField"))
    relType <- Decoder.instance(_.get[String]("relationshipType"))
    propertyFields <- Decoder.instance(_.getOrElse[List[CsvField]]("propertyFields")(List()))
  } yield new CsvRelSchema(id, startIdField, endIdField, relType, propertyFields)

  def apply(schemaJson: String): CsvRelSchema = {
    JsonUtils.parseJson(schemaJson)
  }
}