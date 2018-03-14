/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
 */
package org.opencypher.spark.tools

import java.io.{FileOutputStream, PrintWriter}
import java.nio.file.{Files, Path, Paths}
import java.util.stream.Collectors

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.opencypher.spark.tools.StructTypes._

import scala.collection.JavaConverters._
import scala.collection.Map

object LdbcConverter extends App {

  if (args.length != 3) {
    println("Expecting 'LdbcConverter <spark-master> <input-path> <output-path>'")
  }

  private val spark = {
    val conf = new SparkConf(true)
    conf.set("spark.sql.codegen.wholeStage", "true")
    conf.set("spark.sql.shuffle.partitions", "12")
    conf.set("spark.default.parallelism", "8")

    SparkSession
      .builder()
      .config(conf)
      .master(args(0))
      .appName("LDBC SNB Converter")
      .getOrCreate()
  }

  private val inputPath = args(1)

  private val outputPath = args(2)

  private val nodesFolder = "nodes"

  private val relsFolder = "relationships"

  private val dataSuffix = ".csv"

  private val schemaSuffix = ".csv.SCHEMA"

  private val idColumnName = "id"

  private val id0ColumnName = "id0"

  private val id1ColumnName = "id1"

  private val typeColumnName = "type"

  private val sourceTypeColumnName = "source_type"

  private val targetTypeColumnName = "target_type"

  private val listItemSeparator = "|"

  convert()

  private def convert(): Unit = {

    val inputNodes = getDataFrames("nodes", getLabelKey)
    val inputRels = getDataFrames("relationships", getTypeKey)
    val inputProps = getDataFrames("properties", getPropertyKey)

    val relsWithSourceAndTargetLabel = addSourceAndTargetLabel(inputNodes, inputRels)
    val relsWithFinalLabel = splitRelDataFrames(relsWithSourceAndTargetLabel)

    val nodesWithUpdatedProperties = addProperties(inputNodes)
    val nodesWithListProperties = addListProperties(nodesWithUpdatedProperties, inputProps)
    val nodesWithFinalLabel = splitNodeDataFrames(nodesWithListProperties)

    val nodes = updateNodeIdentifiers(nodesWithFinalLabel)
    val relsWithUpdatedNodeIds = updateSourceAndTargetIdentifiers(relsWithFinalLabel)
    val rels = addRelIdentifiers(relsWithUpdatedNodeIds)

    Files.createDirectories(Paths.get(outputPath, nodesFolder))
    Files.createDirectories(Paths.get(outputPath, relsFolder))

    nodes.foreach {
      case (label, nodeDf) =>
        writeNodeSchema(label, nodeDf.schema)
        nodeDf.write.csv(Paths.get(outputPath, nodesFolder, s"$label$dataSuffix").toString)
    }

    rels.foreach {
      case (relType, relDf) =>
        writeRelSchema(relType, relDf.schema)
        relDf.write.csv(Paths.get(outputPath, relsFolder, s"$relType$dataSuffix").toString)
    }
  }

  private def getDataFrames[T](folder: String, labelLookup: Path => T): Map[T, DataFrame] =
    listDirectory(Paths.get(inputPath, folder))
      .map(path => labelLookup(path) -> loadDF(path, StructTypes(labelLookup(path))))
      .toMap

  private def listDirectory(directory: Path): List[Path] = Files
    .list(directory)
    .collect(Collectors.toList())
    .asScala
    .toList

  private def parseLabel(label: String): String = nodeMapping.keys.collectFirst {
    case Label(nodeLabel, _) if nodeLabel.toLowerCase == label.toLowerCase => nodeLabel
  }.getOrElse(throw new IllegalArgumentException(s"Expected one of: ${nodeMapping.keys.mkString("[",", ", "]")} got: $label"))

  private def parseType(relType: String) = relType.map(c => if (c.isUpper) s"_$c" else c.toString).mkString.toUpperCase

  private def parseProperty(prop: String) = prop

  private def getLabelKey(path: Path): Label = Label(parseLabel(path.getFileName.toString.split("_")(0)))

  private def getTypeKey(path: Path): RelType = {
    val tokens = path.getFileName.toString.split("_")
    RelType(parseLabel(tokens(0)), parseType(tokens(1)), parseLabel(tokens(2)))
  }

  private def getPropertyKey(path: Path): Property = {
    val tokens = path.getFileName.toString.split("_")
    Property(parseLabel(tokens(0)), parseProperty(tokens(1)))
  }

  private def loadDF(path: Path, schema: StructType): DataFrame = spark.read.format("csv")
    .option("header", "true")
    .option("sep", "|")
    .schema(schema)
    .load(path.toString)

  private def addProperties(nodes: Map[Label, DataFrame]): Map[Label, DataFrame] = nodes.map {
    case (label, nodeDf) if label.nodeLabel == "Person" =>
      val birthDay = from_unixtime(nodeDf.col("birthday").divide(lit(1000)))
      label -> nodeDf
        .withColumn("birthday_year", year(birthDay))
        .withColumn("birthday_month", month(birthDay))
        .withColumn("birthday_day", functions.dayofmonth(birthDay))
    case (label, nodeDf) => label -> nodeDf
  }

  private def addListProperties(nodes: Map[Label, DataFrame], properties: Map[Property, DataFrame]): Map[Label, DataFrame] = {
    val listToString = udf((items: Seq[String]) => items.mkString(listItemSeparator))

    nodes.map {
      case (label, nodeDf) => (label, nodeDf, properties.collect {
        case (property, propertyDf) if property.nodeLabel == label.nodeLabel => property -> propertyDf
      })
    }.map {
      case (label, nodeDf, props) => label -> props.foldLeft(nodeDf) {
        case (tmpNodeDf, (prop, propDF)) =>
          val withListProperty = tmpNodeDf
            .join(propDF, tmpNodeDf.col(idColumnName) === propDF.col(s"${label.nodeLabel}_id"), "left_outer")
            .drop(propDF.col(s"${label.nodeLabel}_id"))
            .groupBy(tmpNodeDf.columns.map(tmpNodeDf.col): _*)
            .agg(functions.collect_list(propDF.col(prop.propertyKey)).as(prop.propertyKey))

          // this is necessary because Sparks CSV writer does not support array<string> data type
          withListProperty.withColumn(prop.propertyKey, listToString(withListProperty.col(prop.propertyKey)))


      }
    }.toMap
  }

  private def updateNodeIdentifiers(nodes: Map[Label, DataFrame]): Map[Label, DataFrame] = nodes.map {
    case (label, nodeDf) =>
      val newIdColumn = concat(nodeDf.col(idColumnName), lit(StructTypes.nodeDictionary(label))).as(idColumnName)
      val columns = nodeDf.columns.filterNot(_ == idColumnName).map(nodeDf.col) :+ newIdColumn
      label -> nodeDf.select(columns: _*)
  }

  private def updateSourceAndTargetIdentifiers(rels: Map[RelType, DataFrame]): Map[RelType, DataFrame] = rels.map {
    case (relType, relDf) =>

      val sourceIdColumn = relDf.col(s"${relType.sourceLabel.nodeLabel}_${sourceSuffix(relType)}")
      val newSourceIdColumn = concat(sourceIdColumn,
        lit(StructTypes.nodeDictionary(relType.sourceLabel))).as(sourceIdColumn.toString)

      val targetIdColumn = relDf.col(s"${relType.targetLabel.nodeLabel}_${targetSuffix(relType)}")
      val newTargetIdColumn = concat(targetIdColumn,
        lit(StructTypes.nodeDictionary(relType.targetLabel))).as(targetIdColumn.toString)

      val columns = relDf.columns
        .map(relDf.col)
        .filterNot(_ == sourceIdColumn)
        .filterNot(_ == targetIdColumn)

      relType -> relDf.select(columns :+ newSourceIdColumn :+ newTargetIdColumn: _*)
  }

  private def addRelIdentifiers(rels: Map[RelType, DataFrame]): Map[RelType, DataFrame] = rels.map {
    case (relType, relDf) => relType -> relDf
      .withColumn(idColumnName, concat(monotonically_increasing_id().cast(StringType), lit(StructTypes.relDictionary(relType))))
  }

  private def splitNodeDataFrames(nodes: Map[Label, DataFrame]): Map[Label, DataFrame] = nodes.flatMap {
    case (label, nodeDf) if StructTypes(label).fields.exists(_.name == typeColumnName) =>
      val typeColumn = nodeDf.col(typeColumnName)
      val newLabels = nodeDf
        .select(typeColumn)
        .distinct()
        .collect()
        .map(_.get(0).toString)

      newLabels.map(newLabel => Label(parseLabel(newLabel), Some(label)) -> nodeDf.where(typeColumn === lit(newLabel)).drop(typeColumn))

    case (label, nodeDf) => Seq(label -> nodeDf)
  }

  private def splitRelDataFrames(rels: Map[RelType, DataFrame]): Map[RelType, DataFrame] = {


    rels.flatMap {
      case (relType, relDf) if relType.sourceLabel.subtypes.nonEmpty || relType.targetLabel.subtypes.nonEmpty =>
        val newTypes = relDf
          .select(sourceTypeColumnName, targetTypeColumnName)
          .distinct()
          .collect()
          .map(row => row.get(0).toString -> row.get(1).toString)

        newTypes.map {
          case (sourceLabel, targetLabel) =>

            val srcSuffix = sourceSuffix(relType)
            val tgtSuffix = targetSuffix(relType)

            val newSrcSuffix = sourceSuffix(sourceLabel, targetLabel)
            val newTgtSuffix = targetSuffix(sourceLabel, targetLabel)

            val parsedSourceLabel = parseLabel(sourceLabel)
            val parsedTargetLabel = parseLabel(targetLabel)

            val sourceSuperType = if (relType.sourceLabel.nodeLabel == parsedSourceLabel) None else Some(relType.sourceLabel)
            val targetSuperType = if (relType.targetLabel.nodeLabel == parsedTargetLabel) None else Some(relType.targetLabel)

            val newReltype = RelType(
              Label(parsedSourceLabel, sourceSuperType),
              relType.relType,
              Label(parsedTargetLabel, targetSuperType))

            val newDf = relDf
              .where(relDf.col(sourceTypeColumnName) === lit(sourceLabel) && relDf.col(targetTypeColumnName) === lit(targetLabel))
              .drop(sourceTypeColumnName, targetTypeColumnName)
              .withColumnRenamed(s"${relType.sourceLabel.nodeLabel}_$srcSuffix", s"${parsedSourceLabel}_$newSrcSuffix")
              .withColumnRenamed(s"${relType.targetLabel.nodeLabel}_$tgtSuffix", s"${parsedTargetLabel}_$newTgtSuffix")

            newReltype -> newDf
        }

      case (relType, relDf) =>
        Seq(relType -> relDf.drop(sourceTypeColumnName, targetTypeColumnName))
    }
  }


  /**
    * Adds 'source_type' and 'target_type' column to each relationship dataframe that store the corresponding node label.
    * If the relationship connects super types (e.g. organisation), the column stores the specific type (e.g. university).
    *
    * @param nodes
    * @param rels
    * @return
    */
  private def addSourceAndTargetLabel(nodes: Map[Label, DataFrame], rels: Map[RelType, DataFrame]): Map[RelType, DataFrame] = {
    val typeColumns = Seq(sourceTypeColumnName, targetTypeColumnName)

    rels.map {
      case (relType, relDf) if relType.sourceLabel.subtypes.nonEmpty && relType.targetLabel.subtypes.nonEmpty =>
        val sourceNodes = nodes(relType.sourceLabel)
        val targetNodes = nodes(relType.targetLabel)

        val sourceSelectColumns = sourceTypeColumnName :: relDf.columns.toList
        val selectColumns = typeColumns ++ relDf.columns.toList

        val withSourceLabel = sourceNodes
          .join(relDf, sourceNodes.col(idColumnName) === relDf.col(s"${relType.sourceLabel.nodeLabel}_${sourceSuffix(relType)}"))
          .withColumnRenamed(typeColumnName, sourceTypeColumnName)
          .select(sourceSelectColumns.head, sourceSelectColumns.tail: _*)

        val withTargetLabel = withSourceLabel
          .join(targetNodes, relDf.col(s"${relType.targetLabel.nodeLabel}_${targetSuffix(relType)}") === targetNodes.col(idColumnName))
          .withColumnRenamed(typeColumnName, targetTypeColumnName)
          .select(selectColumns.head, selectColumns.tail: _*)

        relType -> withTargetLabel

      case (relType, relDf) if relType.sourceLabel.subtypes.nonEmpty =>
        val sourceNodes = nodes(relType.sourceLabel)
        val selectColumns = typeColumns ++ relDf.columns.toList

        relType -> sourceNodes
          .join(relDf, sourceNodes.col(idColumnName) === relDf.col(s"${relType.sourceLabel.nodeLabel}_id"))
          .withColumnRenamed(typeColumnName, sourceTypeColumnName)
          .withColumn(targetTypeColumnName, lit(relType.targetLabel.nodeLabel.toLowerCase))
          .select(selectColumns.head, selectColumns.tail: _*)

      case (relType, relDf) if relType.targetLabel.subtypes.nonEmpty =>
        val targetNodes = nodes(relType.targetLabel)
        val selectColumns = typeColumns ++ relDf.columns.toList

        relType -> relDf
          .join(targetNodes, relDf.col(s"${relType.targetLabel.nodeLabel}_id") === targetNodes.col(idColumnName))
          .withColumnRenamed(typeColumnName, targetTypeColumnName)
          .withColumn(sourceTypeColumnName, lit(relType.sourceLabel.nodeLabel.toLowerCase))
          .select(selectColumns.head, selectColumns.tail: _*)

      case (relType, relDf) => relType -> relDf
        .withColumn(sourceTypeColumnName, lit(relType.sourceLabel.nodeLabel.toLowerCase))
        .withColumn(targetTypeColumnName, lit(relType.targetLabel.nodeLabel.toLowerCase))
    }
  }

  private def sourceSuffix(sourceLabel: String, targetLabel: String): String =
    sourceSuffix(RelType(sourceLabel, "", targetLabel))

  private def targetSuffix(sourceLabel: String, targetLabel: String): String =
    targetSuffix(RelType(sourceLabel, "", targetLabel))

  private def sourceSuffix(relType: RelType): String =
    if (relType.sourceLabel == relType.targetLabel) id0ColumnName else idColumnName

  private def targetSuffix(relType: RelType): String =
    if (relType.sourceLabel == relType.targetLabel) id1ColumnName else idColumnName

  private def writeNodeSchema(label: Label, structType: StructType): Unit = {
    val schemaString =
      s"""{
         |  "idField": {
         |    "name": "CapsID",
         |    "column": ${structType.fieldIndex(idColumnName)},
         |    "valueType": "String"
         |  },
         |  "implicitLabels": ["${label.nodeLabel}"],
         |  "propertyFields": [${
        structType.fields.filterNot(_.name == idColumnName).map { structField =>
          s"""
             |    {
             |      "name": "${structField.name}",
             |      "column": ${structType.fieldIndex(structField.name)},
             |      "valueType": "${PropertyTypes(structField.name)}"
             |    }""".stripMargin
        }.mkString(",")
      }
         |  ]
         |}""".stripMargin

    val writer = new PrintWriter(new FileOutputStream(Paths.get(outputPath, nodesFolder, s"$label$schemaSuffix").toString, false))
    writer.write(schemaString)
    writer.close()
  }

  private def writeRelSchema(relType: RelType, structType: StructType): Unit = {

    val sourceColumn = s"${relType.sourceLabel.nodeLabel}_${sourceSuffix(relType)}"
    val targetColumn = s"${relType.targetLabel.nodeLabel}_${targetSuffix(relType)}"

    val fixedColumns = Set(idColumnName, sourceColumn, targetColumn)
    val schema =
      s"""{
         |  "idField": {
         |    "name": "CapsID",
         |    "column": ${structType.fieldIndex(idColumnName)},
         |    "valueType": "String"
         |  },
         |  "startIdField": {
         |     "name": "SRC",
         |     "column": ${structType.fieldIndex(sourceColumn)},
         |     "valueType": "String"
         |  },
         |  "endIdField": {
         |     "name": "DST",
         |     "column": ${structType.fieldIndex(targetColumn)},
         |     "valueType": "String"
         |  },
         |  "relationshipType": "${relType.relType}",
         |  "propertyFields": [${
        structType.fields.filterNot(field => fixedColumns.contains(field.name)).map { structField =>
        s"""
           |    {
           |      "name": "${structField.name}",
           |      "column": ${structType.fieldIndex(structField.name)},
           |      "valueType": "${PropertyTypes(structField.name)}"
           |    }""".stripMargin}.mkString(",")}
         |  ]
         |}""".stripMargin

    val writer = new PrintWriter(new FileOutputStream(Paths.get(outputPath, relsFolder, s"$relType$schemaSuffix").toString, false))
    writer.write(schema)
    writer.close()
  }
}

case class Label(nodeLabel: String, superType: Option[Label] = None) {
  override def toString: String = nodeLabel
}

object Label {
  def apply(nodeLabel: String, superType: String): Label =
    Label(nodeLabel, Some(Label(superType)))
}

case class RelType(sourceLabel: Label, relType: String, targetLabel: Label) {
  override def toString: String = s"${sourceLabel.nodeLabel.toUpperCase}_${relType}_${targetLabel.nodeLabel.toUpperCase}"
}

object RelType {
  def apply(sourceLabel: String, relType: String, targetLabel: String): RelType =
    RelType(Label(sourceLabel), relType, Label(targetLabel))
}

case class Property(nodeLabel: String, propertyKey: String)


object PropertyTypes {
  def apply(property: String): String = mapping(property)

  val mapping = Map(
    "birthday" -> "Long",
    "name" -> "String",
    "imageFile" -> "String",
    "email" -> "List[String]",
    "classYear" -> "Integer",
    "workFrom" -> "Integer",
    "locationIP" -> "String",
    "birthday_month" -> "Integer",
    "birthday_year" -> "Integer",
    "lastName" -> "String",
    "firstName" -> "String",
    "speaks" -> "List[String]",
    "language" -> "String",
    "browserUsed" -> "String",
    "joinDate" -> "Long",
    "content" -> "String",
    "creationDate" -> "Long",
    "length" -> "Integer",
    "title" -> "String",
    "birthday_day" -> "Integer",
    "gender" -> "String",
    "id" -> "Long",
    "url" -> "String"
  )
}

//noinspection NameBooleanParameters
object StructTypes {

  def apply[T](label: T): StructType = label match {
    case l: Label => nodeMapping(l)
    case r: RelType => relMapping(r)
    case p: Property => propertyMapping(p)
  }

  val nodeMapping = Map(
    Label("Organisation") -> StructType(Seq(StructField("id", StringType, false), StructField("type", StringType, true), StructField("name", StringType, true), StructField("url", StringType, true))),
    Label("University", Some(Label("Organisation"))) -> StructType(Seq(StructField("id", StringType, false), StructField("name", StringType, true), StructField("url", StringType, true))),
    Label("Company", Some(Label("Organisation"))) -> StructType(Seq(StructField("id", StringType, false), StructField("name", StringType, true), StructField("url", StringType, true))),
    Label("Forum") -> StructType(Seq(StructField("id", StringType, false), StructField("title", StringType, true), StructField("creationDate", LongType, true))),
    Label("Person") -> StructType(Seq(StructField("id", StringType, false), StructField("firstName", StringType, true), StructField("lastName", StringType, true), StructField("gender", StringType, true), StructField("birthday", LongType, true), StructField("creationDate", LongType, true), StructField("locationIP", StringType, true), StructField("browserUsed", StringType, true))),
    Label("Place") -> StructType(Seq(StructField("id", StringType, false), StructField("name", StringType, true), StructField("url", StringType, true), StructField("type", StringType, true))),
    Label("City", Some(Label("Place"))) -> StructType(Seq(StructField("id", StringType, false), StructField("name", StringType, true), StructField("url", StringType, true))),
    Label("Country", Some(Label("Place"))) -> StructType(Seq(StructField("id", StringType, false), StructField("name", StringType, true), StructField("url", StringType, true))),
    Label("Continent", Some(Label("Place"))) -> StructType(Seq(StructField("id", StringType, false), StructField("name", StringType, true), StructField("url", StringType, true))),
    Label("Post") -> StructType(Seq(StructField("id", StringType, false), StructField("imageFile", StringType, true), StructField("creationDate", LongType, true), StructField("locationIP", StringType, true), StructField("browserUsed", StringType, true), StructField("language", StringType, true), StructField("content", StringType, true), StructField("length", StringType, true))),
    Label("Comment") -> StructType(Seq(StructField("id", StringType, false), StructField("creationDate", LongType, true), StructField("locationIP", StringType, true), StructField("browserUsed", StringType, true), StructField("content", StringType, true), StructField("length", IntegerType, true))),
    Label("TagClass") -> StructType(Seq(StructField("id", StringType, false), StructField("name", StringType, true), StructField("url", StringType, true))),
    Label("Tag") -> StructType(Seq(StructField("id", StringType, false), StructField("name", StringType, true), StructField("url", StringType, true)))
  )

  val nodeDictionary: Map[Label, Int] = nodeMapping.zipWithIndex.map { case ((label, _), i) => label -> i }

  implicit class RichLabel(label: Label) {
    def subtypes: Iterable[Label] = nodeMapping.filterKeys {
      case Label(_, Some(superType)) => superType == label
      case _ => false
    }.keys
  }

  val relMapping = Map(
    RelType("Comment", "HAS_CREATOR", "Person") -> StructType(Seq(StructField("Comment_id", StringType, false), StructField("Person_id", StringType, false))),
    RelType("Person", "LIKES", "Post") -> StructType(Seq(StructField("Person_id", StringType, false), StructField("Post_id", StringType, false), StructField("creationDate", LongType, true))),
    RelType("Post", "HAS_TAG", "Tag") -> StructType(Seq(StructField("Post_id", StringType, false), StructField("Tag_id", StringType, false))),
    RelType("Forum", "HAS_MEMBER", "Person") -> StructType(Seq(StructField("Forum_id", StringType, false), StructField("Person_id", StringType, false), StructField("joinDate", LongType, true))),
    RelType("Comment", "REPLY_OF", "Post") -> StructType(Seq(StructField("Comment_id", StringType, false), StructField("Post_id", StringType, false))),
    RelType("Forum", "HAS_TAG", "Tag") -> StructType(Seq(StructField("Forum_id", StringType, false), StructField("Tag_id", StringType, false))),
    RelType("Person", "STUDY_AT", "Organisation") -> StructType(Seq(StructField("Person_id", StringType, false), StructField("Organisation_id", StringType, false), StructField("classYear", IntegerType, true))),
    RelType(Label("Person"), "STUDY_AT", Label("University", "Organisation")) -> StructType(Seq(StructField("Person_id", StringType, false), StructField("University_id", StringType, false), StructField("classYear", IntegerType, true))),
    RelType("Person", "WORK_AT", "Organisation") -> StructType(Seq(StructField("Person_id", StringType, false), StructField("Organisation_id", StringType, false), StructField("workFrom", IntegerType, true))),
    RelType(Label("Person"), "WORK_AT", Label("Company", "Organisation")) -> StructType(Seq(StructField("Person_id", StringType, false), StructField("Company_id", StringType, false), StructField("workFrom", IntegerType, true))),
    RelType("Place", "IS_PART_OF", "Place") -> StructType(Seq(StructField("Place_id0", StringType, false), StructField("Place_id1", StringType, false))),
    RelType(Label("City", "Place"), "IS_PART_OF", Label("Country", "Place")) -> StructType(Seq(StructField("City_id", StringType, false), StructField("Country_id", StringType, false))),
    RelType(Label("Country", "Place"), "IS_PART_OF", Label("Continent", "Place")) -> StructType(Seq(StructField("Country_id", StringType, false), StructField("Continent_id", StringType, false))),
    RelType("Forum", "CONTAINER_OF", "Post") -> StructType(Seq(StructField("Forum_id", StringType, false), StructField("Post_id", StringType, false))),
    RelType("Person", "HAS_INTEREST", "Tag") -> StructType(Seq(StructField("Person_id", StringType, false), StructField("Tag_id", StringType, false))),
    RelType("Comment", "HAS_TAG", "Tag") -> StructType(Seq(StructField("Comment_id", StringType, false), StructField("Tag_id", StringType, false))),
    RelType("Comment", "REPLY_OF", "Comment") -> StructType(Seq(StructField("Comment_id0", StringType, false), StructField("Comment_id1", StringType, false))),
    RelType("Person", "KNOWS", "Person") -> StructType(Seq(StructField("Person_id0", StringType, false), StructField("Person_id1", StringType, false), StructField("creationDate", LongType, true))),
    RelType("Comment", "IS_LOCATED_IN", "Place") -> StructType(Seq(StructField("Comment_id", StringType, false), StructField("Place_id", StringType, false))),
    RelType(Label("Comment"), "IS_LOCATED_IN", Label("Country", "Place")) -> StructType(Seq(StructField("Comment_id", StringType, false), StructField("Country_id", StringType, false))),
    RelType("Forum", "HAS_MODERATOR", "Person") -> StructType(Seq(StructField("Forum_id", StringType, false), StructField("Person_id", StringType, false))),
    RelType("Tag", "HAS_TYPE", "TagClass") -> StructType(Seq(StructField("Tag_id", StringType, false), StructField("TagClass_id", StringType, false))),
    RelType("Post", "HAS_CREATOR", "Person") -> StructType(Seq(StructField("Post_id", StringType, false), StructField("Person_id", StringType, false))),
    RelType("Person", "LIKES", "Comment") -> StructType(Seq(StructField("Person_id", StringType, false), StructField("Comment_id", StringType, false), StructField("creationDate", LongType, true))),
    RelType("Post", "IS_LOCATED_IN", "Place") -> StructType(Seq(StructField("Post_id", StringType, false), StructField("Place_id", StringType, false))),
    RelType(Label("Post"), "IS_LOCATED_IN", Label("Country", "Place")) -> StructType(Seq(StructField("Post_id", StringType, false), StructField("Country_id", StringType, false))),
    RelType("Organisation", "IS_LOCATED_IN", "Place") -> StructType(Seq(StructField("Organisation_id", StringType, false), StructField("Place_id", StringType, false))),
    RelType(Label("University", "Organisation"), "IS_LOCATED_IN", Label("City", "Place")) -> StructType(Seq(StructField("University_id", StringType, false), StructField("City_id", StringType, false))),
    RelType(Label("Company", "Organisation"), "IS_LOCATED_IN", Label("Country", "Place")) -> StructType(Seq(StructField("Company_id", StringType, false), StructField("Country_id", StringType, false))),
    RelType("Person", "IS_LOCATED_IN", "Place") -> StructType(Seq(StructField("Person_id", StringType, false), StructField("Place_id", StringType, false))),
    RelType(Label("Person"), "IS_LOCATED_IN", Label("City", "Place")) -> StructType(Seq(StructField("Person_id", StringType, false), StructField("City_id", StringType, false))),
    RelType("TagClass", "IS_SUBCLASS_OF", "TagClass") -> StructType(Seq(StructField("TagClass_id0", StringType, false), StructField("TagClass_id1", StringType, false)))
  )

  val relDictionary: Map[RelType, Int] = relMapping.zipWithIndex.map { case ((relType, _), i) => relType -> i }

  val propertyMapping = Map(
    Property("Person", "email") -> StructType(Seq(StructField("Person_id", StringType, false), StructField("email", StringType, true))),
    Property("Person", "speaks") -> StructType(Seq(StructField("Person_id", StringType, false), StructField("speaks", StringType, true)))
  )
}
