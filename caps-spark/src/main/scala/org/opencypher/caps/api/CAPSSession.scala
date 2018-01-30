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
package org.opencypher.caps.api

import java.util.{ServiceLoader, UUID}

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.opencypher.caps.api.SparkUtils.cypherTypeForColumn
import org.opencypher.caps.api.exception.IllegalArgumentException
import org.opencypher.caps.api.graph.{CypherSession, PropertyGraph}
import org.opencypher.caps.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.caps.api.schema.{Node, Relationship, Schema}
import org.opencypher.caps.api.types._
import org.opencypher.caps.demo.CypherKryoRegistrar
import org.opencypher.caps.impl.spark._
import org.opencypher.caps.impl.spark.convert.fromSparkType
import org.opencypher.caps.impl.spark.io.{CAPSGraphSourceHandler, CAPSPropertyGraphDataSourceFactory}
import org.opencypher.caps.impl.util.Annotation

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._

object SparkUtils {

  def cypherTypeForColumn(df: DataFrame, columnName: String): CypherType = {
    val structField = sparkFieldForColumn(df, columnName)
    fromSparkType(structField.dataType, structField.nullable) match {
      case Some(cypherType) => cypherType
      case None => throw IllegalArgumentException("a supported Spark DataType that can be converted to CypherType", structField.dataType)
    }
  }

  def sparkFieldForColumn(df: DataFrame, column: String): StructField = {
    if (df.schema.fieldIndex(column) < 0) {
      throw IllegalArgumentException(s"column with name $column", s"columns with names ${df.columns.mkString("[", ", ", "]")}")
    }
    df.schema.fields(df.schema.fieldIndex(column))
  }
}

sealed trait EntityTable {
  def schema: Schema

  def table: DataFrame

  // TODO: create CTEntity type
  private[caps] def entityType: CypherType with DefiniteCypherType

  private[caps] def records(implicit caps: CAPSSession): CAPSRecords = CAPSRecords.create(this)

}

// TODO: Move where they belong
case class NodeTable(mapping: NodeMapping, table: DataFrame)(implicit session: CAPSSession) extends EntityTable {
  override val schema: Schema = {
    // TODO: validate that optional label columns have structfield datatype boolean

    val propertyKeys = mapping.propertyMapping.toSeq.map {
      case (propertyKey, sourceKey) => propertyKey -> cypherTypeForColumn(table, sourceKey)
    }

    mapping.optionalLabelMapping.values.toSet.subsets
      .map(_.union(mapping.impliedLabels))
      .map(combo => Schema.empty.withNodePropertyKeys(combo.toSeq: _*)(propertyKeys: _*))
      .reduce(_ ++ _)
  }

  override private[caps] def entityType: CTNode = CTNode(schema.labels)

}

object NodeTable {

  private val nodeIdColumnName = "id"

  def apply[E <: Node : TypeTag](nodes: Seq[E])(implicit caps: CAPSSession): NodeTable = {
    val nodeLabels = Annotation.labels[E]
    val nodeDF = caps.sparkSession.createDataFrame(nodes)
    val nodeProperties = properties(nodeDF.columns)
    val nodeMapping = NodeMapping.create(nodeIdKey = nodeIdColumnName, impliedLabels = nodeLabels, propertyKeys = nodeProperties)
    NodeTable(nodeMapping, nodeDF)
  }

  private def properties(nodeColumnNames: Seq[String]): Set[String] = {
    nodeColumnNames.filter(_ != nodeIdColumnName).toSet
  }

  // TODO: validate record schema vs mapping
  private[caps] def apply(mapping: NodeMapping, records: CAPSRecords): NodeTable =
    NodeTable(mapping, records.data)(records.caps)
}

case class RelationshipTable(mapping: RelationshipMapping, table: DataFrame)(implicit session: CAPSSession) extends EntityTable {
  override private[caps] def entityType: CTRelationship = CTRelationship(schema.relationshipTypes)

  override def schema: Schema = {
    val relTypes = mapping.relTypeOrSourceRelTypeKey match {
      case Left(name) => Set(name)
      case Right((_, possibleTypes)) => possibleTypes
    }

    val propertyKeys = mapping.propertyMapping.toSeq.map {
      case (propertyKey, sourceKey) => propertyKey -> cypherTypeForColumn(table, sourceKey)
    }

    relTypes.foldLeft(Schema.empty) {
      case (schema, relType) => schema.withRelationshipPropertyKeys(relType)(propertyKeys: _*)
    }
  }

}

object RelationshipTable {
  private val relationshipIdColumnName = "id"
  private val relationshipSourceColumnName = "source"
  private val relationshipTargetColumnName = "target"
  private val nonPropertyAttributes =
    Set(relationshipIdColumnName, relationshipSourceColumnName, relationshipTargetColumnName)

  def apply[E <: Relationship : TypeTag](relationships: Seq[E])(implicit caps: CAPSSession): RelationshipTable = {
    val relationshipType: String = Annotation.relType[E]
    val relationshipDF = caps.sparkSession.createDataFrame(relationships)
    val relationshipProperties = properties(relationshipDF.columns.toSet)

    val relationshipMapping = RelationshipMapping.create(relationshipIdColumnName,
      relationshipSourceColumnName,
      relationshipTargetColumnName,
      relationshipType,
      relationshipProperties)

    RelationshipTable(relationshipMapping, relationshipDF)
  }

  private def properties(relColumnNames: Set[String]): Set[String] = {
    relColumnNames.filter(!nonPropertyAttributes.contains(_))
  }

  private[caps] def apply(mapping: RelationshipMapping, records: CAPSRecords): RelationshipTable =
    RelationshipTable(mapping, records.data)(records.caps)
}

trait CAPSSession extends CypherSession {

  def sparkSession: SparkSession

  /**
    * Reads a graph from sequences of nodes and relationships.
    *
    * @param nodes         sequence of nodes
    * @param relationships sequence of relationships
    * @tparam N node type implementing [[org.opencypher.caps.api.schema.Node]]
    * @tparam R relationship type implementing [[org.opencypher.caps.api.schema.Relationship]]
    * @return graph defined by the sequences
    */
  def readFrom[N <: Node : TypeTag, R <: Relationship : TypeTag](
    nodes: Seq[N],
    relationships: Seq[R] = Seq.empty): PropertyGraph = {
    implicit val session: CAPSSession = this
    CAPSGraph.create(NodeTable(nodes), RelationshipTable(relationships))
  }

  /**
    * Reads a graph from a sequence of entity tables and expects, that the first table is a node table.
    *
    * @param entityTables sequence of node and relationship tables defining the graph
    * @return property graph
    */
  def readFrom(entityTables: EntityTable*): PropertyGraph = entityTables.head match {
    case h: NodeTable => readFrom(h, entityTables.tail: _*)
    case _ => throw IllegalArgumentException("first argument of type NodeTable", "RelationshipTable")
  }

  /**
    * Reads a graph from a sequence of entity tables that contains at least one node table.
    *
    * @param nodeTable    first parameter to guarantee there is at least one node table
    * @param entityTables sequence of node and relationship tables defining the graph
    * @return property graph
    */
  def readFrom(nodeTable: NodeTable, entityTables: EntityTable*): PropertyGraph = {
    CAPSGraph.create(nodeTable, entityTables: _*)(this)
  }
}

object CAPSSession extends Serializable {

  /**
    * Creates a new CAPSSession that wraps a local Spark session with CAPS default parameters.
    */
  def local(): CAPSSession = {
    val conf = new SparkConf(true)
    conf.set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
    conf.set("spark.kryo.registrator", classOf[CypherKryoRegistrar].getCanonicalName)
    conf.set("spark.sql.codegen.wholeStage", "true")
    conf.set("spark.kryo.unsafe", "true")
    conf.set("spark.kryo.referenceTracking", "false")
    conf.set("spark.kryo.registrationRequired", "true")

    val session = SparkSession
      .builder()
      .config(conf)
      .master("local[*]")
      .appName(s"caps-local-${UUID.randomUUID()}")
      .getOrCreate()
    session.sparkContext.setLogLevel("error")

    create(session)
  }

  def create(implicit session: SparkSession): CAPSSession = Builder(session).build

  def builder(sparkSession: SparkSession): Builder = Builder(sparkSession)

  case class Builder(
    session: SparkSession,
    private val graphSourceFactories: Set[CAPSPropertyGraphDataSourceFactory] = Set.empty) {

    def withGraphSourceFactory(factory: CAPSPropertyGraphDataSourceFactory): Builder =
      copy(graphSourceFactories = graphSourceFactories + factory)

    def build: CAPSSession = {
      val discoveredFactories = ServiceLoader.load(classOf[CAPSPropertyGraphDataSourceFactory]).asScala.toSet
      val allFactories = graphSourceFactories ++ discoveredFactories

      new CAPSSessionImpl(
        session,
        CAPSGraphSourceHandler(allFactories)
      )
    }
  }
}
