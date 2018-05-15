package org.opencypher.spark.api.io

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.impl.exception.{GraphAlreadyExistsException, GraphNotFoundException}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.metadata.CAPSGraphMetaData
import org.opencypher.spark.api.io.util.CAPSGraphExport._
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.impl.CAPSGraph
import org.opencypher.spark.impl.io.CAPSPropertyGraphDataSource
import org.opencypher.spark.schema.CAPSSchema

import scala.util.Try

abstract class AbstractDataSource(implicit session: CAPSSession) extends CAPSPropertyGraphDataSource {

  def tableStorageFormat: String

  protected var schemaCache: Map[GraphName, CAPSSchema] = Map.empty

  protected var graphNameCache: Set[GraphName] = listGraphNames.map(GraphName).toSet

  protected def listGraphNames: List[String]

  protected def deleteGraph(graphName: GraphName): Unit

  protected def readSchema(graphName: GraphName): CAPSSchema

  protected def writeSchema(graphName: GraphName, schema: CAPSSchema): Unit

  protected def readCAPSGraphMetaData(graphName: GraphName): CAPSGraphMetaData

  protected def writeCAPSGraphMetaData(graphName: GraphName, capsGraphMetaData: CAPSGraphMetaData): Unit

  protected def readNodeTable(graphName: GraphName, tableStorageFormat: String, labels: Set[String], sparkSchema: StructType): DataFrame

  protected def writeNodeTable(graphName: GraphName, tableStorageFormat: String, labels: Set[String], table: DataFrame): Unit

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
    Try {
      val capsSchema: CAPSSchema = schema(graphName).get
      val capsMetaData: CAPSGraphMetaData = readCAPSGraphMetaData(graphName)
      val nodeTables = capsSchema.allLabelCombinations.map { combo =>
        val nonNullableProperties = capsSchema.keysFor(Set(combo)).filterNot {
          case (_, cypherType) => cypherType.isNullable
        }.keySet
        val nonNullableColumns = nonNullableProperties + GraphEntity.sourceIdKey
        val df = readNodeTable(graphName, capsMetaData.tableStorageFormat, combo, capsSchema.canonicalNodeTableSchema(combo))
        CAPSNodeTable(combo, df.setNonNullable(nonNullableColumns))
      }

      val relTables = capsSchema.relationshipTypes.map { relType =>
        val nonNullableProperties = capsSchema.relationshipKeys(relType).filterNot {
          case (_, cypherType) => cypherType.isNullable
        }.keySet
        val nonNullableColumns = nonNullableProperties ++ Relationship.nonPropertyAttributes
        val df = readRelationshipTable(graphName, relType, capsSchema.canonicalRelTableSchema(relType))
        CAPSRelationshipTable(relType, df.setNonNullable(nonNullableColumns))
      }
      CAPSGraph.create(capsMetaData.tags, nodeTables.head, (nodeTables.tail ++ relTables).toSeq: _*)
    }.toOption.getOrElse(throw GraphNotFoundException(s"Graph with name '$graphName'"))
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
    if (hasGraph(graphName)) {
      throw GraphAlreadyExistsException(s"A graph with name $graphName is already stored in this graph data source.")
    }

    val capsGraph = graph.asCaps
    val schema = capsGraph.schema
    schemaCache += graphName -> schema
    graphNameCache += graphName
    writeCAPSGraphMetaData(graphName, CAPSGraphMetaData(tableStorageFormat, capsGraph.tags))
    writeSchema(graphName, schema)

    schema.labelCombinations.combos.foreach { combo =>
      writeNodeTable(graphName, tableStorageFormat, combo, capsGraph.canonicalNodeTable(combo))
    }

    schema.relationshipTypes.foreach { relType =>
      writeRelationshipTable(graphName, relType, capsGraph.canonicalRelationshipTable(relType))
    }
  }

}
