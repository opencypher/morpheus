package org.opencypher.spark.api.io.sql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.{AbstractPropertyGraphDataSource, JdbcFormat, StorageFormat}
import org.opencypher.spark.api.io.metadata.CAPSGraphMetaData
import org.opencypher.spark.schema.CAPSSchema
import org.opencypher.sql.ddl.DdlDefinitions

case class SqlPropertyGraphDataSource(
  ddl: DdlDefinitions
)(
  override implicit val caps: CAPSSession
) extends AbstractPropertyGraphDataSource {

  override def tableStorageFormat: StorageFormat = JdbcFormat

  override protected def listGraphNames: List[String] = ddl.graphSchemas.keySet.toList

  override protected def deleteGraph(graphName: GraphName): Unit =
    unsupported("deleting of source data")

  override protected def readSchema(graphName: GraphName): CAPSSchema =
    CAPSSchema(
      ddl.graphSchemas.getOrElse(graphName.value, notFound(graphName.value, ddl.graphSchemas.keySet))
    )

  override protected def writeSchema(graphName: GraphName, schema: CAPSSchema): Unit = ???

  override protected def readCAPSGraphMetaData(graphName: GraphName): CAPSGraphMetaData = ???

  override protected def writeCAPSGraphMetaData(graphName: GraphName, capsGraphMetaData: CAPSGraphMetaData): Unit = ???

  override protected def readNodeTable(graphName: GraphName, labels: Set[String], sparkSchema: StructType): DataFrame = ???

  override protected def writeNodeTable(graphName: GraphName, labels: Set[String], table: DataFrame): Unit = ???

  override protected def readRelationshipTable(graphName: GraphName, relKey: String, sparkSchema: StructType): DataFrame = ???

  override protected def writeRelationshipTable(graphName: GraphName, relKey: String, table: DataFrame): Unit = ???


  private val className = getClass.getSimpleName

  private def unsupported(operation: String): Nothing =
    throw UnsupportedOperationException(s"$className does not allow $operation")

  private def notFound(needle: Any, haystack: Traversable[Any]): Nothing =
    throw IllegalArgumentException(
      expected = s"one of ${stringList(haystack)}",
      actual = needle
    )

  private def stringList(elems: Traversable[Any]): String =
    elems.mkString("[", ",", "]")


}
