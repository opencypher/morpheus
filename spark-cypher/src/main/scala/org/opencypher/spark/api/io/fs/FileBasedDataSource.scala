package org.opencypher.spark.api.io.fs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.AbstractDataSource
import org.opencypher.spark.api.io.json.JsonSerialization

abstract class FileBasedDataSource[PathType](implicit session: CAPSSession)
  extends AbstractDataSource with JsonSerialization with GraphDirectoryStructure[PathType] {

  protected def listDirectories(path: PathType): List[String]

  protected def deleteDirectory(path: PathType): Unit

  protected def readFile(path: PathType): String

  protected def writeFile(path: PathType, content: String): Unit

  protected def readTable(path: PathType, schema: StructType): DataFrame

  protected def writeTable(path: PathType, table: DataFrame): Unit

  override protected def listGraphNames: List[String] = {
    listDirectories(rootPath)
  }

  override protected def deleteGraph(graphName: GraphName): Unit = {
    deleteDirectory(pathToGraphToDirectory(graphName))
  }

  override protected def readNodeTable(graphName: GraphName, labels: Set[String], sparkSchema: StructType): DataFrame = {
    readTable(pathToNodeTable(graphName, labels), sparkSchema)
  }

  override protected def writeNodeTable(graphName: GraphName, labels: Set[String], table: DataFrame): Unit = {
    writeTable(pathToNodeTable(graphName, labels), table)
  }

  override protected def readRelationshipTable(graphName: GraphName, relKey: String, sparkSchema: StructType): DataFrame = {
    readTable(pathToRelationshipTable(graphName, relKey), sparkSchema)
  }

  override protected def writeRelationshipTable(graphName: GraphName, relKey: String, table: DataFrame): Unit = {
    writeTable(pathToRelationshipTable(graphName, relKey), table)
  }

  override protected def readJsonSchema(graphName: GraphName): String = {
    readFile(pathToGraphSchema(graphName))
  }

  override protected def writeJsonSchema(graphName: GraphName, schema: String): Unit = {
    writeFile(pathToGraphSchema(graphName), schema)
  }

  override protected def readJsonCAPSGraphMetaData(graphName: GraphName): String = {
    readFile(pathToCAPSMetaData(graphName))
  }

  override protected def writeJsonCAPSGraphMetaData(graphName: GraphName, capsGraphMetaData: String): Unit = {
    writeFile(pathToCAPSMetaData(graphName), capsGraphMetaData)
  }

}
