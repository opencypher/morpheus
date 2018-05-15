package org.opencypher.spark.api.io.fs

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.AbstractDataSource
import org.opencypher.spark.api.io.fs.DefaultFileSystem._
import org.opencypher.spark.api.io.json.JsonSerialization

private[io] class FileBasedDataSource(
  rootPath: String,
  val tableStorageFormat: String,
  customFileSystem: Option[CAPSFileSystem] = None,
  filesPerTable: Option[Int] = Some(1)
)(implicit session: CAPSSession)
  extends AbstractDataSource with JsonSerialization {

  protected val directoryStructure = DefaultGraphDirectoryStructure(rootPath)

  import directoryStructure._

  protected lazy val fileSystem: CAPSFileSystem = customFileSystem.getOrElse(
    FileSystem.get(session.sparkSession.sparkContext.hadoopConfiguration))

  protected def listDirectories(path: String): List[String] = fileSystem.listDirectories(path)

  protected def deleteDirectory(path: String): Unit = fileSystem.deleteDirectory(path)

  protected def readFile(path: String): String = fileSystem.readFile(path)

  protected def writeFile(path: String, content: String): Unit = fileSystem.writeFile(path, content)

  protected def readTable(path: String, tableStorageFormat: String, schema: StructType): DataFrame = {
    session.sparkSession.read.format(tableStorageFormat).schema(schema).load(path)
  }

  protected def writeTable(path: String, tableStorageFormat: String, table: DataFrame): Unit = {
    val coalescedTable = filesPerTable match {
      case None => table
      case Some(numFiles) => table.coalesce(numFiles)
    }
    coalescedTable.write.format(tableStorageFormat).save(path)
  }

  override protected def listGraphNames: List[String] = {
    listDirectories(rootPath)
  }

  override protected def deleteGraph(graphName: GraphName): Unit = {
    deleteDirectory(pathToGraphToDirectory(graphName))
  }

  override protected def readNodeTable(graphName: GraphName, tableStorageFormat: String, labels: Set[String], sparkSchema: StructType): DataFrame = {
    readTable(pathToNodeTable(graphName, labels), tableStorageFormat, sparkSchema)
  }

  override protected def writeNodeTable(graphName: GraphName, tableStorageFormat: String, labels: Set[String], table: DataFrame): Unit = {
    writeTable(pathToNodeTable(graphName, labels), tableStorageFormat, table)
  }

  override protected def readRelationshipTable(graphName: GraphName, relKey: String, sparkSchema: StructType): DataFrame = {
    readTable(pathToRelationshipTable(graphName, relKey), tableStorageFormat: String, sparkSchema)
  }

  override protected def writeRelationshipTable(graphName: GraphName, relKey: String, table: DataFrame): Unit = {
    writeTable(pathToRelationshipTable(graphName, relKey), tableStorageFormat: String, table)
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
