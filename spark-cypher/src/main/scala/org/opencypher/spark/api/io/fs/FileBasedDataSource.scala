package org.opencypher.spark.api.io.fs

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.AbstractDataSource
import org.opencypher.spark.api.io.json.JsonSerialization
import org.opencypher.spark.api.io.util.FileSystemUtils._

private[io] class FileBasedDataSource(rootPath: String, val tableStorageFormat: String)(implicit session: CAPSSession)
  extends AbstractDataSource with JsonSerialization {

  protected val directoryStructure = DefaultGraphDirectoryStructure(rootPath)

  import directoryStructure._

  protected lazy val hadoopConfig = session.sparkSession.sparkContext.hadoopConfiguration

  protected lazy val fileSystem = FileSystem.get(hadoopConfig)

  protected def listDirectories(path: String): List[String] = {
    fileSystem.listStatus(new Path(path))
      .filter(_.isDirectory)
      .map(_.getPath.getName)
      .toList
  }

  protected def deleteDirectory(path: String): Unit = {
    fileSystem.delete(new Path(path), /* recursive = */ true)
  }

  protected def readFile(path: String): String = {
    using(new BufferedReader(new InputStreamReader(fileSystem.open(new Path(path)), "UTF-8"))) { reader =>
      def readLines = Stream.cons(reader.readLine(), Stream.continually(reader.readLine))
      readLines.takeWhile(_ != null).mkString
    }
  }

  protected def writeFile(path: String, content: String): Unit = {
    using(fileSystem.create(new Path(path))) { outputStream =>
      using(new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"))) { bufferedWriter =>
        bufferedWriter.write(content)
      }
    }
  }

  protected def readTable(path: String, tableStorageFormat: String, schema: StructType): DataFrame = {
    session.sparkSession.read.format(tableStorageFormat).schema(schema).load(path)
  }

  protected def writeTable(path: String, tableStorageFormat: String, table: DataFrame): Unit = {
    table.write.format(tableStorageFormat).save(path)
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
