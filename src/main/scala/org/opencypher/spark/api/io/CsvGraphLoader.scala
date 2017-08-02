package org.opencypher.spark.api.io

import java.io.File
import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.opencypher.spark.api.record.{NodeScan, RelationshipScan}
import org.opencypher.spark.api.spark.{SparkCypherGraph, SparkCypherRecords, SparkGraphSpace}

/**
  * Loads a graph stored in indexed CSV format from HDFS or the local file system
  * The CSV files must be stored following this schema:
  * # Nodes
  *   - all files describing nodes are stored in a subfolder called "nodes"
  *   - create one csv file per disjunct label set (e.g. one file for all nodes labeled with :Person:Employee and one
  *     file for all nodes only labeled :Person)
  *   - for every node csv file create a schema file called FILE_NAME.csv.SCHEMA
  *   - for information about the structure of the node schema file see [[CsvNodeSchema]]
  * # Relationships
  *   - all files describing nodes are stored in a subfolder called "relationships"
  *   - create one csv file per relationship type
  *   - for every relationship csv file create a schema file called FILE_NAME.csv.SCHEMA
  *   - for information about the structure of the relationship schema file see [[CsvRelSchema]]

  *
  * @param location Location of the top level folder containing the node and relationship files
  * @param graphSpace
  * @param sc
  */
class CsvGraphLoader(location: String)(implicit graphSpace: SparkGraphSpace, sc: SparkSession) {
  private val fs: FileSystem = FileSystem.get(new URI(location), sc.sparkContext.hadoopConfiguration)

  def load: SparkCypherGraph= {
    val nodeScans = loadNodes
    val relScans = loadRels
    SparkCypherGraph.create(nodeScans.head, nodeScans.tail ++ relScans: _*)
  }

  private def loadNodes: Array[NodeScan] = {
    val nodeLocation = s"$location${File.separator}nodes"
    val entries = listEntries(nodeLocation)

    entries.map(e => {
      val schema = readSchema(e)(CsvNodeSchema(_))

      val records = SparkCypherRecords.create(
        sc.read
        .schema(schema.toStructType)
        .csv(e.toUri.toString)
      )

      NodeScan.on("n" -> schema.idField.name)(builder => {
        val withImpliedLabels = schema.implicitLabels.foldLeft(builder.build)(_ withImpliedLabel _)
        val withOptionalLabels = schema.optionalLabels.foldLeft(withImpliedLabels)((a, b) => {
          a.withOptionalLabel(b.name, b.name)
        })
        schema.propertyFields.foldLeft(withOptionalLabels)((builder, field) => {
          builder.withPropertyKey(field.name -> field.name)
        })
      }).from(records)
    })
  }

  private def loadRels: Array[RelationshipScan] = {
    val relLocation = s"$location${File.separator}relationships"
    val entries = listEntries(relLocation)

    entries.map(e => {
      val schema: CsvRelSchema = readSchema[CsvRelSchema](e)(CsvRelSchema(_))

      val records = SparkCypherRecords.create(
        sc.read
          .schema(schema.toStructType)
          .csv(e.toUri.toString)
      )

      RelationshipScan.on("r" -> schema.idField.name)(builder => {
        val baseBuilder = builder
          .from(schema.startIdField.name)
          .to(schema.endIdField.name)
          .relType(schema.relType)
            .build

        schema.propertyFields.foldLeft(baseBuilder)((builder, field) => {
          builder.withPropertyKey(field.name -> field.name)
        })
      }).from(records)
    })
  }

  private def listEntries(directory: String): Array[Path] = {
    fs.listStatus(new Path(directory))
      .filterNot(_.getPath.toString.endsWith(".SCHEMA"))
      .map(_.getPath)
  }

  private def readSchema[T <: CsvSchema](path: Path)(parser: String => T): T = {
    val schemaPath = path.suffix(".SCHEMA")
    val stream = fs.open(schemaPath)
    def readLines = Stream.cons(stream.readLine(), Stream.continually( stream.readLine))
    parser(readLines.takeWhile(_ != null).mkString)
  }
}
