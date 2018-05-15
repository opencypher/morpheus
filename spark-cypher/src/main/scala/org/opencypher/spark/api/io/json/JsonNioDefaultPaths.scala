package org.opencypher.spark.api.io.json

import java.nio.file.{Path => NioPath}

import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.spark.api.io.fs.GraphDirectoryStructure
import org.opencypher.spark.api.io.json.JsonNioDefaultPaths._

object JsonNioDefaultPaths {

  implicit class RichPath(val path: NioPath) extends AnyVal {
    def /(segment: String): NioPath = path.resolve(segment)
  }

}

trait JsonNioDefaultPaths extends GraphDirectoryStructure[NioPath] {

  override def pathToGraphToDirectory(graphName: GraphName): NioPath = {
    rootPath / graphName.value
  }

  override def pathToGraphSchema(graphName: GraphName): NioPath = {
    rootPath / graphName.value / "schema.json"
  }

  override def pathToCAPSMetaData(graphName: GraphName): NioPath = {
    rootPath / graphName.value / "capsGraphMetaData.json"
  }

  override def pathToNodeTable(graphName: GraphName, labels: Set[String]): NioPath = {
    rootPath / graphName.value / "nodes" / labels.toSeq.sorted.mkString("_")
  }

  override def pathToRelationshipTable(graphName: GraphName, relKey: String): NioPath = {
    rootPath / graphName.value / "relationships" / relKey
  }

}
