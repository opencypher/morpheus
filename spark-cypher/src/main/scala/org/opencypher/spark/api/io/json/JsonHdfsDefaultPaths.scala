package org.opencypher.spark.api.io.json

import java.nio.file.{Paths, Path => NioPath}

import org.apache.hadoop.fs.{Path => HDFSPath}
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.spark.api.io.fs.GraphDirectoryStructure
import org.opencypher.spark.api.io.json.JsonHdfsDefaultPaths._

object JsonHdfsDefaultPaths {

  implicit class RichNioPath(val nioPath: NioPath) extends AnyVal {
    def toHDFS: HDFSPath = new HDFSPath(nioPath.toString)
  }

}

trait JsonHdfsDefaultPaths extends GraphDirectoryStructure[HDFSPath] {
  self =>

  def rootPath: HDFSPath

  private lazy val nioPaths: JsonNioDefaultPaths = new JsonNioDefaultPaths {
    override def rootPath: NioPath = Paths.get(self.rootPath.toString)
  }

  def pathToGraphToDirectory(graphName: GraphName): HDFSPath = {
    nioPaths.pathToGraphToDirectory(graphName).toHDFS
  }

  override def pathToGraphSchema(graphName: GraphName): HDFSPath = {
      nioPaths.pathToGraphSchema(graphName).toHDFS
  }

  override def pathToCAPSMetaData(graphName: GraphName): HDFSPath = {
    nioPaths.pathToCAPSMetaData(graphName).toHDFS
  }

  override def pathToNodeTable(graphName: GraphName, labels: Set[String]): HDFSPath = {
    nioPaths.pathToNodeTable(graphName, labels).toHDFS
  }

  override def pathToRelationshipTable(graphName: GraphName, relKey: String): HDFSPath = {
    nioPaths.pathToRelationshipTable(graphName, relKey).toHDFS
  }

}
