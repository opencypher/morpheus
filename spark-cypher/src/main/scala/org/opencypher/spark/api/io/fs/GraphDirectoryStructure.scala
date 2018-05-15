package org.opencypher.spark.api.io.fs

import org.opencypher.okapi.api.graph.GraphName

trait GraphDirectoryStructure {

  def dataSourceRootPath: String

  def pathToGraphToDirectory(graphName: GraphName): String

  def pathToGraphSchema(graphName: GraphName): String

  def pathToCAPSMetaData(graphName: GraphName): String

  def pathToNodeTable(graphName: GraphName, labels: Set[String]): String

  def pathToRelationshipTable(graphName: GraphName, relKey: String): String

}

object DefaultGraphDirectoryStructure {

  implicit class StringPath(val path: String) extends AnyVal {
    def /(segment: String): String = s"$path$pathSeparator$segment"
  }

  implicit class GraphPath(graphName: GraphName) {
    def path: String = graphName.value.replace(".", pathSeparator)
  }

  val pathSeparator = "/"

}

case class DefaultGraphDirectoryStructure(dataSourceRootPath: String) extends GraphDirectoryStructure {

  import DefaultGraphDirectoryStructure._

  override def pathToGraphToDirectory(graphName: GraphName): String = {
    dataSourceRootPath / graphName.path
  }

  override def pathToGraphSchema(graphName: GraphName): String = {
    dataSourceRootPath / graphName.path / "schema.json"
  }

  override def pathToCAPSMetaData(graphName: GraphName): String = {
    dataSourceRootPath / graphName.path / "capsGraphMetaData.json"
  }

  override def pathToNodeTable(graphName: GraphName, labels: Set[String]): String = {
    dataSourceRootPath / graphName.path / "nodes" / labels.toSeq.sorted.mkString("_")
  }

  override def pathToRelationshipTable(graphName: GraphName, relKey: String): String = {
    dataSourceRootPath / graphName.path / "relationships" / relKey
  }

}
