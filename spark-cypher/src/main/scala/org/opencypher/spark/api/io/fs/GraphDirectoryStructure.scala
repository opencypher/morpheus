package org.opencypher.spark.api.io.fs

import org.opencypher.okapi.api.graph.GraphName

trait GraphDirectoryStructure[PathType] {

  def rootPath: PathType

  def pathToGraphToDirectory(graphName: GraphName): PathType

  def pathToGraphSchema(graphName: GraphName): PathType

  def pathToCAPSMetaData(graphName: GraphName): PathType

  def pathToNodeTable(graphName: GraphName, labels: Set[String]): PathType

  def pathToRelationshipTable(graphName: GraphName, relKey: String): PathType

}
