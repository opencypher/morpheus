package org.opencypher.spark.api.io.util

import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.spark.api.io.fs.DefaultGraphDirectoryStructure._
import org.opencypher.okapi.impl.util.StringEncodingUtilities._

sealed abstract class HiveTableName(
  databaseName: String,
  graphName: GraphName,
  entityType: String,
  entityIdentifiers: Set[String]
) {

  def tableName: String = s"${graphName.path.replace('/', '_')}_${entityType}_${entityIdentifiers.toSeq.sorted.mkString("_")}".encodeSpecialCharacters

  override def toString: String =
    s"$databaseName.$tableName"
}

case class HiveNodeTableName(
  databaseName: String,
  graphName: GraphName,
  labels: Set[String]) extends HiveTableName(databaseName, graphName, "nodes", labels)

case class HiveRelationshipTableName(
  databaseName: String,
  graphName: GraphName,
  relType: String) extends HiveTableName(databaseName, graphName, "relationships", Set(relType))