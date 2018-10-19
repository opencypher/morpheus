package org.opencypher.spark.api.io.util

import org.opencypher.okapi.api.graph.{GraphEntityType, GraphName}
import org.opencypher.okapi.impl.util.StringEncodingUtilities._
import org.opencypher.spark.api.io.fs.DefaultGraphDirectoryStructure._

case object HiveTableName {

  def apply(databaseName: String,
    graphName: GraphName,
    entityType: GraphEntityType,
    entityIdentifiers: Set[String]): String = {

    val entityString = entityType.name.toLowerCase

    val tableName = s"${graphName.path.replace('/', '_')}_${entityString}_${entityIdentifiers.toSeq.sorted.mkString("_")}".encodeSpecialCharacters
    s"$databaseName.$tableName"
  }

}
