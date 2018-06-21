package org.opencypher.spark.api

import org.opencypher.spark.api.io.fs.{CAPSFileSystem, FSGraphSource}
import org.opencypher.spark.api.io.neo4j.{Neo4jConfig, Neo4jReadOnlyNamedQueryGraphSource}

object GraphSources {
  def fs(
    rootPath: String,
    customFileSystem: Option[CAPSFileSystem] = None,
    filesPerTable: Option[Int] = Some(1)
  ) = FSGraphSources(rootPath, customFileSystem, filesPerTable)

  def cypher = CypherGraphSources
}

object FSGraphSources {
  def apply(
    rootPath: String,
    customFileSystem: Option[CAPSFileSystem] = None,
    filesPerTable: Option[Int] = Some(1)
  ): FSGraphSourceFactory = FSGraphSourceFactory(rootPath, customFileSystem, filesPerTable)

  case class FSGraphSourceFactory(
    rootPath: String,
    customFileSystem: Option[CAPSFileSystem] = None,
    filesPerTable: Option[Int] = Some(1)
  ) {

    def csv(implicit session: CAPSSession): FSGraphSource =
      new FSGraphSource(rootPath, "csv", customFileSystem, filesPerTable)
  }
}

object CypherGraphSources {
  def neo4jReadOnlyNamedQuery(config: Neo4jConfig)(implicit session: CAPSSession): Neo4jReadOnlyNamedQueryGraphSource =
    Neo4jReadOnlyNamedQueryGraphSource(config)
}
