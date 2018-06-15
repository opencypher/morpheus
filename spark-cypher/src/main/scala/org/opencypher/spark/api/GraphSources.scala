package org.opencypher.spark.api

import org.opencypher.spark.api.io.fs.FSGraphSource
import org.opencypher.spark.api.io.neo4j.{Neo4jConfig, Neo4jReadOnlyNamedQueryGraphSource}

object GraphSources {
  def fs = FSGraphSources
  def cypher = CypherGraphSources
}

object FSGraphSources {
  def csv(rootPath: String)(implicit session: CAPSSession): FSGraphSource =
    new FSGraphSource(rootPath, "csv")

}

object CypherGraphSources {
  def neo4jReadOnly(config: Neo4jConfig)(implicit session: CAPSSession): Neo4jReadOnlyNamedQueryGraphSource =
    Neo4jReadOnlyNamedQueryGraphSource(config)
}
