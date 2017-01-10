package org.opencypher.spark.impl

import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.parser.CypherParser

object Frontend {

  val parser = new CypherParser

  def parse(query: String): Statement =
    parser.parse(query)
}




