package org.opencypher.spark.testing.support.creation

import org.neo4j.harness.ServerControls
import org.neo4j.kernel.builtinprocs.SchemaProcedure
import org.opencypher.okapi.neo4j.io.testing.Neo4jHarnessUtils._

object CAPSNeo4jHarnessUtils {
  implicit class CAPSServerControls(val neo4j: ServerControls) extends AnyVal {
    def withSchemaProcedure = neo4j.withProcedure(classOf[SchemaProcedure])
  }
}
