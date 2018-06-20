package org.opencypher.spark.impl.io.neo4j

import org.neo4j.driver.v1.Session
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.CypherValue
import org.opencypher.spark.api.io.neo4j.Neo4jConfig

import scala.collection.JavaConverters._

object Neo4jHelpers {
  implicit class RichConfig(val config: Neo4jConfig) extends AnyVal {

    def execute[T](f: Session => T): T = {
      val session = config.driver().session
      val t = f(session)
      session.close()
      t
    }

    def cypher(query: String): List[Map[String, CypherValue]] = {
      execute { session =>
        session.run(query).list().asScala.map(_.asMap().asScala.mapValues(CypherValue(_)).toMap).toList
      }
    }
  }
}
