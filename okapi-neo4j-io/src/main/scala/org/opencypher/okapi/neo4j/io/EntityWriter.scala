package org.opencypher.okapi.neo4j.io

import org.apache.logging.log4j.scala.Logging
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherNode, CypherRelationship}
import org.opencypher.okapi.neo4j.io.Neo4jHelpers.Neo4jDefaults._
import org.opencypher.okapi.neo4j.io.Neo4jHelpers._

object EntityWriter extends Logging {

  def writeNodes[Node <: CypherNode[_]](nodes: Iterator[Node], config: Neo4jConfig, metaLabel: Option[String] = None): Unit = {
    val queryStringBuilder = new StringBuilder()
    config.withSession { session =>
      nodes.grouped(1000).foreach { nodes =>
        queryStringBuilder.clear()

        nodes.foreach { node =>
          val labels = if (metaLabel.isDefined) node.labels + metaLabel.get else node.labels

          val properties = node.properties.updated(metaPropertyKey, CypherValue(node.id))
          val propertyString = properties.toCypherString

          queryStringBuilder ++= "CREATE (" ++= labels.mkString(":`", "`:`", "`") ++= propertyString ++= ")\n"
        }
        val queryString = queryStringBuilder.result()
        logger.debug(s"Executing query on Neo4j: ${queryString.replaceAll("\n", " ")}")
        session.run(queryString).consume()
      }
    }
  }

  def writeRelationships[Rel <: CypherRelationship[_]](
    relIterator: Iterator[Rel],
    config: Neo4jConfig,
    metaLabel: Option[String] = None
  ): Unit = {

    val queryStringBuilder = new StringBuilder()

    val graphLabelString = metaLabel.mkString(":", "", "")
    val matchString = s"MATCH (a$graphLabelString), (b$graphLabelString)"
    val seperatorString = "\nWITH 1 as __sep__\n"

    config.withSession { session =>

      relIterator.grouped(1000).foreach { rels =>
        queryStringBuilder.clear()

        rels.foreach { rel =>
          val properties = rel.properties.updated(metaPropertyKey, CypherValue(rel.id))
          val propertyString = properties.toCypherString

          queryStringBuilder ++=
            matchString ++=
            "\nWHERE a." ++= metaPropertyKey ++= "=" ++= rel.startId.toString ++= " AND b." ++= metaPropertyKey ++= "=" ++= rel.endId.toString ++=
            "\nCREATE (a)-[r:" ++= rel.relType ++= " " ++= propertyString ++= "]->(b)" ++=
            seperatorString
        }

        // remove the last separator
        val stringLength = queryStringBuilder.size
        val queryString = queryStringBuilder.substring(0, stringLength - seperatorString.length)

        logger.debug(s"Executing query on Neo4j: ${queryString.replaceAll("\n", " ")}")
        session.run(queryString).consume()
      }
    }
  }
}
