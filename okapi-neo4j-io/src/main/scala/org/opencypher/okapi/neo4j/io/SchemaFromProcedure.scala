package org.opencypher.okapi.neo4j.io

import org.apache.logging.log4j.scala.Logging
import org.neo4j.driver.v1.Value
import org.neo4j.driver.v1.util.Function
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{CTVoid, CypherType}
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.neo4j.io.Neo4jHelpers._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object SchemaFromProcedure extends Logging {

  val schemaProcedureName = "org.opencypher.okapi.procedures.schema"

  def apply(config: Neo4jConfig, omitImportFailures: Boolean): Option[Schema] = {
    Try {
      config.cypher("CALL dbms.procedures() YIELD name AS name").exists { map =>
        map("name").value == schemaProcedureName
      }
    } match {
      case Success(true) =>
        schemaFromProcedure(config, omitImportFailures)
      case Success(false) =>
        logger.warn("Neo4j schema procedure not activated. Consider activating the procedure `" + schemaProcedureName + "`.")
        None
      case Failure(error) =>
        logger.error(s"Retrieving the procedure list from the Neo4j database failed: $error")
        None
    }
  }

  private[neo4j] def schemaFromProcedure(config: Neo4jConfig, omitImportFailures: Boolean): Option[Schema] = {
    Try {
      val rows = config.withSession { session =>
        val result = session.run("CALL " + schemaProcedureName)

        result.list().asScala.flatMap { row =>
          val extractString = new Function[Value, String] {
            override def apply(v1: Value): String = v1.asString()
          }

          val typ = row.get("type").asString()
          val labels = row.get("nodeLabelsOrRelType").asList(extractString).asScala.toList

          row.get("property").asString() match {
            case "" => // this label/type has no properties
              Seq((typ, labels, None, None))
            case property =>
              val typeStrings = row.get("cypherTypes").asList(extractString).asScala.toList
              val cypherType: CypherType = typeStrings
                .flatMap { s => CypherType.fromName(s) match {
                  case v@ Some(_) => v
                  case None if omitImportFailures  =>
                    logger.warn(s"At least one $typ with labels ${labels.mkString(",")} has unsupported property type $s for property $property")
                    None
                  case None => throw UnsupportedOperationException(
                    s"At least one $typ with labels ${labels.mkString(",")} has unsupported property type $s for property $property"
                  )
                }}
                .foldLeft(CTVoid: CypherType)(_ join _)

              Seq((typ, labels, Some(property), Some(cypherType)))
          }
        }
      }

      rows.groupBy(row => row._1 -> row._2).map {
        case ((typ, labels), tuples) =>
          val properties = tuples.collect {
            case (_, _, p, t) if p.nonEmpty => p.get -> t.get
          }

          typ match {
            case "Node" =>
              Schema.empty.withNodePropertyKeys(labels: _*)(properties: _*)
            case "Relationship" =>
              Schema.empty.withRelationshipPropertyKeys(labels.headOption.getOrElse(""))(properties: _*)
          }
      }.foldLeft(Schema.empty)(_ ++ _)
    } match {
      case Success(schema) => Some(schema)
      case Failure(error) =>
        logger.error(s"Could not load schema from Neo4j: ${error.getMessage}")
        None
    }
  }

}