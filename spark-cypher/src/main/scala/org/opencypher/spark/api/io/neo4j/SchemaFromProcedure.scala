package org.opencypher.spark.api.io.neo4j

import org.neo4j.driver.v1.Value
import org.neo4j.driver.v1.util.Function
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.CypherType
import org.opencypher.spark.impl.io.neo4j.Neo4jHelpers._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object SchemaFromProcedure {

  val schemaProcedureName = "org.neo4j.morpheus.procedures.schema"

  def apply(config: Neo4jConfig): Option[Schema] = {
    Try {
      config.cypher("CALL dbms.procedures() YIELD name AS name").exists { map =>
        map("name").value == schemaProcedureName
      }
    } match {
      case Success(true) =>
        schemaFromProcedure(config)
      case Success(false) =>
        System.err.println("Neo4j schema procedure not activated. Consider activating the procedure `" + schemaProcedureName + "`.")
        None
      case Failure(error) =>
        System.err.println(s"Retrieving the procedure list from the Neo4j database failed: $error")
        None
    }
  }

  private[neo4j] def schemaFromProcedure(config: Neo4jConfig): Option[Schema] = {
    Try {
      val rows = config.execute { session =>
        val result = session.run("CALL " + schemaProcedureName)

        result.list().asScala.map { row =>
          val typ = row.get("type").asString()
          val labels = row.get("nodeLabelsOrRelType").asList(new Function[Value, String] {
            override def apply(v1: Value): String = v1.asString()
          }).asScala.toList

          row.get("property").asString() match {
            case "" => // this label/type has no properties
              (typ, labels, None, None)
            case property =>
              val typeString = row.get("cypherType").asString()
              val cypherType = CypherType.fromName(typeString)
              (typ, labels, Some(property), cypherType)
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
        System.err.println(s"Could not load schema from Neo4j: ${error.getMessage}")
        error.printStackTrace()
        None
    }
  }

}
