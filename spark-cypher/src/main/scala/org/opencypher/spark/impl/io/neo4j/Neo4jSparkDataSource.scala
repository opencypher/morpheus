package org.opencypher.spark.impl.io.neo4j

import java.net.URI
import java.util

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupportWithSchema}
import org.apache.spark.sql.types.{StructField, StructType}
import org.opencypher.okapi.api.types.{EntityType, Node, Relationship}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.impl.exception._
import org.opencypher.okapi.neo4j.io.Neo4jConfig
import org.opencypher.okapi.neo4j.io.Neo4jHelpers.Neo4jDefaults._
import org.opencypher.okapi.neo4j.io.Neo4jHelpers._
import org.opencypher.spark.impl.io.neo4j.Neo4jSparkDataSource._

import scala.tools.nsc.interpreter.JList

object Neo4jSparkDataSource {
  implicit class RichFilter(filter: Filter) {
    def toNeo4jFilter: String = filter match {
      case EqualTo(attribute, value) => toBinaryString(attribute, value, "=")
      case LessThan(attribute, value) => toBinaryString(attribute, value, "<")
      case LessThanOrEqual(attribute, value) => toBinaryString(attribute, value, "<=")
      case GreaterThan(attribute, value) => toBinaryString(attribute, value, ">")
      case GreaterThanOrEqual(attribute, value) => toBinaryString(attribute, value, ">=")
      case In(attribute, values) => toBinaryString(attribute, values, "IN")
      case IsNull(attribute) => s"$attribute IS NULL"
      case IsNotNull(attribute) => s"$attribute IS NOT NULL"
      case StringStartsWith(attribute, prefix) => toBinaryString(attribute, prefix, "STARTS WITH")
      case StringEndsWith(attribute, suffix) => toBinaryString(attribute, suffix, "ENDS WITH")
      case StringContains(attribute, substring) => toBinaryString(attribute, substring, "CONTAINS")
      case And(left, right) => s"(${left.toNeo4jFilter} AND ${right.toNeo4jFilter})"
      case Or(left, right) => s"(${left.toNeo4jFilter} OR ${right.toNeo4jFilter})"
      case Not(inner) => s"NOT (${inner.toNeo4jFilter})"
    }
  }

  private def toBinaryString(attribute: String, value: Any, comparator: String): String =
    s"$attribute $comparator ${CypherValue(value).toCypherString}"
}

/**
  * This a custom data source implementation that allows to read tabular data from a Neo4j database.
  * It is possible to retrieve either nodes or relationships. The data source supports column pruning and
  * simple predicate pushdown.
  *
  * You can set the following Neo4j-specific options:
  * <ul>
  * <li>`boltAddress` <em>required</em>: specifies the bolt address of the Neo4j sever.</li>
  * <li>`boltUser` <em>required</em>: specifies the Neo4j user which is used to connect to the database.</li>
  * <li>`boltPassword` <em>optional</em>: sets the optional password which is used to authenticate the user.</li>
  * <li>`entityType` <em>required</em>: specifies whether to load nodes or relationships.</li>
  * <li>`labels` <em>required if entityType is set to `node`</em>: only load nodes that have the exact labels.</li>
  * <li>`type` <em>required if entityType is set to `relationship`</em>: only load relationships with the given type.</li>
  * </ul>
  */
class Neo4jSparkDataSource extends DataSourceV2 with ReadSupportWithSchema {

  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader = {
    import scala.collection.JavaConverters._

    val scalaOpts = options.asMap().asScala

    val boltUri = scalaOpts.getOrElse("boltaddress", throw IllegalArgumentException("a value", None, "Missing option `boltAddress`"))
    val boltUser = scalaOpts.getOrElse("boltuser", throw IllegalArgumentException("a value", None, "Missing option `boltUser`"))
    val boltPassword = scalaOpts.get("boltpassword")
    val neo4jConfig = Neo4jConfig(new URI(boltUri), boltUser, boltPassword)

    val typ = scalaOpts.getOrElse(
      "entitytype",
      throw IllegalArgumentException("`node` or `relationship", None, "Missing option `entityType`")
    ).toLowerCase match {
      case "node" => Node
      case "relationship" => Relationship
      case other => throw IllegalArgumentException("'node' or 'relationship", other, "Invalid entity type")
    }

    val labels = typ match {
      case Node => scalaOpts.getOrElse("labels", throw IllegalArgumentException("a comma separated list of labels", None, "Missing option `labels`") ).split(",").toSet
      case Relationship => Set(scalaOpts.getOrElse("type", throw IllegalArgumentException("a relationship type", None, "Missing option `type`")))
    }

    new Neo4jDataSourceReader(neo4jConfig, typ, labels, schema)
  }
}

class Neo4jDataSourceReader(neo4jConfig: Neo4jConfig, typ: EntityType, labels: Set[String], var requiredSchema: StructType)
  extends DataSourceReader with SupportsPushDownRequiredColumns with SupportsPushDownFilters with Logging {

  var pushedFilters: Array[Filter] = Array.empty

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }

  override def readSchema(): StructType = {
    requiredSchema
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] ={
    pushedFilters = filters.filterNot(_.isInstanceOf[EqualNullSafe])
    pushedFilters
  }

  override def createDataReaderFactories(): JList[DataReaderFactory[Row]] = {
    val matchClause = typ match {
      case Node => "MATCH (__e)"
      case Relationship => "MATCH (__s)-[__e]->(__t)"
    }

    val projections = requiredSchema.fields.collect {
      case StructField(`idPropertyKey`, _, _, _) => s"id(__e) as $idPropertyKey"
      case StructField(`startIdPropertyKey`, _, _, _) => s"id(__s) as $startIdPropertyKey"
      case StructField(`endIdPropertyKey`, _, _, _) => s"id(__t) as $endIdPropertyKey"
      case StructField(name, _, _, _) => s"__e.$name as $name"
    }.mkString(", ")
    val withClause = s"WITH $projections, __e"


    val labelFilterString =
      if (labels.isEmpty) {
        ""
      } else {
        typ match {
          case Node => s"labels(__e) = ${labels.map(l => s""""$l"""").mkString("[", ",", "]")}"
          case Relationship => s"""type(__e) = "${labels.head}""""
        }
      }

    val predicateString = (pushedFilters.map(_.toNeo4jFilter) :+ labelFilterString).mkString(" AND ")
    val whereClause = if (predicateString == "") "" else s"WHERE $predicateString"

    val query = s"""
         |$matchClause
         |$withClause
         |$whereClause
         |RETURN COUNT(*) as cnt""".stripMargin

    logger.debug(s"Executing Neo4j query: $query")
    val count = neo4jConfig.cypher(query).headOption.get("cnt").cast[Long]

    val batches = (count / 10000.0).ceil.toInt

    val res = new util.ArrayList[DataReaderFactory[Row]]

    (0 until batches).map { i=>
      res.add(Neo4jDataReaderFactory(neo4jConfig, requiredSchema, matchClause, withClause, whereClause, i * 10000))
    }

    res
  }

}

case class Neo4jDataReaderFactory(
  neo4jConfig: Neo4jConfig,
  requiredSchema: StructType,
  matchClause: String,
  withClause: String,
  whereClause: String,
  start: Long)
  extends DataReaderFactory[Row] {

  override def createDataReader(): DataReader[Row] = new Neo4jDataReader(
    neo4jConfig,
    requiredSchema,
    matchClause,
    withClause,
    whereClause,
    start
  )
}

class Neo4jDataReader(
  neo4jConfig: Neo4jConfig,
  requiredSchema: StructType,
  matchClause: String,
  withClause: String,
  whereClause: String,
  start: Long
) extends DataReader[Row] with Logging {

  private val driver = neo4jConfig.driver()
  private val session = driver.session()
  private val tx = session.beginTransaction()

  private val data = {
       val query = s"""
       |$matchClause
       |$withClause
       |$whereClause
       |RETURN ${requiredSchema.map(_.name).mkString(", ")}
       |ORDER BY id(__e)
       |SKIP $start LIMIT 10000
    """.stripMargin
    logger.debug(s"Executing Neo4j query: $query")
    tx.run(query)
  }

  override def close(): Unit = {
    try {
      tx.close()
    } finally {
      try {
        session.close()
      } finally {
        driver.close()
      }
    }
  }

  override def next(): Boolean = {
    data.hasNext
  }

  override def get(): Row = {
    val record = data.next()

    val values = requiredSchema.map(_.name).map { name =>
      record.get(name).asObject()
    }

    Row.fromSeq(values)
  }
}
