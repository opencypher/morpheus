package org.opencypher.spark.impl.io.neo4j

import java.net.URI
import java.util

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.neo4j.driver.v1.{Record, Value}
import org.opencypher.okapi.api.types.{EntityType, Node, Relationship}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.impl.exception._
import org.opencypher.okapi.impl.util.StringEncodingUtilities._
import org.opencypher.okapi.neo4j.io.Neo4jConfig
import org.opencypher.okapi.neo4j.io.Neo4jHelpers._
import org.opencypher.spark.api.io.GraphEntity._
import org.opencypher.spark.api.io.{Relationship => RelationshipEntity}
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
class Neo4jSparkDataSource extends DataSourceV2 with ReadSupport {

  override def createReader(options: DataSourceOptions): DataSourceReader =
    throw UnsupportedOperationException("The read schema needs to be specified")

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
  extends DataSourceReader with SupportsPushDownFilters with SupportsPushDownRequiredColumns with Logging {

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

  override def planInputPartitions(): JList[InputPartition[InternalRow]] = {

    val labelMatch = labels.map(x =>s":`$x`").mkString("")

    val matchClause = typ match {
      case Node => s"MATCH (__e$labelMatch)"
      case Relationship => "MATCH (__s)-[__e]->(__t)"
    }

    val projections = requiredSchema.fields.collect {
      case StructField(`sourceIdKey`, _, _, _) => s"id(__e) as $sourceIdKey"
      case StructField(RelationshipEntity.sourceStartNodeKey, _, _, _) => s"id(__s) as ${RelationshipEntity.sourceStartNodeKey}"
      case StructField(RelationshipEntity.sourceEndNodeKey, _, _, _) => s"id(__t) as ${RelationshipEntity.sourceEndNodeKey}"
      case StructField(name, _, _, _) if name.isPropertyColumnName => s"__e.${name.toProperty} as $name"
      case StructField(other, _, _, _) => sys.error(other)
    }

    val withClause = s"WITH ${(projections :+ "__e").mkString(", ")}"


    val labelFilterString =
      if (labels.isEmpty) {
        ""
      } else {
        typ match {
          case Node => s"length(labels(__e)) = ${labels.size}"
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

    val res = new util.ArrayList[InputPartition[InternalRow]]

    (0 until batches).map { i =>
      res.add(Neo4jInputPartition(neo4jConfig, requiredSchema, matchClause, withClause, whereClause, i * 10000))
    }

    res
  }

}

case class Neo4jInputPartition(
  neo4jConfig: Neo4jConfig,
  requiredSchema: StructType,
  matchClause: String,
  withClause: String,
  whereClause: String,
  start: Long)
  extends InputPartition[InternalRow] {

  override def createPartitionReader(): InputPartitionReader[InternalRow] = new Neo4jInputPartitionReader(
    neo4jConfig,
    requiredSchema,
    matchClause,
    withClause,
    whereClause,
    start
  )
}

class Neo4jInputPartitionReader(
  neo4jConfig: Neo4jConfig,
  requiredSchema: StructType,
  matchClause: String,
  withClause: String,
  whereClause: String,
  start: Long
) extends InputPartitionReader[InternalRow] with Logging {

  private val driver = neo4jConfig.driver()
  private val session = driver.session()
  private val tx = session.beginTransaction()

  private val data = {
   val returnProperties = if(requiredSchema.isEmpty) "id(__e)" else requiredSchema.map(_.name).mkString(", ")

   val query = s"""
       |$matchClause
       |$withClause
       |$whereClause
       |RETURN $returnProperties
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

  var record: Record = _

  override def next(): Boolean = {
    if(data.hasNext) {
      record = data.next()
      true
    } else {
      false
    }
  }

  override def get(): InternalRow = {
    val values = requiredSchema.map {
      case StructField(name, typ, _, _) =>
        val value = record.get(name)
        if (value.isNull) {
          null
        } else typ match {
          case StringType => value.asString()
          case IntegerType => value.asInt()
          case LongType => value.asLong()
          case FloatType => value.asFloat()
          case DoubleType => value.asDouble()
          case BooleanType => value.asBoolean()
          case ArrayType(_, _) => value.asList().toArray
          case _ => value.asObject()
        }
    }

    Row.fromSeq(values)
  }
}
