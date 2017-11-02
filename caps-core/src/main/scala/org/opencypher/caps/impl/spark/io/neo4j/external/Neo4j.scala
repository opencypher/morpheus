/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
  * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package org.opencypher.caps.impl.spark.io.neo4j.external

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.neo4j.driver.v1.{Driver, Session, StatementResult}
import org.opencypher.caps.impl.spark.io.neo4j.external.Neo4j.{LoadDsl, NameProp, PartitionsDsl, Pattern, QueriesDsl, Query}

import scala.collection.JavaConverters._
import scala.language.implicitConversions
import scala.reflect.ClassTag

object Neo4j {

  val UNDEFINED: Long = Long.MaxValue

  implicit def apply(sc: SparkContext): Neo4j = {
    new Neo4j(sc)
  }

  trait QueriesDsl {
    def cypher(cypher: String, params: Map[String, Any]): Neo4j

    def params(params: Map[String, Any]): Neo4j

    def param(key: String, value: Any): Neo4j

    def nodes(cypher: String, params: Map[String, Any]): Neo4j

    def rels(cypher: String, params: Map[String, Any]): Neo4j
  }


  trait PartitionsDsl {
    def partitions(partitions: Long): Neo4j

    def batch(batch: Long): Neo4j

    def rows(rows: Long): Neo4j
  }

  trait LoadDsl {
    def loadRdd[T: ClassTag]: RDD[T]

    def loadRowRdd: RDD[Row]

    def loadNodeRdds: RDD[Row]

    def loadRelRdd: RDD[Row]
  }

  case class Stats(nodes: Long = 0, rels: Long = 0, properties: Long = 0, indexes: Long = 0, constraints: Long = 0)

  case class Updates(created: Stats, updated: Stats, deleted: Stats)

  case class NameProp(name: String, property: String = null) {
    def this(tuple: (String, String)) = this(tuple._1, tuple._2)

    def asTuple = (name, property)
  }

  case class Pattern(source: NameProp, edges: Seq[NameProp], target: NameProp) {
    private def quote(s: String): String = "`" + s + "`"

    private def relTypes = ":" + edges.map("`" + _.name + "`").mkString(":")

    // fast count-queries for the partition sizes
    def countNode(node: NameProp) = s"MATCH (:`${node.name}`) RETURN count(*) as total"

    def countRelsSource(rel: NameProp) = s"MATCH (:`${source.name}`)-[:`${rel.name}`]->() RETURN count(*)"

    def countRelsTarget(rel: NameProp) = s"MATCH ()-[:`${rel.name}`]->(:`${target.name}`) RETURN count(*) AS total"

    def nodeQueries = List(nodeQuery(source), nodeQuery(target))

    def relQueries = edges.map(relQuery)

    def relQuery(rel: NameProp) = {
      val c: List[String] = List(countRelsSource(rel), countRelsTarget(rel))
      var q = s"MATCH (n:`${source.name}`)-[rel:`${rel.name}`]->(m:`${target.name}`) WITH n,rel,m SKIP {_skip} LIMIT {_limit} RETURN id(n) as src, id(m) as dst "
      if (rel.property != null) (q + s", rel.`${rel.property}` as value", c)
      else (q, c)
    }

    def nodeQuery(node: NameProp) = {
      var c = countNode(node)
      var q: String = s"MATCH (n:`${node.name}`) WITH n SKIP {_skip} LIMIT {_limit} RETURN id(n) AS id"
      if (node.property != null) (q + s", n.`${node.property}` as value", c)
      else (q, c)
    }

    def this(source: (String, String), edges: Seq[(String, String)], target: (String, String)) =
      this(new NameProp(source), edges.map(new NameProp(_)), new NameProp(target))

    def this(source: String, edges: Seq[String], target: String) =
      this(NameProp(source), edges.map(NameProp(_)), NameProp(target))

    def edgeNames = edges.map(_.name)
  }

  case class Query(query: String, params: Map[String, Any] = Map.empty) {
    def paramsSeq = params.toSeq

    def isEmpty = query == null
  }

}

case class Partitions(partitions: Long = 1, batchSize: Long = Neo4j.UNDEFINED, rows: Long = Neo4j.UNDEFINED, rowSource: Option[() => Long] = None) {
  def upper(v1: Long, v2: Long): Long = v1 / v2 + Math.signum(v1 % v2).asInstanceOf[Long]

  def effective(): Partitions = {
    //      if (this.rows == Neo4j.UNDEFINED) this.rows = rowSource.getOrElse(() -> Neo4j.UNDEFINED)
    if (this.batchSize == Neo4j.UNDEFINED && this.rows == Neo4j.UNDEFINED) return Partitions()
    if (this.batchSize == Neo4j.UNDEFINED) return this.copy(batchSize = upper(rows, partitions))
    if (this.rows == Neo4j.UNDEFINED) return this.copy(rows = this.batchSize * this.partitions)
    if (this.partitions == 1) return this.copy(partitions = upper(rows, batchSize))
    this
  }

  def skip(index: Int) = index * batchSize

  // if the last batch is smaller to fit the total rows
  def limit(index: Int) = {
    val remainder = rows % batchSize
    if (index < partitions - 1 || remainder == 0) batchSize else remainder
  }
}

class Neo4j(val sc: SparkContext) extends QueriesDsl with PartitionsDsl with LoadDsl {

  // todo
  private def sqlContext: SQLContext = new SQLContext(sc)

  var pattern: Pattern = null
  var nodes: Query = Query(null)
  var rels: Query = Query(null)

  // todo case/base class for partitions, rows, batch
  var partitions = Partitions()
  var defaultRelValue: Any = null


  // --- configure plain query

  override def cypher(cypher: String, params: Map[String, Any] = Map.empty): Neo4j = {
    this.nodes = Query(cypher, this.nodes.params ++ params)
    this
  }

  override def param(key: String, value: Any): Neo4j = {
    this.nodes = this.nodes.copy(params = this.nodes.params + (key -> value))
    this
  }

  override def params(params: Map[String, Any]): Neo4j = {
    this.nodes = this.nodes.copy(params = this.nodes.params ++ params)
    this
  }

  override def nodes(cypher: String, params: Map[String, Any]) = this.cypher(cypher, params)

  override def rels(cypher: String, params: Map[String, Any] = Map.empty) = {
    this.rels = Query(cypher, params)
    this
  }

  // --- configure partitions

  override def rows(rows: Long) = {
    assert(rows > 0)
    this.partitions = partitions.copy(rows = rows)
    this
  }

  override def batch(batch: Long) = {
    assert(batch > 0)
    this.partitions = partitions.copy(batchSize = batch)
    this
  }

  // todo for partitions > 1, generate a batched query SKIP {_skip} LIMIT {_limit}
  // batch could be hard-coded in query, so we only have to pass skip
  override def partitions(partitions: Long): Neo4j = {
    assert(partitions > 0)
    this.partitions = this.partitions.copy(partitions = partitions)
    this
  }

  // -- output

  def loadRelRdd: RDD[Row] = {
    if (pattern != null) {
      val queries: Seq[(String, List[String])] = pattern.relQueries
      val rdds: Seq[RDD[Row]] = queries.map(query => {
        //        val maxCountQuery: () => Long = () => { query._2.map(countQuery => new Neo4jRDD(sc, countQuery).first().getLong(0)).max }
        new Neo4jRDD(sc, query._1, rels.params, partitions) // .copy(rowSource = Option(maxCountQuery)))
      })
      rdds.reduce((a, b) => a.union(b)).distinct()
    } else if (!rels.isEmpty) {
      new Neo4jRDD(sc, rels.query, rels.params, partitions)
    } else {
      throw new RuntimeException("No relationship query provided either as pattern or with rels()")
    }
  }

  private def loadNodeRdds(node: NameProp, params: Map[String, Any], partitions: Partitions) = {
    // todo use count queries
    val queries = pattern.nodeQuery(node)

    new Neo4jRDD(sc, queries._1, params, partitions)
  }


  def loadNodeRdds: RDD[Row] = {
    if (pattern != null) {
      loadNodeRdds(pattern.source, nodes.params, partitions)
        .union(loadNodeRdds(pattern.target, nodes.params, partitions)).distinct()
    } else if (!nodes.isEmpty) {
      new Neo4jRDD(sc, nodes.query, nodes.params, partitions)
    } else if (!rels.isEmpty) {
      new Neo4jRDD(sc, rels.query, rels.params, partitions)
    } else {
      throw new RuntimeException("No relationship query provided either as pattern or with cypher() or nodes()")
    }
  }

  override def loadRowRdd: RDD[Row] = loadNodeRdds

  override def loadRdd[T: ClassTag]: RDD[T] = loadRowRdd.map(_.getAs[T](0))

}

object Executor {

  def toJava(parameters: Map[String, Any]): java.util.Map[String, Object] = {
    parameters.mapValues(toJava).asJava
  }

  private def toJava(x: Any): AnyRef = x match {
    case y: Seq[_] => y.asJava
    case _ => x.asInstanceOf[AnyRef]
  }

  val EMPTY = Array.empty[Any]

  class CypherResult(val schema: StructType, val rows: Iterator[Array[Any]]) {
    def sparkRows: Iterator[Row] = rows.map(row => new GenericRowWithSchema(row, schema))

    def fields = schema.fieldNames
  }

  def execute(sc: SparkContext, query: String, parameters: Map[String, AnyRef]): CypherResult = {
    execute(Neo4jConfig(sc.getConf), query, parameters)
  }

  private def rows(result: StatementResult) = {
    var i = 0
    while (result.hasNext) i = i + 1
    i
  }

  def execute(config: Neo4jConfig, query: String, parameters: Map[String, Any]): CypherResult = {

    def close(driver: Driver, session: Session) = {
      try {
        if (session.isOpen) {
          session.close()
        }
        driver.close()
      } catch {
        case _ => // ignore
      }
    }

    val driver: Driver = config.driver()
    val session = driver.session()

    try {
      val result: StatementResult = session.run(query, toJava(parameters))
      if (!result.hasNext) {
        result.consume()
        session.close()
        driver.close()
        return new CypherResult(new StructType(), Iterator.empty)
      }
      val peek = result.peek()
      val keyCount = peek.size()
      if (keyCount == 0) {
        val res: CypherResult = new CypherResult(new StructType(), Array.fill[Array[Any]](rows(result))(EMPTY).toIterator)
        result.consume()
        close(driver, session)
        return res
      }
      val keys = peek.keys().asScala
      val fields = keys.map(k => (k, peek.get(k).`type`())).map(keyType => CypherTypes.field(keyType))
      val schema = StructType(fields)

      val it = result.asScala.map((record) => {
        val row = new Array[Any](keyCount)
        var i = 0
        while (i < keyCount) {
          row.update(i, record.get(i).asObject())
          i = i + 1
        }
        if (!result.hasNext) {
          result.consume()
          close(driver, session)
        }
        row
      })
      new CypherResult(schema, it)
    } finally {
      close(driver, session)
    }
  }
}

class Neo4jRDD(@transient sc: SparkContext, val query: String, val parameters: Map[String, Any] = Map.empty, partitions: Partitions = Partitions())
  extends RDD[Row](sc, Nil) {

  val neo4jConfig = Neo4jConfig(sc.getConf)

  override def compute(partition: Partition, context: TaskContext): Iterator[Row] = {

    val neo4jPartition: Neo4jPartition = partition.asInstanceOf[Neo4jPartition]

    Executor.execute(neo4jConfig, query, parameters ++ neo4jPartition.window).sparkRows
  }

  override protected def getPartitions: Array[Partition] = {
    val p = partitions.effective()
    Range(0, p.partitions.toInt).map(idx => new Neo4jPartition(idx, p.skip(idx), p.limit(idx))).toArray
  }

  override def toString(): String = s"Neo4jRDD partitions $partitions $query using $parameters"
}


