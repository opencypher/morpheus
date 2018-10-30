/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.spark.api.io.sql

import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.testing.Bag
import org.opencypher.spark.api.io.HiveFormat
import org.opencypher.spark.api.value.{CAPSNode, CAPSRelationship}
import org.opencypher.spark.impl.CAPSFunctions.{partitioned_id_assignment, rowIdSpaceBitsUsedByMonotonicallyIncreasingId}
import org.opencypher.spark.testing.CAPSTestSuite
import org.opencypher.sql.ddl.GraphDdl
import org.scalatest.BeforeAndAfterEach

import scala.util.Random

class SqlPropertyGraphDataSourceTest extends CAPSTestSuite with BeforeAndAfterEach {

  private val dataSourceName = "fooDataSource"
  private val databaseName = "fooDatabase"
  private val fooGraphName = GraphName("fooGraph")

  private def generateTableName: String = s"view_${Random.alphanumeric take 10 mkString ""}"

  private def computePartitionedRowId(rowIndex: Long, partitionStartDelta: Long): Long = {
     (partitionStartDelta << rowIdSpaceBitsUsedByMonotonicallyIncreasingId) + rowIndex
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    sparkSession.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName").count()
  }

  override protected def afterEach(): Unit = {
    sparkSession.sql(s"DROP DATABASE IF EXISTS $databaseName CASCADE").count()
    super.afterEach()
  }

  it("adds deltas to generated ids") {
    import sparkSession.implicits._
    val df = sparkSession.createDataFrame(Seq(Tuple1("A"), Tuple1("B"), Tuple1("C"))).toDF("alphabet")
    val withIds = df.withColumn("id", partitioned_id_assignment(0))
    val vanillaIds = List(0, 1, 2)
    withIds.select("id").collect().map(row => row.get(0)).toList should equal(vanillaIds)
    val idsWithDeltaAdded = df.withColumn("id", partitioned_id_assignment(2))
    val resultWithDelta = idsWithDeltaAdded.select("id").collect().map(row => row.get(0))
    resultWithDelta should equal(vanillaIds.map(computePartitionedRowId(_, 2)))
    resultWithDelta should equal(List(0x400000000L, 0x400000001L, 0x400000002L))

    val largeDf = sparkSession.sparkContext.parallelize(
      Seq.fill(100) {
        Tuple1("foo")
      }, 100
    ).toDF("fooCol")
    val largeDfWithIds = largeDf.withColumn("id", partitioned_id_assignment(100))
    val largeResultWithDelta = largeDfWithIds.select("id").collect().map(row => row.get(0).asInstanceOf[Long]).map(_ >> 33).sorted.toList
    val expectation = (0L until 100L).map(rowIndex => computePartitionedRowId(rowIndex, 100L + rowIndex)).map(_ >> 33).sorted.toList
    largeResultWithDelta should equal(expectation)
  }

  it("reads nodes from a table") {
    val fooView = generateTableName

    val ddlString =
      s"""
         |SET SCHEMA $dataSourceName.$databaseName
         |
         |CREATE GRAPH SCHEMA fooSchema
         | LABEL (Foo { foo : STRING })
         | (Foo)
         |
         |CREATE GRAPH fooGraph WITH GRAPH SCHEMA fooSchema
         |  NODE LABEL SETS (
         |    (Foo) FROM $fooView
         |  )
     """.stripMargin

    sparkSession
      .createDataFrame(Seq(Tuple1("Alice")))
      .toDF("foo")
      .write.saveAsTable(s"$databaseName.$fooView")

    val ds = SqlPropertyGraphDataSource(GraphDdl(ddlString), List(SqlDataSourceConfig(HiveFormat, dataSourceName)))(caps)

    ds.graph(fooGraphName).nodes("n").toMapsWithCollectedEntities should equal(Bag(
      CypherMap("n" -> CAPSNode(0, Set("Foo"), CypherMap("foo" -> "Alice")))
    ))
  }

  it("reads nodes from a table with custom column mapping") {
    val fooView = generateTableName

    val ddlString =
      s"""
         |SET SCHEMA $dataSourceName.$databaseName
         |
         |CREATE GRAPH SCHEMA fooSchema
         | LABEL (Foo { key1 : INTEGER, key2 : String })
         | (Foo)
         |
         |CREATE GRAPH fooGraph WITH GRAPH SCHEMA fooSchema
         |  NODE LABEL SETS (
         |    (Foo) FROM $fooView (col1 AS key2, col2 AS key1)
         |  )
     """.stripMargin

    sparkSession
      .createDataFrame(Seq(Tuple2("Alice", 42L)))
      .toDF("col1", "col2")
      .write.saveAsTable(s"$databaseName.$fooView")

    val ds = SqlPropertyGraphDataSource(GraphDdl(ddlString), List(SqlDataSourceConfig(HiveFormat, dataSourceName)))(caps)

    ds.graph(fooGraphName).nodes("n").toMapsWithCollectedEntities should equal(Bag(
      CypherMap("n" -> CAPSNode(0, Set("Foo"), CypherMap("key1" -> 42L, "key2" -> "Alice")))
    ))
  }

  it("reads nodes from multiple tables") {
    val fooView = generateTableName
    val barView = generateTableName

    val ddlString =
      s"""
         |SET SCHEMA $dataSourceName.$databaseName
         |
         |CREATE GRAPH SCHEMA fooSchema
         | LABEL (Foo { foo : STRING })
         | LABEL (Bar { bar : INTEGER })
         | (Foo)
         | (Bar)
         |
         |CREATE GRAPH fooGraph WITH GRAPH SCHEMA fooSchema
         |  NODE LABEL SETS (
         |    (Foo) FROM $fooView
         |    (Bar) FROM $barView
         |  )
     """.stripMargin

    sparkSession
      .createDataFrame(Seq(Tuple1("Alice")))
      .toDF("foo")
      .write.saveAsTable(s"$databaseName.$fooView")
    sparkSession
      .createDataFrame(Seq(Tuple1(0L)))
      .toDF("bar")
      .write.saveAsTable(s"$databaseName.$barView")

    val ds = SqlPropertyGraphDataSource(GraphDdl(ddlString), List(SqlDataSourceConfig(HiveFormat, dataSourceName)))(caps)

    ds.graph(fooGraphName).nodes("n").toMapsWithCollectedEntities should equal(Bag(
      CypherMap("n" -> CAPSNode(computePartitionedRowId(rowIndex = 0, partitionStartDelta = 0), Set("Foo"), CypherMap("foo" -> "Alice"))),
      CypherMap("n" -> CAPSNode(computePartitionedRowId(rowIndex = 0, partitionStartDelta = 1), Set("Bar"), CypherMap("bar" -> 0L)))
    ))
  }

  it("reads relationships from a table") {
    val personView = generateTableName
    val bookView   = generateTableName
    val readsView  = generateTableName

    val ddlString =
      s"""
         |SET SCHEMA $dataSourceName.$databaseName
         |
         |CREATE GRAPH SCHEMA fooSchema
         | LABEL (Person { name   : STRING })
         | LABEL (Book   { title  : STRING })
         | LABEL (READS  { rating : FLOAT  })
         | (Person)
         | (Book)
         | [READS]
         |
         |CREATE GRAPH fooGraph WITH GRAPH SCHEMA fooSchema
         |  NODE LABEL SETS (
         |    (Person) FROM $personView ( person_name AS name )
         |    (Book)   FROM $bookView (book_title AS title )
         |  )
         |  RELATIONSHIP LABEL SETS (
         |    (READS)
         |      FROM $readsView edge
         |        START NODES
         |          LABEL SET (Person) FROM $personView alias_person JOIN ON alias_person.person_id = edge.person
         |        END NODES
         |          LABEL SET (Book)   FROM $bookView   alias_book   JOIN ON edge.book = alias_book.book_id
         |  )
     """.stripMargin

    sparkSession
      .createDataFrame(Seq((0L, "Alice")))
      .toDF("person_id", "person_name")
      .write.saveAsTable(s"$databaseName.$personView")
    sparkSession
      .createDataFrame(Seq((1L, "1984")))
      .toDF("book_id", "book_title")
      .write.saveAsTable(s"$databaseName.$bookView")
    sparkSession
      .createDataFrame(Seq((0L, 1L, 42.23)))
      .toDF("person", "book", "rating")
      .write.saveAsTable(s"$databaseName.$readsView")

    val ds = SqlPropertyGraphDataSource(GraphDdl(ddlString), List(SqlDataSourceConfig(HiveFormat, dataSourceName)))(caps)

    val personId = computePartitionedRowId(rowIndex = 0, partitionStartDelta = 0)
    val bookId = computePartitionedRowId(rowIndex = 0, partitionStartDelta = 1)

    ds.graph(fooGraphName).nodes("n").toMapsWithCollectedEntities should equal(Bag(
      CypherMap("n" -> CAPSNode(personId, Set("Person"), CypherMap("name" -> "Alice"))),
      CypherMap("n" -> CAPSNode(bookId, Set("Book"), CypherMap("title" -> "1984")))
    ))

    ds.graph(fooGraphName).relationships("r").toMapsWithCollectedEntities should equal(Bag(
      CypherMap("r" -> CAPSRelationship(
        id = computePartitionedRowId(rowIndex = 0, partitionStartDelta = 0),
        startId = personId,
        endId = bookId,
        relType = "READS",
        properties = CypherMap("rating" -> 42.23)))
    ))
  }

  it("reads relationships from a table with colliding column names") {
    val nodeView = generateTableName
    val relsView = generateTableName

    val ddlString =
      s"""
         |SET SCHEMA $dataSourceName.$databaseName
         |
         |CREATE GRAPH SCHEMA fooSchema
         | LABEL (Node { id : INTEGER, start : STRING, end : STRING })
         | LABEL (REL  { id : INTEGER, start : STRING, end : STRING })
         | (Node)
         | [REL]
         |
         |CREATE GRAPH fooGraph WITH GRAPH SCHEMA fooSchema
         |  NODE LABEL SETS (
         |    (Node) FROM $nodeView
         |  )
         |  RELATIONSHIP LABEL SETS (
         |    (REL)
         |      FROM $relsView edge
         |        START NODES
         |          LABEL SET (Node) FROM $nodeView alias_node JOIN ON alias_node.node_id = edge.source_id
         |        END NODES
         |          LABEL SET (Node) FROM $nodeView alias_node JOIN ON alias_node.node_id = edge.target_id
         |  )
     """.stripMargin

    sparkSession
      .createDataFrame(Seq(
        (0L, 23, "startValue", "endValue"),
        (1L, 42, "startValue", "endValue")
      )).repartition(1) // to keep id generation predictable
      .toDF("node_id", "id", "start", "end")
      .write.saveAsTable(s"$databaseName.$nodeView")
    sparkSession
      .createDataFrame(Seq((0L, 1L, 1984, "startValue", "endValue")))
      .toDF("source_id", "target_id", "id", "start", "end")
      .write.saveAsTable(s"$databaseName.$relsView")

    val ds = SqlPropertyGraphDataSource(GraphDdl(ddlString), List(SqlDataSourceConfig(HiveFormat, dataSourceName)))(caps)

    val nodeId1 = computePartitionedRowId(rowIndex = 0, partitionStartDelta = 0)
    val nodeId2 = computePartitionedRowId(rowIndex = 1, partitionStartDelta = 0)

    ds.graph(fooGraphName).nodes("n").toMapsWithCollectedEntities should equal(Bag(
      CypherMap("n" -> CAPSNode(nodeId1, Set("Node"), CypherMap("id" -> 23, "start" -> "startValue", "end" -> "endValue"))),
      CypherMap("n" -> CAPSNode(nodeId2, Set("Node"), CypherMap("id" -> 42, "start" -> "startValue", "end" -> "endValue")))
    ))

    ds.graph(fooGraphName).relationships("r").toMapsWithCollectedEntities should equal(Bag(
      CypherMap("r" -> CAPSRelationship(
        id = computePartitionedRowId(rowIndex = 0, partitionStartDelta = 0),
        startId = nodeId1,
        endId = nodeId2,
        relType = "REL",
        properties = CypherMap("id" -> 1984, "start" -> "startValue", "end" -> "endValue")))
    ))
  }

  it("reads relationships from multiple tables") {
    val personView = generateTableName
    val bookView   = generateTableName
    val readsView1 = generateTableName
    val readsView2 = generateTableName

    val ddlString =
      s"""
         |SET SCHEMA $dataSourceName.$databaseName
         |
         |CREATE GRAPH SCHEMA fooSchema
         | LABEL (Person { name   : STRING })
         | LABEL (Book   { title  : STRING })
         | LABEL (READS  { rating : FLOAT  })
         | (Person)
         | (Book)
         | [READS]
         |
         |CREATE GRAPH fooGraph WITH GRAPH SCHEMA fooSchema
         |  NODE LABEL SETS (
         |    (Person) FROM $personView ( person_name AS name )
         |    (Book) FROM $bookView (book_title AS title )
         |  )
         |  RELATIONSHIP LABEL SETS (
         |    (READS)
         |      FROM $readsView1 edge
         |        START NODES
         |          LABEL SET (Person) FROM $personView alias_person JOIN ON alias_person.person_id = edge.person
         |        END NODES
         |          LABEL SET (Book)   FROM $bookView   alias_book   JOIN ON edge.book = alias_book.book_id
         |      FROM $readsView2 edge (rates AS rating)
         |        START NODES
         |          LABEL SET (Person) FROM $personView alias_person JOIN ON edge.p_id = alias_person.person_id
         |        END NODES
         |          LABEL SET (Book)   FROM $bookView   alias_book   JOIN ON alias_book.book_id = edge.b_id
         |  )
     """.stripMargin

    sparkSession
      .createDataFrame(Seq((0L, "Alice")))
      .toDF("person_id", "person_name")
      .write.saveAsTable(s"$databaseName.$personView")
    sparkSession
      .createDataFrame(Seq((1L, "1984"), (2L, "Scala with Cats"))).repartition(1) // to keep id generation predictable
      .toDF("book_id", "book_title")
      .write.saveAsTable(s"$databaseName.$bookView")
    sparkSession
      .createDataFrame(Seq((0L, 1L, 42.23)))
      .toDF("person", "book", "rating")
      .write.saveAsTable(s"$databaseName.$readsView1")
    sparkSession
      .createDataFrame(Seq((0L, 2L, 13.37)))
      .toDF("p_id", "b_id", "rates")
      .write.saveAsTable(s"$databaseName.$readsView2")

    val ds = SqlPropertyGraphDataSource(GraphDdl(ddlString), List(SqlDataSourceConfig(HiveFormat, dataSourceName)))(caps)

    val personId = computePartitionedRowId(rowIndex = 0, partitionStartDelta = 0)
    val book1Id = computePartitionedRowId(rowIndex = 0, partitionStartDelta = 1)
    val book2Id = computePartitionedRowId(rowIndex = 1, partitionStartDelta = 1)

    ds.graph(fooGraphName).nodes("n").toMapsWithCollectedEntities should equal(Bag(
      CypherMap("n" -> CAPSNode(personId, Set("Person"), CypherMap("name" -> "Alice"))),
      CypherMap("n" -> CAPSNode(book1Id, Set("Book"), CypherMap("title" -> "1984"))),
      CypherMap("n" -> CAPSNode(book2Id, Set("Book"), CypherMap("title" -> "Scala with Cats")))
    ))

    ds.graph(fooGraphName).relationships("r").toMapsWithCollectedEntities should equal(Bag(
      CypherMap("r" -> CAPSRelationship(
        id = computePartitionedRowId(rowIndex = 0, partitionStartDelta = 0),
        startId = personId,
        endId = book1Id,
        relType = "READS",
        properties = CypherMap("rating" -> 42.23))),
      CypherMap("r" -> CAPSRelationship(
        id = computePartitionedRowId(rowIndex = 0, partitionStartDelta = 1),
        startId = personId,
        endId = book2Id,
        relType = "READS",
        properties = CypherMap("rating" -> 13.37)))

    ))
  }

}
