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
package org.opencypher.spark.impl.io.neo4j

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StructField, StructType}
import org.opencypher.okapi.api.types.{CTInteger, CypherType}
import org.opencypher.okapi.impl.exception._
import org.opencypher.okapi.impl.util.StringEncodingUtilities._
import org.opencypher.spark.api.io.GraphEntity._
import org.opencypher.spark.api.io.Relationship
import org.opencypher.spark.impl.convert.SparkConversions._
import org.opencypher.spark.impl.io.neo4j.Neo4jSparkDataSource._
import org.opencypher.spark.testing.CAPSTestSuite
import org.opencypher.spark.testing.fixture.{CAPSNeo4jServerFixture, TeamDataFixture}

class Neo4jSparkDataSourceTest extends CAPSTestSuite with CAPSNeo4jServerFixture with TeamDataFixture {
  private val dataSourceClass = classOf[Neo4jSparkDataSource]

  describe("reading nodes") {
    it("can read all nodes with specific labels") {
      val schema = toSchema(dataFixtureSchema.nodePropertyKeys(Set("Person","Swede")).toSeq, Seq(sourceIdKey -> CTInteger))

      val result = caps.sparkSession.read
        .format(dataSourceClass.getName)
        .option("boltAddress", neo4jConfig.uri.toString)
        .option("boltUser", neo4jConfig.user)
        .option("entityType", "node")
        .option("labels", "Person,Swede")
        .schema(schema)
        .load

      result.collect shouldEqual Array(
        Row("Mats", 23, 1L)
      )
    }

    it("can push down filters for nodes") {
      val schema = toSchema(dataFixtureSchema.nodePropertyKeys(Set("Person","German")).toSeq, Seq(sourceIdKey -> CTInteger))

      val df = caps.sparkSession.read
        .format(dataSourceClass.getName)
        .option("boltAddress", neo4jConfig.uri.toString)
        .option("boltUser", neo4jConfig.user)
        .option("entityType", "node")
        .option("labels", "Person,German")
        .schema(schema)
        .load

      val result = df.filter(s"${"luckyNumber".toPropertyColumnName} > 21").filter(df.col("name".toPropertyColumnName).startsWith("M"))

      result.collect shouldEqual Array(
        Row(null, "Martin", 1337, 2L)
      )
    }

    it("can prune columns nodes") {
      val schema = toSchema(dataFixtureSchema.nodePropertyKeys(Set("Person","German")).toSeq, Seq(sourceIdKey -> CTInteger))

      val df = caps.sparkSession.read
        .format(dataSourceClass.getName)
        .option("boltAddress", neo4jConfig.uri.toString)
        .option("boltUser", neo4jConfig.user)
        .option("entityType", "node")
        .option("labels", "Person,German")
        .schema(schema)
        .load

      val result = df.select("luckyNumber".toPropertyColumnName)

      result.collect shouldEqual Array(
        Row(42),
        Row(1337),
        Row(8)
      )
    }

    it("can run count queries") {
      val schema = toSchema(dataFixtureSchema.nodePropertyKeys(Set("Person","Swede")).toSeq, Seq(sourceIdKey -> CTInteger))

      val result = caps.sparkSession.read
        .format(dataSourceClass.getName)
        .option("boltAddress", neo4jConfig.uri.toString)
        .option("boltUser", neo4jConfig.user)
        .option("entityType", "node")
        .option("labels", "Person,Swede")
        .schema(schema)
        .load.count

      result shouldBe 1
    }

    it("foo") {
      val swedeSchema = toSchema(dataFixtureSchema.nodePropertyKeys(Set("Person","Swede")).toSeq, Seq(sourceIdKey -> CTInteger))
      val swedes = caps.sparkSession.read
        .format(dataSourceClass.getName)
        .option("boltAddress", neo4jConfig.uri.toString)
        .option("boltUser", neo4jConfig.user)
        .option("entityType", "node")
        .option("labels", "Person,Swede")
        .schema(swedeSchema)
        .load
        .select("property_name", "property_luckyNumber", "id")

      val germansSchema = toSchema(dataFixtureSchema.nodePropertyKeys(Set("Person","German")).toSeq, Seq(sourceIdKey -> CTInteger))
      val germans = caps.sparkSession.read
        .format(dataSourceClass.getName)
        .option("boltAddress", neo4jConfig.uri.toString)
        .option("boltUser", neo4jConfig.user)
        .option("entityType", "node")
        .option("labels", "Person,German")
        .schema(germansSchema)
        .load
        .select("property_name", "property_luckyNumber", "id")

      println(germans.union(swedes).groupBy().count().queryExecution.analyzed.verboseString)
      println(germans.union(swedes).groupBy().count().queryExecution.withCachedData.verboseString)
      println(germans.union(swedes).groupBy().count().queryExecution.optimizedPlan.verboseString)
      println(germans.union(swedes).groupBy().count().queryExecution.sparkPlan.verboseString)
      println(germans.union(swedes).groupBy().count().queryExecution.executedPlan.verboseString)
//      println(germans.union(swedes).count)
    }
  }

  describe("reading relationships") {
    val relSpecificKeys = Seq(
      sourceIdKey -> CTInteger,
      Relationship.sourceStartNodeKey -> CTInteger,
      Relationship.sourceEndNodeKey -> CTInteger
    )

    it("can read all relationships with specific type") {
      val schema = toSchema(dataFixtureSchema.relationshipPropertyKeys("KNOWS").toSeq, relSpecificKeys )

      val result = caps.sparkSession.read
        .format(dataSourceClass.getName)
        .option("boltAddress", neo4jConfig.uri.toString)
        .option("boltUser", neo4jConfig.user)
        .option("entityType", "relationship")
        .option("type", "KNOWS")
        .schema(schema)
        .load

      result.collect shouldEqual Array(
        Row(2016, 0,0,1),
        Row(2016, 1,1,2),
        Row(2016, 2,2,3)
      )
    }

    it("can push down filters") {
      val schema = toSchema(dataFixtureSchema.relationshipPropertyKeys("KNOWS").toSeq, relSpecificKeys )

      val df = caps.sparkSession.read
        .format(dataSourceClass.getName)
        .option("boltAddress", neo4jConfig.uri.toString)
        .option("boltUser", neo4jConfig.user)
        .option("entityType", "relationship")
        .option("type", "KNOWS")
        .schema(schema)
        .load

      val result = df.filter(s"${Relationship.sourceStartNodeKey} > 0").filter("since".toPropertyColumnName +" = 2016")

      result.collect shouldEqual Array(
        Row(2016, 1,1,2),
        Row(2016, 2,2,3)
      )
    }

    it("can prune columns") {
      val schema = toSchema(dataFixtureSchema.relationshipPropertyKeys("KNOWS").toSeq, relSpecificKeys )

      val df = caps.sparkSession.read
        .format(dataSourceClass.getName)
        .option("boltAddress", neo4jConfig.uri.toString)
        .option("boltUser", neo4jConfig.user)
        .option("entityType", "relationship")
        .option("type", "KNOWS")
        .schema(schema)
        .load

      val result = df.select("since".toPropertyColumnName)

      result.collect shouldEqual Array(
        Row(2016),
        Row(2016),
        Row(2016)
      )
    }
  }

  describe("filter conversion"){
    it("can converter EqualTo") {
      EqualTo("foo", 3).toNeo4jFilter shouldEqual "foo = 3"
    }
    it("can converter LessThan") {
      LessThan("foo", 3).toNeo4jFilter shouldEqual "foo < 3"
    }
    it("can converter LessThanOrEqual") {
      LessThanOrEqual("foo", 3).toNeo4jFilter shouldEqual "foo <= 3"
    }
    it("can converter GreaterThan") {
      GreaterThan("foo", 3).toNeo4jFilter shouldEqual "foo > 3"
    }
    it("can converter GreaterThanOrEqual") {
      GreaterThanOrEqual("foo", 3).toNeo4jFilter shouldEqual "foo >= 3"
    }
    it("can converter In") {
      In("foo", Array(3, 4, 5)).toNeo4jFilter  shouldEqual "foo IN [3, 4, 5]"
    }
    it("can converter IsNull") {
      IsNull("foo") .toNeo4jFilter shouldEqual "foo IS NULL"
    }
    it("can converter IsNotNull") {
      IsNotNull("foo") .toNeo4jFilter shouldEqual "foo IS NOT NULL"
    }
    it("can converter StringStartsWith") {
      StringStartsWith("foo", "prefix") .toNeo4jFilter shouldEqual "foo STARTS WITH 'prefix'"
    }
    it("can converter StringEndsWith") {
      StringEndsWith("foo", "suffix") .toNeo4jFilter shouldEqual "foo ENDS WITH 'suffix'"
    }
    it("can converter StringContains") {
      StringContains("foo", "substring") .toNeo4jFilter shouldEqual "foo CONTAINS 'substring'"
    }
    it("can converter And") {
      And(LessThan("foo", 10), GreaterThan("foo", 3)).toNeo4jFilter  shouldEqual "(foo < 10 AND foo > 3)"
    }
    it("can converter Or") {
      Or(LessThan("foo", 10), GreaterThan("foo", 3)).toNeo4jFilter  shouldEqual "(foo < 10 OR foo > 3)"
    }
    it("can converter Not") {
      Not(LessThan("foo", 10)).toNeo4jFilter  shouldEqual s"NOT (foo < 10)"
    }
  }

  describe("required options") {
    it("throws errors on missing options") {
      val schema = toSchema(Seq.empty, Seq(sourceIdKey -> CTInteger))

      val ds = caps.sparkSession.read.format(dataSourceClass.getName).schema(schema)

      a[IllegalArgumentException] mustBe thrownBy {
        ds.load
      }

      a[IllegalArgumentException] mustBe thrownBy {
        ds.option("boltAddress", neo4jConfig.uri.toString).load
      }

      a[IllegalArgumentException] mustBe thrownBy {
        ds.option("boltUser", neo4jConfig.user).load
      }

      a[IllegalArgumentException] mustBe thrownBy {
        ds.option("entityType", "relationship").load
      }

      a[IllegalArgumentException] mustBe thrownBy {
        ds.option("entityType", "node").load
      }
    }
  }


  private def toSchema(propertyKeys: Seq[(String, CypherType)], other: Seq[(String, CypherType)]): StructType = {
    val propertyFields = propertyKeys.map {
      case (property, ct) => StructField(property.toPropertyColumnName, ct.toSparkType.get)
    }

    val otherFields = other.map {
      case (field, ct) => StructField(field, ct.toSparkType.get)
    }

    StructType(propertyFields ++ otherFields)
  }
}
