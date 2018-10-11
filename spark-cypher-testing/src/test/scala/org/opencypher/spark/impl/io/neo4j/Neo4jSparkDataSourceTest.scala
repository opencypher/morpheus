package org.opencypher.spark.impl.io.neo4j

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StructField, StructType}
import org.opencypher.okapi.api.types.{CTInteger, CypherType}
import org.opencypher.okapi.impl.exception._
import org.opencypher.okapi.neo4j.io.Neo4jHelpers.Neo4jDefaults._
import org.opencypher.spark.impl.convert.SparkConversions._
import org.opencypher.spark.impl.io.neo4j.Neo4jSparkDataSource._
import org.opencypher.spark.testing.CAPSTestSuite
import org.opencypher.spark.testing.fixture.{CAPSNeo4jServerFixture, TeamDataFixture}

class Neo4jSparkDataSourceTest extends CAPSTestSuite with CAPSNeo4jServerFixture with TeamDataFixture {
  private val dataSourceClass = classOf[Neo4jSparkDataSource]

  describe("reading nodes") {
    it("can read all nodes with specific labels") {
      val schema = toSchema(dataFixtureSchema.keysFor(Set(Set("Person","Swede"))).toSeq :+ (idPropertyKey -> CTInteger))

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
      val schema = toSchema(dataFixtureSchema.keysFor(Set(Set("Person","German"))).toSeq :+ (idPropertyKey -> CTInteger))

      val df = caps.sparkSession.read
        .format(dataSourceClass.getName)
        .option("boltAddress", neo4jConfig.uri.toString)
        .option("boltUser", neo4jConfig.user)
        .option("entityType", "node")
        .option("labels", "Person,German")
        .schema(schema)
        .load

      val result = df.filter("luckyNumber > 21").filter(df.col("name").startsWith("M"))

      result.collect shouldEqual Array(
        Row(null, "Martin", 1337, 2L)
      )
    }

    it("can prune columns nodes") {
      val schema = toSchema(dataFixtureSchema.keysFor(Set(Set("Person","German"))).toSeq :+ (idPropertyKey -> CTInteger))

      val df = caps.sparkSession.read
        .format(dataSourceClass.getName)
        .option("boltAddress", neo4jConfig.uri.toString)
        .option("boltUser", neo4jConfig.user)
        .option("entityType", "node")
        .option("labels", "Person,German")
        .schema(schema)
        .load

      val result = df.select("luckyNumber")

      result.collect shouldEqual Array(
        Row(42),
        Row(1337),
        Row(8)
      )
    }
  }

  describe("reading relationships") {
    val relSpecificKeys = Seq(
      idPropertyKey -> CTInteger,
      startIdPropertyKey -> CTInteger,
      endIdPropertyKey -> CTInteger
    )

    it("can read all relationships with specific type") {
      val schema = toSchema(dataFixtureSchema.relationshipKeys("KNOWS").toSeq ++ relSpecificKeys )

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
      val schema = toSchema(dataFixtureSchema.relationshipKeys("KNOWS").toSeq ++ relSpecificKeys )

      val df = caps.sparkSession.read
        .format(dataSourceClass.getName)
        .option("boltAddress", neo4jConfig.uri.toString)
        .option("boltUser", neo4jConfig.user)
        .option("entityType", "relationship")
        .option("type", "KNOWS")
        .schema(schema)
        .load

      val result = df.filter(s"$startIdPropertyKey > 0").filter("since = 2016")

      result.collect shouldEqual Array(
        Row(2016, 1,1,2),
        Row(2016, 2,2,3)
      )
    }

    it("can prune columns") {
      val schema = toSchema(dataFixtureSchema.relationshipKeys("KNOWS").toSeq ++ relSpecificKeys )

      val df = caps.sparkSession.read
        .format(dataSourceClass.getName)
        .option("boltAddress", neo4jConfig.uri.toString)
        .option("boltUser", neo4jConfig.user)
        .option("entityType", "relationship")
        .option("type", "KNOWS")
        .schema(schema)
        .load

      val result = df.select("since")

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
      val schema = toSchema(Seq(idPropertyKey -> CTInteger))

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


  private def toSchema(propertyKeys: Seq[(String, CypherType)]): StructType = {
    val fields = propertyKeys.map {
      case (property, ct) => StructField(property, ct.toSparkType.get)
    }

    StructType(fields)
  }
}
