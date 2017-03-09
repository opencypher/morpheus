package org.opencypher.spark.prototype.impl.load

import org.apache.spark.sql.types._
import org.opencypher.spark.api.types.{CTInteger, CTString}
import org.opencypher.spark.prototype.api.schema.Schema
import org.opencypher.spark.{StdTestSuite, TestSession}

class GraphSpacesTest extends StdTestSuite with TestSession.Fixture {

  test("import nodes from neo") {
    val schema = Schema.empty
      .withNodeKeys("Tweet")("id" -> CTInteger, "text" -> CTString.nullable, "created" -> CTInteger.nullable)
    val space = GraphSpaces.fromNeo4j(schema, "MATCH (n:Tweet) RETURN n LIMIT 100", "RETURN 1")
    val df = space.base.nodes.records.toDF

    df.count() shouldBe 100
    df.schema should equal(StructType(Array(
      StructField("n", LongType, nullable = false),
      StructField("label_Tweet", BooleanType, nullable = false),
      StructField("prop_id", LongType, nullable = false),
      StructField("prop_text", StringType, nullable = true),
      StructField("prop_created", LongType, nullable = true)
    )))
  }
}
