package org.opencypher.spark.impl

import org.neo4j.cypher.internal.frontend.v3_2.InputPosition
import org.neo4j.cypher.internal.frontend.v3_2.ast.{Expression, Property, PropertyKeyName, Variable}
import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.api.schema.StdSchema
import org.opencypher.spark.api.types.{CTInteger, CTNode, CTString}

class FrontendTest extends StdTestSuite {

  val pos = InputPosition(0, 0, 0)
  val schema = StdSchema.empty.withNodeKeys("Person")("name" -> CTString, "age" -> CTInteger)

  test("infer pattern and expression types") {
    val statement = Frontend.parse("MATCH (n:Person) RETURN n.name")

    val context = TypeContext.init(statement, schema).inferExpressions()

    context.patternTypeTable should equal(Map(Variable("n")(pos) -> CTNode("Person")))
    context.exprTypeTable should equal(Map(Property(Variable("n")(pos), PropertyKeyName("name")(pos))(pos) -> CTString))
  }
}
