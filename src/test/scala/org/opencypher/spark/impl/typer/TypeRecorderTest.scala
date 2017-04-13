package org.opencypher.spark.impl.typer

import org.neo4j.cypher.internal.frontend.v3_2.Ref
import org.neo4j.cypher.internal.frontend.v3_2.ast.{AstConstructionTestSupport, True}
import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.api.types.{CTBoolean, CTString}

class TypeRecorderTest extends StdTestSuite with AstConstructionTestSupport {

  test("can convert to map") {
    val expr1 = True()(pos)
    val expr2 = True()(pos)
    val recorder = TypeRecorder(List(Ref(expr1) -> CTBoolean, Ref(expr2) -> CTString))

    recorder.toMap should equal(Map(Ref(expr1) -> CTBoolean, Ref(expr2) -> CTString))
  }

}
