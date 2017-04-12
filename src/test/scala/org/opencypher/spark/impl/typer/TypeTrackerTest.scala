package org.opencypher.spark.impl.typer

import org.neo4j.cypher.internal.frontend.v3_2.ast.{AstConstructionTestSupport, False, True}
import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.api.types.{CTBoolean, CTString}

class TypeTrackerTest extends StdTestSuite with AstConstructionTestSupport {

  test("insert and lookup") {
    val tracker = TypeTracker.empty.updated(True()(pos), CTString)

    tracker.get(True()(pos)) shouldBe Some(CTString)
  }

  test("push scope and lookup") {
    val tracker = TypeTracker.empty.updated(True()(pos), CTString).pushScope()

    tracker.get(True()(pos)) shouldBe Some(CTString)
  }

  test("pushing and popping scope") {
    val tracker1 = TypeTracker.empty.updated(True()(pos), CTString)

    val tracker2 = tracker1.pushScope().updated(False()(pos), CTBoolean).popScope()

    tracker1 should equal(tracker2.get)
  }

}
