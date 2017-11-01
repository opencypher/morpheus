/*
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
package org.opencypher.caps.impl.typer

import cats.data.NonEmptyList
import org.neo4j.cypher.internal.frontend.v3_3.ast.{Expression, Parameter}
import org.neo4j.cypher.internal.frontend.v3_3.symbols
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types._
import org.opencypher.caps.test.BaseTestSuite
import org.opencypher.caps.test.support.Neo4jAstTestSupport
import org.scalatest.mockito.MockitoSugar

import scala.language.reflectiveCalls

class SchemaTyperTest extends BaseTestSuite with Neo4jAstTestSupport with MockitoSugar {

  val schema = Schema.empty
    .withNodePropertyKeys("Person")("name" -> CTString, "age" -> CTInteger)
    .withNodePropertyKeys("Foo")("name" -> CTBoolean, "age" -> CTFloat)
    .withRelationshipPropertyKeys("KNOWS")("since" -> CTInteger, "relative" -> CTBoolean)

  val typer = SchemaTyper(schema)

  test("typing exists()") {
    implicit val context = typeTracker("n" -> CTNode)

    assertExpr.from("exists(n.prop)") shouldHaveInferredType CTBoolean
    assertExpr.from("exists([n.prop])") shouldFailToInferTypeWithErrors
      InvalidArgument("exists([n.prop])", "[n.prop]")
    assertExpr.from("exists()") shouldFailToInferTypeWithErrors
      WrongNumberOfArguments("exists()", 1, 0)
    assertExpr.from("exists(n.prop, n.prop)") shouldFailToInferTypeWithErrors
      WrongNumberOfArguments("exists(n.prop, n.prop)", 1, 2)
  }

  test("typing count()") {
    implicit val context = typeTracker("a" -> CTNode)

    assertExpr.from("count(*)") shouldHaveInferredType CTInteger
    assertExpr.from("count(a)") shouldHaveInferredType CTInteger
    assertExpr.from("count(a.name)") shouldHaveInferredType CTInteger
  }

  test("typing property of node without label") {
    implicit val context = typeTracker("a" -> CTNode)

    assertExpr.from("a.name") shouldHaveInferredType CTAny
    assertExpr.from("a.age") shouldHaveInferredType CTNumber
  }

  test("typing add") {
    implicit val context = typeTracker("a" -> CTInteger,
                                       "b" -> CTFloat,
                                       "c" -> CTNumber,
                                       "d" -> CTAny.nullable,
                                       "e" -> CTBoolean)

    assertExpr.from("a + a") shouldHaveInferredType CTInteger
    assertExpr.from("b + b") shouldHaveInferredType CTFloat
    assertExpr.from("a + b") shouldHaveInferredType CTFloat
    assertExpr.from("b + a") shouldHaveInferredType CTFloat
    assertExpr.from("a + c") shouldHaveInferredType CTNumber
    assertExpr.from("c + b") shouldHaveInferredType CTNumber

    assertExpr.from("a + d") shouldHaveInferredType CTAny.nullable
    assertExpr.from("d + c") shouldHaveInferredType CTAny.nullable

    assertExpr.from("a + e") shouldFailToInferTypeWithErrors
      NoSuitableSignatureForExpr("a + e", Seq(CTInteger, CTBoolean))
    assertExpr.from("e + a") shouldFailToInferTypeWithErrors
      NoSuitableSignatureForExpr("e + a", Seq(CTBoolean, CTInteger))
    assertExpr.from("e + e") shouldFailToInferTypeWithErrors
      NoSuitableSignatureForExpr("e + e", Seq(CTBoolean, CTBoolean))
  }

  test("typing add for string and list concatenation") {
    assertExpr.from("'foo' + 'bar'") shouldHaveInferredType CTString
    assertExpr.from("[] + [1, 2, 3]") shouldHaveInferredType CTList(CTInteger)
    assertExpr.from("[true] + [1, 2, 3]") shouldHaveInferredType CTList(CTAny)

    assertExpr.from("'foo' + 1") shouldHaveInferredType CTString
    assertExpr.from("'foo' + 3.14") shouldHaveInferredType CTString
    assertExpr.from("'foo' + ['bar', 'giz']") shouldHaveInferredType CTList(CTString)

    assertExpr.from("[] + 1") shouldHaveInferredType CTList(CTInteger)
    assertExpr.from("[3.14] + 1") shouldHaveInferredType CTList(CTNumber)
  }

  test("typing subtract") {
    implicit val context = typeTracker("a" -> CTInteger,
                                       "b" -> CTFloat,
                                       "c" -> CTNumber,
                                       "d" -> CTAny.nullable,
                                       "e" -> CTString)

    assertExpr.from("a - a") shouldHaveInferredType CTInteger
    assertExpr.from("b - b") shouldHaveInferredType CTFloat
    assertExpr.from("a - b") shouldHaveInferredType CTFloat
    assertExpr.from("b - a") shouldHaveInferredType CTFloat
    assertExpr.from("a - c") shouldHaveInferredType CTNumber
    assertExpr.from("c - b") shouldHaveInferredType CTNumber

    assertExpr.from("a - d") shouldHaveInferredType CTAny.nullable
    assertExpr.from("d - c") shouldHaveInferredType CTAny.nullable

    assertExpr.from("a - e") shouldFailToInferTypeWithErrors
      InvalidType("e", Seq(CTInteger, CTFloat, CTNumber), CTString)
  }

  test("typing multiply") {
    implicit val context = typeTracker("a" -> CTInteger,
                                       "b" -> CTFloat,
                                       "c" -> CTNumber,
                                       "d" -> CTAny.nullable,
                                       "e" -> CTString)

    assertExpr.from("a * a") shouldHaveInferredType CTInteger
    assertExpr.from("b * b") shouldHaveInferredType CTFloat
    assertExpr.from("a * b") shouldHaveInferredType CTFloat
    assertExpr.from("b * a") shouldHaveInferredType CTFloat
    assertExpr.from("a * c") shouldHaveInferredType CTNumber
    assertExpr.from("c * b") shouldHaveInferredType CTNumber

    assertExpr.from("a * d") shouldHaveInferredType CTAny.nullable
    assertExpr.from("d * c") shouldHaveInferredType CTAny.nullable

    assertExpr.from("a * e") shouldFailToInferTypeWithErrors
      InvalidType("e", Seq(CTInteger, CTFloat, CTNumber), CTString)
  }

  test("typing divide") {
    implicit val context = typeTracker("a" -> CTInteger,
                                       "b" -> CTFloat,
                                       "c" -> CTNumber,
                                       "d" -> CTAny.nullable,
                                       "e" -> CTString)

    assertExpr.from("a / a") shouldHaveInferredType CTInteger
    assertExpr.from("b / b") shouldHaveInferredType CTFloat
    assertExpr.from("a / b") shouldHaveInferredType CTFloat
    assertExpr.from("b / a") shouldHaveInferredType CTFloat
    assertExpr.from("a / c") shouldHaveInferredType CTNumber
    assertExpr.from("c / b") shouldHaveInferredType CTNumber

    assertExpr.from("a / d") shouldHaveInferredType CTAny.nullable
    assertExpr.from("d / c") shouldHaveInferredType CTAny.nullable

    assertExpr.from("a / e") shouldFailToInferTypeWithErrors
      InvalidType("e", Seq(CTInteger, CTFloat, CTNumber), CTString)
  }

  test("typing label predicates") {
    implicit val context = typeTracker("n" -> CTNode())

    assertExpr.from("n:Person") shouldHaveInferredType CTBoolean
    assertExpr.from("n:Person:Car") shouldHaveInferredType CTBoolean
    assertExpr.from("NOT n:Person:Car") shouldHaveInferredType CTBoolean
    assertExpr.from("NOT(NOT(n:Person:Car))") shouldHaveInferredType CTBoolean
  }

  test("typing AND and OR") {
    implicit val context = typeTracker("b" -> CTBoolean, "c" -> CTBoolean, "int" -> CTInteger)

    assertExpr.from("b AND true") shouldHaveInferredType CTBoolean
    assertExpr.from("b OR false") shouldHaveInferredType CTBoolean
    assertExpr.from("(b AND true) OR (b AND c)") shouldHaveInferredType CTBoolean

    Seq("b AND int", "int OR b", "b AND int AND c").foreach { s =>
      assertExpr(parseExpr(s)) shouldFailToInferTypeWithErrors InvalidType(varFor("int"),
                                                                           CTBoolean,
                                                                           CTInteger)
    }
  }

  test("can convert RetypingPredicate") {
    implicit val tracker = typeTracker("b" -> CTBoolean, "n" -> CTNode())

    assertExpr.from("b AND n:Person AND b AND n:Foo") shouldHaveInferredType CTBoolean
    assertExpr.from("b AND n:Person AND b AND n:Foo") shouldMake varFor("n") haveType CTNode(
      "Person",
      "Foo")
    assertExpr.from("n.prop AND n:Person") shouldMake varFor("n") haveType CTNode("Person")
    assertExpr.from("n.name AND n:Person") shouldMake prop("n", "name") haveType CTString
  }

  test("should detail entity type from predicate") {
    implicit val context = typeTracker("n" -> CTNode)

    assertExpr.from("n:Person") shouldMake varFor("n") haveType CTNode("Person")
    assertExpr.from("n:Person AND n:Dog") shouldMake varFor("n") haveType CTNode("Person", "Dog")

    assertExpr.from("n:Person OR n:Dog") shouldMake varFor("n") haveType CTNode // not enough information for us to act
  }

  test("typing equality") {
    implicit val context = typeTracker("n" -> CTInteger)

    assertExpr.from("n = 1") shouldHaveInferredType CTBoolean
    assertExpr.from("n <> 1") shouldHaveInferredType CTBoolean
  }

  test("typing less than") {
    implicit val context = typeTracker("n" -> CTInteger, "m" -> CTInteger, "o" -> CTString)

    assertExpr.from("n < m") shouldHaveInferredType CTBoolean
    assertExpr.from("n < o") shouldHaveInferredType CTVoid
    assertExpr.from("o < n") shouldHaveInferredType CTVoid
  }

  test("typing less than or equal") {
    implicit val context = typeTracker("n" -> CTInteger, "m" -> CTInteger, "o" -> CTString)

    assertExpr.from("n <= m") shouldHaveInferredType CTBoolean
    assertExpr.from("n <= o") shouldHaveInferredType CTVoid
    assertExpr.from("o <= n") shouldHaveInferredType CTVoid
  }

  test("typing greater than") {
    implicit val context = typeTracker("n" -> CTInteger, "m" -> CTInteger, "o" -> CTString)

    assertExpr.from("n > m") shouldHaveInferredType CTBoolean
    assertExpr.from("n > o") shouldHaveInferredType CTVoid
    assertExpr.from("o > n") shouldHaveInferredType CTVoid
  }

  test("typing greater than or equal") {
    implicit val context = typeTracker("n" -> CTInteger, "m" -> CTInteger, "o" -> CTString)

    assertExpr.from("n >= m") shouldHaveInferredType CTBoolean
    assertExpr.from("n >= o") shouldHaveInferredType CTVoid
    assertExpr.from("o >= n") shouldHaveInferredType CTVoid
  }

  test("typing property equality and IN") {
    implicit val context = typeTracker("n" -> CTNode("Person"))

    assertExpr.from("n.name = 'foo'") shouldHaveInferredType CTBoolean
    assertExpr.from("n.name IN ['foo', 'bar']") shouldHaveInferredType CTBoolean
    assertExpr.from("n.name IN 'foo'") shouldFailToInferTypeWithErrors
      InvalidType(parseExpr("'foo'"), CTList(CTWildcard), CTString)
  }

  test("typing of unsupported expressions") {
    val expr = mock[Expression]
    assertExpr(expr) shouldFailToInferTypeWithErrors UnsupportedExpr(expr)
  }

  test("typing of variables") {
    implicit val tracker = typeTracker("n" -> CTNode("Person"))

    assertExpr.from("n") shouldHaveInferredType CTNode("Person")
  }

  test("typing of parameters (1)") {
    implicit val tracker = TypeTracker.empty.withParameters(Map("param" -> CTNode("Person")))

    assertExpr.from("$param") shouldHaveInferredType CTNode("Person")
  }

  test("typing of parameters (2)") {
    implicit val tracker = TypeTracker.empty.withParameters(Map("param" -> CTAny))

    assertExpr.from("$param") shouldHaveInferredType CTAny
  }

  test("typing of basic literals") {
    assertExpr.from("1") shouldHaveInferredType CTInteger
    assertExpr.from("-3") shouldHaveInferredType CTInteger
    assertExpr.from("true") shouldHaveInferredType CTBoolean
    assertExpr.from("false") shouldHaveInferredType CTBoolean
    assertExpr.from("null") shouldHaveInferredType CTNull
    assertExpr.from("3.14") shouldHaveInferredType CTFloat
    assertExpr.from("-3.14") shouldHaveInferredType CTFloat
    assertExpr.from("'-3.14'") shouldHaveInferredType CTString
  }

  test("typing of list literals") {
    assertExpr.from("[]") shouldHaveInferredType CTList(CTVoid)
    assertExpr.from("[1, 2]") shouldHaveInferredType CTList(CTInteger)
    assertExpr.from("[1, 1.0]") shouldHaveInferredType CTList(CTNumber)
    assertExpr.from("[1, 1.0, '']") shouldHaveInferredType CTList(CTAny)
    assertExpr.from("[1, 1.0, null]") shouldHaveInferredType CTList(CTNumber.nullable)
  }

  test("typing of list indexing") {
    assertExpr.from("[1, 2][15]") shouldHaveInferredType CTVoid
    assertExpr.from("[3.14, -1, 5000][15]") shouldHaveInferredType CTVoid
    assertExpr.from("[[], 1, true][15]") shouldHaveInferredType CTVoid

    assertExpr.from("[1, 2][1]") shouldHaveInferredType CTInteger

    implicit val context =
      TypeTracker.empty.updated(Parameter("param", symbols.CTAny)(pos), CTInteger)

    assertExpr.from("[3.14, -1, 5000][$param]")(
      TypeTracker.empty.withParameters(Map("param" -> CTInteger))) shouldHaveInferredType CTNumber
    assertExpr.from("[[], 1, true][$param]")(
      TypeTracker.empty.withParameters(Map("param" -> CTInteger))) shouldHaveInferredType CTAny
  }

  test("infer type of node property lookup") {
    implicit val context = typeTracker("n" -> CTNode("Person"))

    assertExpr.from("n.name") shouldHaveInferredType CTString
  }

  test("infer type of relationship property lookup") {
    implicit val context = typeTracker("r" -> CTRelationship("KNOWS"))

    assertExpr.from("r.relative") shouldHaveInferredType CTBoolean
  }

  test("typing of id function") {
    implicit val context = typeTracker("n" -> CTNode("Person"))

    assertExpr.from("id(n)") shouldHaveInferredType CTInteger
  }

  test("typing of functions") {
    assertExpr.from("timestamp()") shouldHaveInferredType CTInteger
    assertExpr.from("toInteger(1.0)") shouldHaveInferredType CTInteger
    assertExpr.from("size([0, true, []])") shouldHaveInferredType CTInteger

    assertExpr.from("percentileDisc(1, 3.14)") shouldHaveInferredType CTInteger
    assertExpr.from("percentileDisc(6.67, 3.14)") shouldHaveInferredType CTFloat
    assertExpr.from("percentileDisc([1, 3.14][0], 3.14)") shouldHaveInferredType CTInteger

    // TODO: Making this work requires union types and changing how nullability works. Sad!
    //
    // implicit val context = TyperContext.empty :+ Parameter("param", symbols.CTAny)(pos) -> CTInteger
    // assertExpr.from("percentileDisc([1, 3.14][$param], 3.14)") shouldHaveInferredType CTInteger
  }

  private def typeTracker(typings: (String, CypherType)*): TypeTracker =
    TypeTracker(List(typings.map { case (v, t) => varFor(v) -> t }.toMap))

  private object assertExpr {
    def from(exprText: String)(implicit tracker: TypeTracker = TypeTracker.empty) =
      assertExpr(parseExpr(exprText))
  }

  private case class assertExpr(expr: Expression)(implicit val tracker: TypeTracker =
                                                    TypeTracker.empty) {

    def shouldMake(inner: Expression) = new {
      val inferredTypes = typer.inferOrThrow(expr, tracker).tracker
      def haveType(t: CypherType) = {
        inferredTypes.get(inner) should equal(Some(t))
      }
    }

    def shouldHaveInferredType(expected: CypherType) = {
      val result = typer.inferOrThrow(expr, tracker)
      result.value shouldBe expected
    }

    def shouldFailToInferTypeWithErrors(expectedHd: TyperError, expectedTail: TyperError*) = {
      typer.infer(expr, tracker) match {
        case Left(actual) =>
          actual.toList.toSet should equal(
            NonEmptyList.of(expectedHd, expectedTail: _*).toList.toSet)
        case _ =>
          fail("Expected to get typing errors, but succeeded")
      }
    }
  }
}
