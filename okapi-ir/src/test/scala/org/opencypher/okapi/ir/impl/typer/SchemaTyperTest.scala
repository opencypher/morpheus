/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.okapi.ir.impl.typer

import cats.data.NonEmptyList
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.ir.test.support.Neo4jAstTestSupport
import org.opencypher.okapi.testing.BaseTestSuite
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.expressions.functions.Tail
import org.neo4j.cypher.internal.v4_0.util.{InputPosition, symbols}
import org.scalatest.Assertion
import org.scalatest.mockito.MockitoSugar

import scala.language.reflectiveCalls

class SchemaTyperTest extends SchemaTyperTestSuite with MockitoSugar {

  val baseTypes = Seq[CypherType](
    CTAny, CTNumber, CTNull, CTVoid,
    CTBoolean, CTInteger, CTFloat, CTString,
    CTDate, CTLocalDateTime, CTDuration,
    CTIdentity, CTPath
  )

  val simple =
    baseTypes.map(tpe => tpe.name -> tpe) ++
    baseTypes.map(tpe => s"${tpe.name}_OR_NULL" -> tpe.nullable)

  private val lists = simple
    .map { case (n, t) => s"LIST_$n" -> CTList(t) }

  private val maps = Seq(
    "NODE" -> CTNode(Set("Node")),
    "NODE_EMPTY" -> CTNode(),
    "REL" -> CTRelationship(Set("REL")),
    "REL_EMPTY" -> CTRelationship(),
    "MAP" -> CTMap(simple.toMap),
    "MAP_EMPTY" -> CTMap(Map())
  )

  private val all = Seq(
    simple,
    lists,
    lists.map { case (n, t) => s"${n}_OR_NULL" -> t.nullable },
    maps,
    maps.map { case (n, t) => s"${n}_OR_NULL" -> t.nullable }
  ).flatten

  private val properties  =
    simple ++ Seq("name" -> CTString,  "age" -> CTInteger)

  private val properties2 =
    simple ++ Seq("name" -> CTBoolean, "age" -> CTFloat)

  private val propertiesJoined =
    simple ++ Seq("name" -> CTAny, "age" -> CTNumber)

  private val schema: Schema = Schema.empty
    .withNodePropertyKeys("Node")(properties : _*)
    .withRelationshipPropertyKeys("REL")(properties: _*)
    .withNodePropertyKeys("Node2")(properties2 : _*)
    .withRelationshipPropertyKeys("REL2")(properties2: _*)

  implicit val typer: SchemaTyper =
    SchemaTyper(schema)

  implicit val context: TypeTracker =
    typeTracker(all: _*)
      .withParameters(Map(
        "STRING_EMPTY" -> CypherValue(""),
        "STRING_BOOLEAN" -> CypherValue("BOOLEAN"),
        "FLOAT_0" -> CypherValue(0.0),
        "INTEGER_0" -> CypherValue(0),
        "BOOLEAN_FALSE" -> CypherValue(false),
        "NULL" -> CypherValue(null)
      ))

  it("should report good error on unsupported functions") {
    val f = Tail.asInvocation(Variable("LIST_INTEGER")(pos))(pos)

    assertExpr.from("tail(LIST_INTEGER)") shouldFailToInferTypeWithErrors(
      UnsupportedExpr(f), NoSuitableSignatureForExpr(f, Seq(CTList(CTInteger)))
    )
  }

  it("should type Date") {
    assertExpr.from("date()") shouldHaveInferredType CTDate
    assertExpr.from("date(STRING)") shouldHaveInferredType CTDate
    assertExpr.from("date(MAP)") shouldHaveInferredType CTDate
    assertExpr.from("date(NULL)") shouldHaveInferredType CTNull
    assertExpr.from("date(STRING_OR_NULL)") shouldHaveInferredType CTDate.nullable
    assertExpr.from("date(INTEGER_OR_NULL)") shouldHaveInferredType CTNull
  }

  it("should type Date members") {
    assertExpr.from("DATE.year") shouldHaveInferredType CTInteger
    assertExpr.from("DATE_OR_NULL.year") shouldHaveInferredType CTInteger.nullable
  }

  it("should type LocalDateTime") {
    assertExpr.from("localdatetime()") shouldHaveInferredType CTLocalDateTime
    assertExpr.from("localdatetime(STRING)") shouldHaveInferredType CTLocalDateTime
    assertExpr.from("localdatetime(MAP)") shouldHaveInferredType CTLocalDateTime
    assertExpr.from("localdatetime(NULL)") shouldHaveInferredType CTNull
    assertExpr.from("localdatetime(STRING_OR_NULL)") shouldHaveInferredType CTLocalDateTime.nullable
    assertExpr.from("localdatetime(INTEGER_OR_NULL)") shouldHaveInferredType CTNull
  }

  it("should type temporal accessors") {
    assertExpr.from("DATE.year") shouldHaveInferredType CTInteger
    assertExpr.from("LOCALDATETIME.month") shouldHaveInferredType CTInteger
  }

  it("should type trim(), ltrim(), rtrim()") {
    assertExpr.from("trim(STRING)") shouldHaveInferredType CTString
    assertExpr.from("ltrim(STRING)") shouldHaveInferredType CTString
    assertExpr.from("rtrim(STRING)") shouldHaveInferredType CTString
    assertExpr.from("trim(STRING_OR_NULL)") shouldHaveInferredType CTString.nullable
    assertExpr.from("ltrim(STRING_OR_NULL)") shouldHaveInferredType CTString.nullable
    assertExpr.from("rtrim(STRING_OR_NULL)") shouldHaveInferredType CTString.nullable
  }

  it("should type timestamp()") {
    assertExpr.from("timestamp()") shouldHaveInferredType CTInteger
    assertExpr.from("timestamp() + 5") shouldHaveInferredType CTInteger
  }

  it("should type CASE") {
    assertExpr.from("CASE WHEN INTEGER > INTEGER_OR_NULL THEN STRING END") shouldHaveInferredType CTString
    assertExpr.from("CASE WHEN STRING THEN INTEGER WHEN STRING THEN INTEGER_OR_NULL END") shouldHaveInferredType CTInteger.nullable
    assertExpr.from("CASE WHEN STRING THEN INTEGER WHEN INTEGER THEN INTEGER_OR_NULL ELSE STRING END") shouldHaveInferredType CTAny.nullable
  }

  it("should type coalesce()") {
    assertExpr.from("coalesce(INTEGER,         INTEGER)") shouldHaveInferredType CTInteger
    assertExpr.from("coalesce(INTEGER_OR_NULL, INTEGER_OR_NULL)") shouldHaveInferredType CTInteger.nullable
    assertExpr.from("coalesce(INTEGER,         INTEGER_OR_NULL)") shouldHaveInferredType CTInteger
    assertExpr.from("coalesce(INTEGER,         STRING)") shouldHaveInferredType CTInteger
    assertExpr.from("coalesce(INTEGER_OR_NULL, STRING)") shouldHaveInferredType CTAny.nullable
    assertExpr.from("coalesce()") shouldFailToInferTypeWithErrors
      WrongNumberOfArguments("coalesce()", 1, 0)
  }

  it("typing exists()") {
    assertExpr.from("exists(NODE.BOOLEAN)") shouldHaveInferredType CTBoolean
    assertExpr.from("exists([NODE.BOOLEAN])") shouldFailToInferTypeWithErrors
      InvalidArgument("exists([NODE.BOOLEAN])", "[NODE.BOOLEAN]")
    assertExpr.from("exists()") shouldFailToInferTypeWithErrors
      WrongNumberOfArguments("exists()", 1, 0)
    assertExpr.from("exists(NODE.BOOLEAN, NODE.BOOLEAN)") shouldFailToInferTypeWithErrors
      WrongNumberOfArguments("exists(NODE.BOOLEAN, NODE.BOOLEAN)", 1, 2)
  }

  it("typing count()") {
    assertExpr.from("count(*)") shouldHaveInferredType CTInteger
    assertExpr.from("count(NODE)") shouldHaveInferredType CTInteger
    assertExpr.from("count(NODE.STRING)") shouldHaveInferredType CTInteger
  }

  it("typing toString()") {
    assertExpr.from("toString(INTEGER)") shouldHaveInferredType CTString
    assertExpr.from("toString(BOOLEAN)") shouldHaveInferredType CTString
    assertExpr.from("toString(FLOAT)") shouldHaveInferredType CTString
    assertExpr.from("toString(STRING)") shouldHaveInferredType CTString
    assertExpr.from("toString(INTEGER_OR_NULL)") shouldHaveInferredType CTString.nullable
    assertExpr.from("toString(BOOLEAN_OR_NULL)") shouldHaveInferredType CTString.nullable
    assertExpr.from("toString(FLOAT_OR_NULL)") shouldHaveInferredType CTString.nullable
    assertExpr.from("toString(STRING_OR_NULL)") shouldHaveInferredType CTString.nullable
    assertExpr.from("toString(NULL)") shouldHaveInferredType CTNull
  }

  it("typing toBoolean()") {
    assertExpr.from("toBoolean(BOOLEAN)") shouldHaveInferredType CTBoolean
    assertExpr.from("toBoolean(BOOLEAN_OR_NULL)") shouldHaveInferredType CTBoolean.nullable
  }

  describe("logarithmic functions") {
    it("can type sqrt()") {
      assertExpr.from("sqrt(FLOAT)") shouldHaveInferredType CTFloat
      assertExpr.from("sqrt(INTEGER)") shouldHaveInferredType CTFloat
      assertExpr.from("sqrt(NULL)") shouldHaveInferredType CTNull
    }

    it("can type log()") {
      assertExpr.from("log(FLOAT)") shouldHaveInferredType CTFloat
      assertExpr.from("log(INTEGER)") shouldHaveInferredType CTFloat
      assertExpr.from("log(NULL)") shouldHaveInferredType CTNull
    }

    it("can type log10()") {
      assertExpr.from("log10(FLOAT)") shouldHaveInferredType CTFloat
      assertExpr.from("log10(INTEGER)") shouldHaveInferredType CTFloat
      assertExpr.from("log10(NULL)") shouldHaveInferredType CTNull
    }

    it("can type exp()") {
      assertExpr.from("exp(FLOAT)") shouldHaveInferredType CTFloat
      assertExpr.from("exp(INTEGER)") shouldHaveInferredType CTFloat
      assertExpr.from("exp(NULL)") shouldHaveInferredType CTNull
    }

    it("can type e()") {
      assertExpr.from("e()") shouldHaveInferredType CTFloat
    }
  }

  describe("numeric functions") {
    it("can type abs()") {
      assertExpr.from("abs(FLOAT)") shouldHaveInferredType CTFloat
      assertExpr.from("abs(INTEGER)") shouldHaveInferredType CTInteger
      assertExpr.from("abs(NULL)") shouldHaveInferredType CTNull
    }

    it("can type ceil()") {
      assertExpr.from("ceil(FLOAT)") shouldHaveInferredType CTFloat
      assertExpr.from("ceil(INTEGER)") shouldHaveInferredType CTFloat
      assertExpr.from("ceil(NULL)") shouldHaveInferredType CTNull
    }

    it("can type floor()") {
      assertExpr.from("floor(FLOAT)") shouldHaveInferredType CTFloat
      assertExpr.from("floor(INTEGER)") shouldHaveInferredType CTFloat
      assertExpr.from("floor(NULL)") shouldHaveInferredType CTNull
    }

    it("can type rand()") {
      assertExpr.from("rand()") shouldHaveInferredType CTFloat
    }

    it("can type round()") {
      assertExpr.from("round(FLOAT)") shouldHaveInferredType CTFloat
      assertExpr.from("round(INTEGER)") shouldHaveInferredType CTFloat
      assertExpr.from("round(NULL)") shouldHaveInferredType CTNull
    }

    it("can type sign()") {
      assertExpr.from("sign(FLOAT)") shouldHaveInferredType CTInteger
      assertExpr.from("sign(INTEGER)") shouldHaveInferredType CTInteger
      assertExpr.from("sign(NULL)") shouldHaveInferredType CTNull
    }
  }

  it("typing property of node without label") {
    assertExpr.from("NODE_EMPTY.name") shouldHaveInferredType CTAny
    assertExpr.from("NODE_EMPTY.age") shouldHaveInferredType CTNumber
  }

  it("typing add") {
    assertExpr.from("INTEGER + INTEGER") shouldHaveInferredType CTInteger
    assertExpr.from("FLOAT + FLOAT") shouldHaveInferredType CTFloat
    assertExpr.from("INTEGER + FLOAT") shouldHaveInferredType CTFloat
    assertExpr.from("FLOAT + INTEGER") shouldHaveInferredType CTFloat
    assertExpr.from("INTEGER + NUMBER") shouldHaveInferredType CTNumber
    assertExpr.from("NUMBER + FLOAT") shouldHaveInferredType CTNumber

    assertExpr.from("INTEGER + ANY_OR_NULL") shouldHaveInferredType CTAny.nullable
    assertExpr.from("ANY_OR_NULL + NUMBER") shouldHaveInferredType CTAny.nullable

    assertExpr.from("INTEGER + BOOLEAN") shouldFailToInferTypeWithErrors
      NoSuitableSignatureForExpr("INTEGER + BOOLEAN", Seq(CTInteger, CTBoolean))
    assertExpr.from("BOOLEAN + INTEGER") shouldFailToInferTypeWithErrors
      NoSuitableSignatureForExpr("BOOLEAN + INTEGER", Seq(CTBoolean, CTInteger))
    assertExpr.from("BOOLEAN + BOOLEAN") shouldFailToInferTypeWithErrors
      NoSuitableSignatureForExpr("BOOLEAN + BOOLEAN", Seq(CTBoolean, CTBoolean))
  }

  it("typing add for string and list concatenation") {
    assertExpr.from("STRING + STRING") shouldHaveInferredType CTString
    assertExpr.from("LIST_VOID + LIST_INTEGER") shouldHaveInferredType CTList(CTInteger)
    assertExpr.from("LIST_BOOLEAN + LIST_INTEGER") shouldHaveInferredType CTList(CTAny)

    assertExpr.from("STRING + INTEGER") shouldHaveInferredType CTString
    assertExpr.from("STRING + FLOAT") shouldHaveInferredType CTString
    assertExpr.from("STRING + LIST_STRING") shouldHaveInferredType CTList(CTString)

    assertExpr.from("LIST_VOID + INTEGER") shouldHaveInferredType CTList(CTInteger)
    assertExpr.from("LIST_FLOAT + INTEGER") shouldHaveInferredType CTList(CTNumber)
  }

  it("typing subtract") {
    assertExpr.from("INTEGER - INTEGER") shouldHaveInferredType CTInteger
    assertExpr.from("FLOAT - FLOAT") shouldHaveInferredType CTFloat
    assertExpr.from("INTEGER - FLOAT") shouldHaveInferredType CTFloat
    assertExpr.from("FLOAT - INTEGER") shouldHaveInferredType CTFloat

    assertExpr.from("INTEGER - NULL") shouldHaveInferredType CTNull
    assertExpr.from("FLOAT - NULL") shouldHaveInferredType CTNull
    assertExpr.from("NULL - FLOAT") shouldHaveInferredType CTNull
    assertExpr.from("NULL - INTEGER") shouldHaveInferredType CTNull

    assertExpr.from("INTEGER - ANY_OR_NULL") shouldHaveInferredType CTNumber.nullable

    assertExpr.from("DATE - DURATION") shouldHaveInferredType CTDate
    assertExpr.from("LOCALDATETIME - DURATION") shouldHaveInferredType CTLocalDateTime

    assertExpr.from("INTEGER - STRING") shouldFailToInferTypeWithErrors
      NoSuitableSignatureForExpr(
        Subtract(Variable("INTEGER")(InputPosition.NONE), Variable("STRING")(InputPosition.NONE))(InputPosition.NONE),
        Seq(CTInteger, CTString)
      )
  }

  it("typing multiply") {
    assertExpr.from("INTEGER * INTEGER") shouldHaveInferredType CTInteger
    assertExpr.from("FLOAT * FLOAT") shouldHaveInferredType CTFloat
    assertExpr.from("INTEGER * FLOAT") shouldHaveInferredType CTFloat
    assertExpr.from("FLOAT * INTEGER") shouldHaveInferredType CTFloat
    assertExpr.from("INTEGER * NUMBER") shouldHaveInferredType CTNumber
    assertExpr.from("NUMBER * FLOAT") shouldHaveInferredType CTNumber

    assertExpr.from("INTEGER * NULL") shouldHaveInferredType CTNull
    assertExpr.from("NUMBER * NULL") shouldHaveInferredType CTNull
    assertExpr.from("NULL * NUMBER") shouldHaveInferredType CTNull
    assertExpr.from("NULL * FLOAT") shouldHaveInferredType CTNull

    assertExpr.from("INTEGER * ANY_OR_NULL") shouldHaveInferredType CTAny.nullable
    assertExpr.from("ANY_OR_NULL * NUMBER") shouldHaveInferredType CTAny.nullable

    assertExpr.from("INTEGER * STRING") shouldFailToInferTypeWithErrors
      InvalidType("STRING", Seq(CTInteger, CTFloat, CTNumber), CTString)
  }

  it("typing divide") {
    assertExpr.from("INTEGER / INTEGER") shouldHaveInferredType CTInteger
    assertExpr.from("FLOAT / FLOAT") shouldHaveInferredType CTFloat
    assertExpr.from("INTEGER / FLOAT") shouldHaveInferredType CTFloat
    assertExpr.from("FLOAT / INTEGER") shouldHaveInferredType CTFloat
    assertExpr.from("INTEGER / NUMBER") shouldHaveInferredType CTNumber
    assertExpr.from("NUMBER / FLOAT") shouldHaveInferredType CTNumber

    assertExpr.from("INTEGER / NULL") shouldHaveInferredType CTNull
    assertExpr.from("NUMBER / NULL") shouldHaveInferredType CTNull
    assertExpr.from("NULL / NUMBER") shouldHaveInferredType CTNull
    assertExpr.from("NULL / FLOAT") shouldHaveInferredType CTNull

    assertExpr.from("INTEGER / ANY_OR_NULL") shouldHaveInferredType CTAny.nullable
    assertExpr.from("ANY_OR_NULL / NUMBER") shouldHaveInferredType CTAny.nullable

    assertExpr.from("INTEGER / STRING") shouldFailToInferTypeWithErrors
      InvalidType("STRING", Seq(CTInteger, CTFloat, CTNumber), CTString)
  }

  it("typing label predicates") {
    assertExpr.from("NODE:Person") shouldHaveInferredType CTBoolean
    assertExpr.from("NODE:Person:Car") shouldHaveInferredType CTBoolean
    assertExpr.from("NOT NODE:Person:Car") shouldHaveInferredType CTBoolean
    assertExpr.from("NOT(NOT(NODE:Person:Car))") shouldHaveInferredType CTBoolean
  }

  it("typing AND and OR") {
    assertExpr.from("BOOLEAN AND BOOLEAN") shouldHaveInferredType CTBoolean
    assertExpr.from("BOOLEAN OR BOOLEAN_OR_NULL") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("BOOLEAN AND true") shouldHaveInferredType CTBoolean
    assertExpr.from("BOOLEAN OR false") shouldHaveInferredType CTBoolean
    assertExpr.from("(BOOLEAN AND true) OR (BOOLEAN AND BOOLEAN)") shouldHaveInferredType CTBoolean

    Seq("BOOLEAN AND INTEGER", "INTEGER OR BOOLEAN", "BOOLEAN AND INTEGER AND BOOLEAN").foreach { s =>
      assertExpr(parseExpr(s)) shouldFailToInferTypeWithErrors InvalidType(varFor("INTEGER"), CTBoolean, CTInteger)
    }
  }

  it("can get label information through combined predicates") {
    assertExpr.from("BOOLEAN AND NODE_EMPTY:Person AND BOOLEAN AND NODE_EMPTY:Foo") shouldHaveInferredType CTBoolean
    assertExpr.from("BOOLEAN AND NODE_EMPTY:Person AND BOOLEAN AND NODE_EMPTY:Foo") shouldMake varFor("NODE_EMPTY") haveType CTNode("Person", "Foo")
    assertExpr.from("NODE_EMPTY.name = STRING AND NODE_EMPTY:Node") shouldMake varFor("NODE_EMPTY") haveType CTNode("Node")
    assertExpr.from("NODE_EMPTY.name = STRING AND NODE_EMPTY:Node") shouldMake prop("NODE_EMPTY", "name") haveType CTString
  }

  it("should detail entity type from predicate") {
    assertExpr.from("NODE_EMPTY:Person") shouldMake varFor("NODE_EMPTY") haveType CTNode("Person")
    assertExpr.from("NODE_EMPTY:Person AND NODE_EMPTY:Dog") shouldMake varFor("NODE_EMPTY") haveType CTNode("Person", "Dog")
  }

  it("typing equality") {
    assertExpr.from("INTEGER = 1") shouldHaveInferredType CTBoolean
    assertExpr.from("INTEGER <> 1") shouldHaveInferredType CTBoolean
  }

  it("typing less than") {
    assertExpr.from("INTEGER < INTEGER") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("INTEGER < FLOAT") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("INTEGER < STRING") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("STRING  < INTEGER") shouldHaveInferredType CTBoolean.nullable
  }

  it("typing less than or equal") {
    assertExpr.from("INTEGER <= INTEGER") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("INTEGER <= INTEGER_OR_NULL") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("INTEGER <= STRING") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("STRING <= INTEGER") shouldHaveInferredType CTBoolean.nullable
  }

  it("typing greater than") {
    assertExpr.from("INTEGER > INTEGER") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("INTEGER > STRING") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("STRING > INTEGER") shouldHaveInferredType CTBoolean.nullable
  }

  it("typing greater than or equal") {
    assertExpr.from("INTEGER >= INTEGER") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("INTEGER >= STRING") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("STRING >= INTEGER") shouldHaveInferredType CTBoolean.nullable
  }

  it("typing property equality and IN") {
    assertExpr.from("NODE.STRING = STRING") shouldHaveInferredType CTBoolean
    assertExpr.from("NODE.STRING IN LIST_STRING") shouldHaveInferredType CTBoolean
    assertExpr.from("NODE.STRING IN STRING") shouldFailToInferTypeWithErrors
      InvalidType(parseExpr("STRING"), CTList(CTAny), CTString)
  }

  it("typing of unsupported expressions") {
    val expr = mock[Expression]
    assertExpr(expr) shouldFailToInferTypeWithErrors UnsupportedExpr(expr)
  }

  it("typing of variables") {
    assertExpr.from("NODE") shouldHaveInferredType CTNode("Node")
  }

  it("typing of parameters (1)") {
    assertExpr.from("$STRING_EMPTY") shouldHaveInferredType CTString
  }

  it("typing of basic literals") {
    assertExpr.from("1") shouldHaveInferredType CTInteger
    assertExpr.from("-3") shouldHaveInferredType CTInteger
    assertExpr.from("true") shouldHaveInferredType CTBoolean
    assertExpr.from("false") shouldHaveInferredType CTBoolean
    assertExpr.from("null") shouldHaveInferredType CTNull
    assertExpr.from("3.14") shouldHaveInferredType CTFloat
    assertExpr.from("-3.14") shouldHaveInferredType CTFloat
    assertExpr.from("'-3.14'") shouldHaveInferredType CTString
  }

  it("typing of list literals") {
    assertExpr.from("[]") shouldHaveInferredType CTList(CTVoid)
    assertExpr.from("[1, 2]") shouldHaveInferredType CTList(CTInteger)
    assertExpr.from("[1, 1.0]") shouldHaveInferredType CTList(CTNumber)
    assertExpr.from("[1, 1.0, '']") shouldHaveInferredType CTList(CTAny)
    assertExpr.from("[1, 1.0, null]") shouldHaveInferredType CTList(CTNumber.nullable)
  }

  it("typing of list indexing") {
    assertExpr.from("LIST_INTEGER[15]") shouldHaveInferredType CTInteger.nullable
    assertExpr.from("LIST_NUMBER[15]") shouldHaveInferredType CTNumber.nullable
    assertExpr.from("LIST_ANY[15]") shouldHaveInferredType CTAny.nullable
    assertExpr.from("LIST_INTEGER[1]") shouldHaveInferredType CTInteger.nullable
    assertExpr.from("LIST_NUMBER[$INTEGER_0]") shouldHaveInferredType CTNumber.nullable
    assertExpr.from("LIST_ANY[$INTEGER_0]") shouldHaveInferredType CTAny.nullable
  }

  it("typing of map indexing") {
    assertExpr.from("MAP['INTEGER']") shouldHaveInferredType CTInteger
    assertExpr.from("MAP['BOOLEAN']") shouldHaveInferredType CTBoolean
    assertExpr.from("MAP['baz']") shouldHaveInferredType CTVoid

    assertExpr.from("MAP[STRING]") shouldHaveInferredType CTAny.nullable
    assertExpr.from("MAP[INTEGER]") shouldFailToInferTypeWithErrors InvalidType(Variable("INTEGER")(pos), CTString, CTInteger)

    assertExpr.from("MAP[$STRING_BOOLEAN]") shouldHaveInferredType CTBoolean
    assertExpr.from("MAP[$INTEGER_0]") shouldFailToInferTypeWithErrors InvalidType(Parameter("INTEGER_0", symbols.CTAny)(pos), CTString, CTInteger)
  }

  it("infer type of node property lookup") {
    assertExpr.from("NODE.STRING") shouldHaveInferredType CTString
  }

  it("infer type of relationship property lookup") {
    assertExpr.from("REL.BOOLEAN") shouldHaveInferredType CTBoolean
  }

  it("types of id function") {
    assertExpr.from("id(NODE)") shouldHaveInferredType CTIdentity
    assertExpr.from("id(NODE_OR_NULL)") shouldHaveInferredType CTIdentity.nullable
  }

  it("types functions") {
    assertExpr.from("toInteger(FLOAT)") shouldHaveInferredType CTInteger
    assertExpr.from("size(LIST_ANY)") shouldHaveInferredType CTInteger

    assertExpr.from("percentileDisc(INTEGER, FLOAT)") shouldHaveInferredType CTInteger
    assertExpr.from("percentileDisc(FLOAT, FLOAT)") shouldHaveInferredType CTFloat

    // TODO: Making this work requires union types and changing how nullability works. Sad!
    //
    //implicit val context = TyperContext.empty :+ Parameter("param", symbols.CTAny)(pos) -> CTInteger
    // assertExpr.from("percentileDisc([1, 3.14][$param], 3.14)") shouldHaveInferredType CTInteger
  }

  it("types the properties function") {
    assertExpr.from("properties(NODE)") shouldHaveInferredType CTMap(properties.toMap)
    assertExpr.from("properties(NODE_OR_NULL)") shouldHaveInferredType CTMap(properties.toMap).nullable
    assertExpr.from("properties(NODE_EMPTY)") shouldHaveInferredType CTMap(propertiesJoined.toMap)
    assertExpr.from("properties(NODE_EMPTY_OR_NULL)") shouldHaveInferredType CTMap(propertiesJoined.toMap).nullable
    assertExpr.from("properties(REL)") shouldHaveInferredType CTMap(properties.toMap)
    assertExpr.from("properties(REL_EMPTY)") shouldHaveInferredType CTMap(propertiesJoined.toMap)
    assertExpr.from("properties(REL_EMPTY_OR_NULL)") shouldHaveInferredType CTMap(propertiesJoined.toMap).nullable
    assertExpr.from("properties(REL_OR_NULL)") shouldHaveInferredType CTMap(properties.toMap).nullable
    assertExpr.from("properties(MAP)") shouldHaveInferredType CTMap(simple.toMap)
    assertExpr.from("properties(MAP_OR_NULL)") shouldHaveInferredType CTMap(simple.toMap).nullable
  }

  it("types the keys function") {
    assertExpr.from("keys(NODE)") shouldHaveInferredType CTList(CTString)
    assertExpr.from("keys(NULL)") shouldHaveInferredType CTNull
  }

  it("types the range function") {
    assertExpr.from("range(INTEGER, INTEGER, INTEGER)") shouldHaveInferredType CTList(CTInteger)
  }

  it("types the range function with nullable") {
    assertExpr.from("range(INTEGER_OR_NULL, INTEGER)") shouldHaveInferredType CTList(CTInteger).nullable
  }

  it("types STARTS WITH, CONTAINS, ENDS WITH") {
    assertExpr.from("STRING STARTS WITH STRING") shouldHaveInferredType CTBoolean
    assertExpr.from("STRING ENDS WITH STRING") shouldHaveInferredType CTBoolean
    assertExpr.from("STRING CONTAINS STRING") shouldHaveInferredType CTBoolean
    assertExpr.from("STRING_OR_NULL STARTS WITH STRING") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("STRING_OR_NULL ENDS WITH STRING") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("STRING_OR_NULL CONTAINS STRING") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("STRING STARTS WITH STRING_OR_NULL") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("STRING ENDS WITH STRING_OR_NULL") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("STRING CONTAINS STRING_OR_NULL") shouldHaveInferredType CTBoolean.nullable
  }

  it("types STARTS WITH, CONTAINS, ENDS WITH with null test") {
    assertExpr.from("STRING STARTS WITH NULL") shouldHaveInferredType CTNull
    assertExpr.from("STRING ENDS WITH NULL") shouldHaveInferredType CTNull
    assertExpr.from("STRING CONTAINS NULL") shouldHaveInferredType CTNull
  }

  it("types STARTS WITH, CONTAINS, ENDS WITH with null string") {
    assertExpr.from("NULL STARTS WITH STRING") shouldHaveInferredType CTNull
    assertExpr.from("NULL ENDS WITH STRING") shouldHaveInferredType CTNull
    assertExpr.from("NULL CONTAINS STRING") shouldHaveInferredType CTNull
  }
}

protected abstract class SchemaTyperTestSuite extends BaseTestSuite with Neo4jAstTestSupport {
  def typeTracker(typings: (String, CypherType)*): TypeTracker =
    TypeTracker(typings.map { case (v, t) => varFor(v) -> t }.toMap)

  object assertExpr {
    def from(exprText: String)(implicit typer: SchemaTyper, tracker: TypeTracker = TypeTracker.empty) =
      assertExpr(parseExpr(exprText))
  }

  case class assertExpr(expr: Expression)(implicit typer: SchemaTyper, tracker: TypeTracker = TypeTracker.empty) {

    def shouldMake(inner: Expression): Object {
      def haveType(t: CypherType): Assertion

      val inferredTypes: TypeTracker
    } = new {
      val inferredTypes: TypeTracker = typer.inferOrThrow(expr, tracker).tracker
      def haveType(t: CypherType): Assertion = {
        inferredTypes.get(inner) should equal(Some(t))
      }
    }

    def shouldHaveInferredType(expected: CypherType): Assertion = {
      val result = typer.inferOrThrow(expr, tracker)
      result.value shouldBe expected
    }

    def shouldFailToInferTypeWithErrors(expectedHd: TyperError, expectedTail: TyperError*): Assertion = {
      typer.infer(expr, tracker) match {
        case Left(actual) =>
          actual.toList.toSet should equal(NonEmptyList.of(expectedHd, expectedTail: _*).toList.toSet)
        case _ =>
          fail("Expected to get typing errors, but succeeded")
      }
    }
  }

}

