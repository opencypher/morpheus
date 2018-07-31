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
package org.opencypher.okapi.api.types

import org.opencypher.okapi.api.types.CypherType._
import org.opencypher.okapi.api.types.LegacyNames._
import org.opencypher.okapi.trees.MatchHelper._
import org.scalatest.{FunSpec, Matchers}

import scala.language.postfixOps

class CypherTypesTest extends FunSpec with Matchers {

  val materialTypes: Seq[CypherType] = Seq(
    CTAny,
    CTBoolean,
    CTNumber,
    CTInteger,
    CTFloat,
    CTString,
    //    CTMap,
    CTAnyNode,
    CTNode("Person"),
    CTNode("Person", "Employee"),
    CTAnyRelationship,
    CTRelationship("KNOWS"),
    CTRelationship("KNOWS", "LOVES"),
    //    CTPath,
    CTList(CTAny),
    CTList(CTList(CTBoolean)),
    CTList(CTAny),
    CTList(CTString.nullable),
    CTAny,
    CypherType.CTVoid
  )

  val nullableTypes: Seq[CypherType] =
    materialTypes.map(_.nullable)

  val allTypes: Seq[CypherType] =
    materialTypes ++ nullableTypes

  //  it("couldBe") {
  //    CTAny couldBeSameTypeAs CTNode should equalWithTracing( true
  //    CTNode couldBeSameTypeAs CTAny should equalWithTracing( true
  //    CTInteger couldBeSameTypeAs CTNumber should equalWithTracing( true
  //    CTNumber couldBeSameTypeAs CTInteger should equalWithTracing( true
  //    CTFloat couldBeSameTypeAs CTInteger should equalWithTracing( false
  //    CTBoolean couldBeSameTypeAs CTInteger should equalWithTracing( false
  //
  //    CTNode couldBeSameTypeAs CTMap should equalWithTracing( true
  //    CTRelationship couldBeSameTypeAs CTNode should equalWithTracing( false
  //    CTRelationship couldBeSameTypeAs CTMap should equalWithTracing( true
  //
  //    CTList(CTInteger) couldBeSameTypeAs CTList(CTFloat) should equalWithTracing( false
  //    CTList(CTInteger) couldBeSameTypeAs CTList(CTAny) should equalWithTracing( true
  //    CTList(CTAny) couldBeSameTypeAs CTList(CTInteger) should equalWithTracing( true
  //  }

  it("union with list of void") {
    val voidList = CTList(CTVoid)
    val otherList = CTList(CTString).nullable

    voidList union otherList should equalWithTracing(otherList)
    otherList union voidList should equalWithTracing(otherList)
  }

  it("type names") {
    CTNode("Person", "Employee").legacyName

    Seq[(CypherType, (String, String))](
      CTNoLabel -> ("NODE()" -> "NODE()?"),
      CTString -> ("STRING" -> "STRING?"),
      CTBoolean -> ("BOOLEAN" -> "BOOLEAN?"),
      CTNumber -> ("NUMBER" -> "NUMBER?"),
      CTInteger -> ("INTEGER" -> "INTEGER?"),
      CTFloat -> ("FLOAT" -> "FLOAT?"),
      CTAnyMap -> ("MAP" -> "MAP?"),
      CTAnyNode -> ("NODE" -> "NODE?"),
      CTNode("Person") -> ("NODE(:Person)" -> "NODE(:Person)?"),
      CTNode("Employee", "Person") -> ("NODE(:Employee:Person)" -> "NODE(:Employee:Person)?"),
      //      CTNode(Set("Person"), Some(QualifiedGraphName("foo.bar"))) -> ("NODE(:Person) @ foo.bar" -> "NODE(:Person) @ foo.bar?"),
      CTAnyRelationship -> ("RELATIONSHIP" -> "RELATIONSHIP?"),
      CTRelationship(Set("KNOWS")) -> ("RELATIONSHIP(:KNOWS)" -> "RELATIONSHIP(:KNOWS)?"),
      CTRelationship(Set("KNOWS", "LOVES")) -> ("RELATIONSHIP(:KNOWS|LOVES)" -> "RELATIONSHIP(:KNOWS|LOVES)?"),
      //      CTRelationship(Set("KNOWS"), Some(QualifiedGraphName("foo.bar"))) -> ("RELATIONSHIP(:KNOWS) @ foo.bar" -> "RELATIONSHIP(:KNOWS) @ foo.bar?"),
      CTPath -> ("PATH" -> "PATH?"),
      CTList(CTInteger) -> ("LIST OF INTEGER" -> "LIST? OF INTEGER"),
      CTList(CTInteger.nullable) -> ("LIST OF INTEGER?" -> "LIST? OF INTEGER?")
      //      CTWildcard -> ("?" -> "??")
    ).foreach {
      case (t, (materialName, nullableName)) =>
        t.isNullable should equalWithTracing(false)
        t.legacyName should equalWithTracing(materialName)
        t.nullable.legacyName should equalWithTracing(nullableName)
    }

    CTAny.legacyName should equalWithTracing("ANY")
    CTAny.nullable.legacyName should equalWithTracing("ANY")
    CTVoid.legacyName should equalWithTracing("VOID")
    CTNull.legacyName should equalWithTracing("NULL")
    CTNull.nullable.legacyName should equalWithTracing("NULL")
  }

  it("can parse CypherType names into CypherTypes") {
    allTypes.foreach { t =>
      fromLegacyName(t.legacyName).get should equalWithTracing(t)
    }
  }

  it("RELATIONSHIP type") {
    CTAnyRelationship.superTypeOf(CTAnyRelationship) should equalWithTracing(true)
    CTAnyRelationship.superTypeOf(CTRelationship("KNOWS")) should equalWithTracing(true)
    CTRelationship("KNOWS").superTypeOf(CTAnyRelationship) should equalWithTracing(false)
    CTAnyRelationship.subTypeOf(CTRelationship("KNOWS")) should equalWithTracing(false)
    CTRelationship("KNOWS").superTypeOf(CTRelationship("KNOWS")) should equalWithTracing(true)
    CTRelationship("KNOWS").superTypeOf(CTRelationship("KNOWS", "LOVES")) should equalWithTracing(false)
    CTRelationship("KNOWS", "LOVES").superTypeOf(CTRelationship("LOVES")) should equalWithTracing(true)
    CTRelationship("KNOWS").superTypeOf(CTRelationship("NOSE")) should equalWithTracing(false)
  }

  it("RELATIONSHIP? type") {
    CTAnyRelationship.nullable.superTypeOf(CTAnyRelationship.nullable) should equalWithTracing(true)
    CTAnyRelationship.nullable.superTypeOf(CTRelationship("KNOWS").nullable) should equalWithTracing(true)
    CTRelationship("KNOWS").nullable.superTypeOf(CTRelationship("KNOWS").nullable) should equalWithTracing(true)
    CTRelationship("KNOWS").nullable.superTypeOf(CTRelationship("KNOWS", "LOVES").nullable) should equalWithTracing(false)
    CTRelationship("KNOWS", "LOVES").nullable.superTypeOf(CTRelationship("LOVES").nullable) should equalWithTracing(true)
    CTRelationship("KNOWS").nullable.superTypeOf(CTRelationship("NOSE").nullable) should equalWithTracing(false)
    CTRelationship("FOO").nullable.superTypeOf(CTNull) should equalWithTracing(true)
  }

  it("NODE type") {
    CTAnyNode.superTypeOf(CTAnyNode) should equalWithTracing(true)
    CTAnyNode.superTypeOf(CTNode("Person")) should equalWithTracing(true)
    CTNode("Person").superTypeOf(CTAnyNode) should equalWithTracing(false)
    CTAnyNode.subTypeOf(CTNode("Person")) should equalWithTracing(false)
    CTNode("Person").superTypeOf(CTNode("Person")) should equalWithTracing(true)
    CTNode("Person").superTypeOf(CTNode("Person", "Employee")) should equalWithTracing(true)
    CTNode("Person", "Employee").superTypeOf(CTNode("Employee")) should equalWithTracing(false)
    CTNode("Person").superTypeOf(CTNode("Foo")) should equalWithTracing(false)
    CTNode("Person").superTypeOf(CTAnyNode) should equalWithTracing(false)
  }

  it("NODE? type") {
    CTAnyNode.nullable.superTypeOf(CTAnyNode.nullable) should equalWithTracing(true)
    CTAnyNode.nullable.superTypeOf(CTNode("Person")) should equalWithTracing(true)
    CTNode("Person").nullable.superTypeOf(CTNode("Person").nullable) should equalWithTracing(true)
    CTNode("Person").union(CTNode("Employee"))
    CTNode("Person").nullable.superTypeOf(CTNode("Person", "Employee").nullable) should equalWithTracing(true)
    CTNode("Person", "Employee").nullable.superTypeOf(CTNode("Employee").nullable) should equalWithTracing(false)
    CTNode("Person").nullable.superTypeOf(CTNode("Foo").nullable) should equalWithTracing(false)
    CTNode("Foo").nullable.superTypeOf(CTNull) should equalWithTracing(true)
  }

  it("can model a graph schema") {
    //    val person = CTLabel("Person")
    //    person.show()
    //    val name =  CTProperty("name", CTString)
    //    name.show()
    //    val personWithName = person & name
    //    personWithName.show()
    //    val age = CTProperty("age", CTInteger)
    //    val personWithNameAndAge = personWithName & age
    //    personWithNameAndAge.show()
    //
    //
    //    employee.show()
    //
    //

    println(CTNumber.name)
    println(CTNumber.nullable.name)
    println(CTNumber.intersect(CTInteger).name)
    println(CTList(CTInteger | CTFloat).name)
    println((CTList(CTInteger | CTFloat) & CTList(CTString)).name)
    println((CTList(CTAny) & CTList(CTString)).nullable.name)

    val employee = CTLabel("Person") & CTProperty("name", CTString) & CTProperty("age", CTInteger)
    val car = CTLabel("Car") & CTProperty("name", CTString) & CTProperty("top-speed", CTInteger)
    val schema = car | employee
    //    schema.show()

    println(schema.possibleTypes.filter(_.subTypeOf(CTProperty("age", CTNumber))))
    println(schema.possibleTypes.filter(_.subTypeOf(CTProperty("age", CTFloat))))
    println(schema.couldBeSubTypeOf(CTLabel("Person") & CTProperty("name", CTAny)))
    println(schema.couldBeSubTypeOf(CTLabel("Person") & CTProperty("name", CTInteger)))

    //    println(car.subTypeOf(schema))
    //
    //    println(employee.subTypeOf(schema))
    //
    //    val moreAccepting = CTLabel("Person") & CTProperty("name", CTAny)
    //
    //    println(employee.subTypeOf(moreAccepting))
    //
    //    println(moreAccepting.subTypeOf(employee))
    //
    //    println(schema.couldBeSubTypeOf(moreAccepting))
    //
    //    println(schema.possibleTypes.mkString("\n"))
    //
    //    val typesSatisfyingMoreAccepting = schema.possibleTypes.filter(_.subTypeOf(moreAccepting))
    //
    //    println(typesSatisfyingMoreAccepting.map(_.pretty).mkString("\n"))
    //
    //    println(schema.possibleTypes.filter(_.subTypeOf(CTProperty("name"))))

    //    val nodesWithNames = schema & CTProperty("name", CTString)
    //    nodesWithNames.show()
    //
    //    val nodesWithAge = schema & CTProperty("age", CTInteger)
    //    nodesWithAge.show()
  }

  it("conversion between VOID and NULL") {
    CTVoid.nullable should equalWithTracing(CTNull)
    CTNull.material should equalWithTracing(CTVoid)
  }

  it("all nullable types contain their material types") {
    materialTypes.foreach(t => t.nullable superTypeOf t)
    materialTypes.foreach(t => t subTypeOf t.nullable)
  }

  it("conversion between material and nullable types") {
    materialTypes.foreach(t => t.nullable.material == t)
    nullableTypes.foreach(t => t.material.nullable == t)
  }

  it("subTypeOf as the inverse of superTypeOf") {
    allTypes.foreach { t1 =>
      allTypes.foreach { t2 =>
        t1 subTypeOf t2 should be(t2 superTypeOf t1)
        t2 subTypeOf t1 should be(t1 superTypeOf t2)
      }
    }
  }

  it("basic type inheritance") {
    CTNumber superTypeOf CTInteger should equalWithTracing(true)
    CTNumber superTypeOf CTFloat should equalWithTracing(true)
    CTAnyMap superTypeOf CTAnyMap should equalWithTracing(true)
    //    CTMap superTypeOf CTNode should equalWithTracing( true
    //    CTMap superTypeOf CTRelationship should equalWithTracing( true

    CTAny superTypeOf CTInteger should equalWithTracing(true)
    CTAny superTypeOf CTFloat should equalWithTracing(true)
    CTAny superTypeOf CTNumber should equalWithTracing(true)
    CTAny superTypeOf CTBoolean should equalWithTracing(true)
    CTAny superTypeOf CTAnyMap should equalWithTracing(true)
    CTAny superTypeOf CTAnyNode should equalWithTracing(true)
    CTAny superTypeOf CTAnyRelationship should equalWithTracing(true)
    CTAny superTypeOf CTPath should equalWithTracing(true)
    CTAny superTypeOf CTList(CTAny) should equalWithTracing(true)
    CTAny superTypeOf CTVoid should equalWithTracing(true)

    CTList(CTNumber) superTypeOf CTList(CTInteger) should equalWithTracing(true)

    CTVoid subTypeOf CTInteger should equalWithTracing(true)
    CTVoid subTypeOf CTFloat should equalWithTracing(true)
    CTVoid subTypeOf CTNumber should equalWithTracing(true)
    CTVoid subTypeOf CTBoolean should equalWithTracing(true)
    CTVoid subTypeOf CTAnyMap should equalWithTracing(true)
    CTVoid subTypeOf CTAnyNode should equalWithTracing(true)
    CTVoid subTypeOf CTAnyRelationship should equalWithTracing(true)
    CTVoid subTypeOf CTPath should equalWithTracing(true)
    CTVoid subTypeOf CTList(CTAny) should equalWithTracing(true)
    CTVoid subTypeOf CTVoid should equalWithTracing(true)
    CTVoid subTypeOf CTList(CTInteger) should equalWithTracing(true)

    CTBoolean.nullable superTypeOf CTAny should equalWithTracing(false)
    // TODO: Disagree
    //    CTAny superTypeOf CTBoolean.nullable should equalWithTracing( false
  }

  it("union") {
    CTInteger union CTFloat should equalWithTracing(CTNumber)
    CTFloat union CTInteger should equalWithTracing(CTNumber)
    CTNumber union CTFloat should equalWithTracing(CTNumber)
    CTNumber union CTInteger should equalWithTracing(CTNumber)
    CTNumber union CTString should equalWithTracing(CTUnion(CTFloat, CTInteger, CTString))

    //    CTNode union CTRelationship should equalWithTracing( CTMap
    //    CTNode union CTMap should equalWithTracing( CTMap
    CTString union CTBoolean should equalWithTracing(CTUnion(CTString, CTBoolean))
    CTAny union CTInteger should equalWithTracing(CTAny)

    CTList(CTInteger union CTFloat) should equalWithTracing(CTList(CTNumber))
    CTList(CTInteger) union CTList(CTFloat) should equalWithTracing(CTUnion(CTList(CTInteger), CTList(CTFloat)))
    CTList(CTInteger) union CTAnyNode should equalWithTracing(CTUnion(CTAnyNode, CTList(CTInteger)))

    //    CTAny union CTWildcard should equalWithTracing( CTAny
    CTAny union CTVoid should equalWithTracing(CTAny)
    //    CTWildcard union CTAny should equalWithTracing( CTAny
    CTVoid union CTAny should equalWithTracing(CTAny)

    CTNode("Car") union CTAnyNode should equalWithTracing(CTAnyNode)
    CTAnyNode union CTNode("Person") should equalWithTracing(CTAnyNode)
  }

  it("union with nullables") {
    CTInteger union CTFloat.nullable should equalWithTracing(CTNumber.nullable)
    CTFloat.nullable union CTInteger.nullable should equalWithTracing(CTNumber.nullable)
    CTNumber.nullable union CTString should equalWithTracing(CTUnion(CTFloat, CTInteger, CTString, CTNull))

    //    CTNode union CTAnyRelationship.nullable should equalWithTracing( CTMap.nullable
    //    CTAnyNode.nullable union CTMap should equalWithTracing( CTMap.nullable
    CTString.nullable union CTBoolean.nullable should equalWithTracing(CTUnion(CTBoolean, CTString, CTNull))
    CTAny union CTInteger.nullable should equalWithTracing(CTAny.nullable)
  }

  it("union with labels and types") {
    CTAnyNode union CTNode("Person") should equalWithTracing(CTAnyNode)
    CTNode("Other") union CTNode("Person") should equalWithTracing(CTUnion(CTNode("Other"), CTNode("Person")))
    CTNode("Person") union CTNode("Person") should equalWithTracing(CTNode("Person"))

    //    val n0 = CTLabel("L1").intersect(CTLabel("L2"))
    //    val lx = CTLabel("Lx")
    //    val n1 = n0.intersect(lx)
    //    val n2 = CTNode("L1", "L2", "Ly")
    //    val u = n1.union(n2)

    //    CTNode("L1", "L2", "Lx") union CTNode("L1", "L2", "Ly") should equalWithTracing( CTNode("L1", "L2")

    CTAnyRelationship union CTRelationship("KNOWS") should equalWithTracing(CTAnyRelationship)
    CTRelationship("OTHER") union CTRelationship("KNOWS") should equalWithTracing(CTRelationship("KNOWS", "OTHER"))
    CTRelationship("KNOWS") union CTRelationship("KNOWS") should equalWithTracing(CTRelationship("KNOWS"))
    CTRelationship("T1", "T2", "Tx") union CTRelationship("T1", "T2", "Ty") should equalWithTracing(CTRelationship(
      "T1",
      "T2",
      "Tx",
      "Ty"))

    //    CTNode("Person") union CTRelationship("KNOWS") should equalWithTracing( CTMap
    //    CTNode("Person") union CTRelationship should equalWithTracing( CTMap
    //    CTRelationship("KNOWS") union CTNode("Person") should equalWithTracing( CTMap
    //    CTRelationship("KNOWS") union CTNode should equalWithTracing( CTMap
  }

  it("intersect") {
    CTInteger intersect CTNumber should equalWithTracing(CTInteger)
    CTAny intersect CTNumber should equalWithTracing(CTNumber)

    CTList(CTInteger) intersect CTList(CTFloat) should equalWithTracing(CTList(CTVoid))
    CTList(CTInteger) intersect CTAnyNode should equalWithTracing(CTVoid)
    CTList(CTAny) intersect CTList(CTNumber) should equalWithTracing(CTList(CTNumber))

    CTVoid intersect CTInteger should equalWithTracing(CTVoid)
    CTVoid intersect CTAny should equalWithTracing(CTVoid)
    CTVoid intersect CTAny should equalWithTracing(CTVoid)

    CTInteger intersect CTVoid should equalWithTracing(CTVoid)
    CTAny intersect CTVoid should equalWithTracing(CTVoid)

    CTAnyNode intersect CTNode("Person") should equalWithTracing(CTNode("Person"))
  }

  it("intersect with labels and types") {
    //    CTMap intersect CTNode should equalWithTracing( CTNode
    //    CTMap intersect CTNode("Person") should equalWithTracing( CTNode("Person")
    //    CTMap intersect CTRelationship("KNOWS") should equalWithTracing( CTRelationship("KNOWS")

    CTNode("Person") intersect CTAnyNode should equalWithTracing(CTNode("Person"))
    CTNode("Person") intersect CTNode("Foo") should equalWithTracing(CTNode("Person", "Foo"))
    CTNode("Person", "Foo") intersect CTNode("Foo") should equalWithTracing(CTNode("Person", "Foo"))

    CTRelationship("KNOWS") intersect CTAnyRelationship should equalWithTracing(CTRelationship("KNOWS"))
    CTRelationship("KNOWS") intersect CTRelationship("LOVES") should equalWithTracing(CTVoid)
    CTRelationship("KNOWS", "LOVES") intersect CTRelationship("LOVES") should equalWithTracing(CTRelationship("LOVES"))
  }

  //  it("type equality between different types") {
  //    allTypes.foreach { t1 =>
  //      allTypes.foreach { t2 =>
  //        val result = t1 sameTypeAs t2
  //        (result.isDefinite) should be(
  //          (!t1.isWildcard && !t2.isWildcard) ||
  //            (t1.isNullable && !t2.isNullable) ||
  //            (!t1.isNullable && t2.isNullable)
  //        )
  //
  //        result match {
  //          case true =>
  //            (t1 subTypeOf t2).maybetrue should equalWithTracing( true
  //            (t2 subTypeOf t1).maybetrue should equalWithTracing( true
  //            (t1 superTypeOf t2).maybetrue should equalWithTracing( true
  //            (t2 superTypeOf t1).maybetrue should equalWithTracing( true
  //
  //          case false =>
  //            if (t1 subTypeOf t2 istrue)
  //              (t2 subTypeOf t1).maybefalse should equalWithTracing( true
  //
  //            if (t2 subTypeOf t1 istrue)
  //              (t1 subTypeOf t2).maybefalse should equalWithTracing( true
  //
  //            if (t1 superTypeOf t2 istrue)
  //              (t2 superTypeOf t1).maybefalse should equalWithTracing( true
  //
  //            if (t2 superTypeOf t1 istrue)
  //              (t1 superTypeOf t2).maybefalse should equalWithTracing( true
  //
  //          case Maybe =>
  //            (
  //              (t1.isWildcard || t2.isWildcard) ||
  //                (t1.isNullable && !t2.isNullable) ||
  //                (!t1.isNullable && t2.isNullable)
  //            ) should equalWithTracing( true
  //        }
  //      }
  //    }
  //  }

  it("antisymmetry of subtyping") {
    allTypes.foreach { t1 =>
      allTypes.foreach { t2 =>
        if (t1 subTypeOf t2) (t2 subTypeOf t1) should equalWithTracing((t2 == t1))
        if (t1 superTypeOf t2) (t2 superTypeOf t1) should equalWithTracing((t2 == t1))
      }
    }
  }
  //
  //  it("type equality between the same type") {
  //    allTypes.foreach(t => t == t)
  //    allTypes.foreach(t => t superTypeOf t)
  //    allTypes.foreach(t => t subTypeOf t)
  //    allTypes.foreach(t => (t union t) == t)
  //    allTypes.foreach(t => (t intersect t) == t)
  //  }
  //
  //  it("computing definite types (type erasure)") {
  //    CTWildcard.wildcardErasedSuperType sameTypeAs CTAny should equalWithTracing( true
  //    CTWildcard.nullable.wildcardErasedSuperType sameTypeAs CTAny.nullable should equalWithTracing( true
  //    CTList(CTWildcard).wildcardErasedSuperType sameTypeAs CTList(CTAny) should equalWithTracing( true
  //    CTList(CTWildcard.nullable).wildcardErasedSuperType sameTypeAs CTList(CTAny.nullable) should equalWithTracing( true
  //    CTList(CTBoolean).wildcardErasedSuperType sameTypeAs CTList(CTBoolean) should equalWithTracing( true
  //    CTList(CTBoolean).nullable.wildcardErasedSuperType sameTypeAs CTList(CTBoolean).nullable should equalWithTracing( true
  //
  //    CTWildcard.wildcardErasedSubType sameTypeAs CTVoid should equalWithTracing( true
  //    CTWildcard.nullable.wildcardErasedSubType sameTypeAs CTNull should equalWithTracing( true
  //    CTList(CTWildcard).wildcardErasedSubType sameTypeAs CTList(CTVoid) should equalWithTracing( true
  //    CTList(CTWildcard.nullable).wildcardErasedSubType sameTypeAs CTList(CTNull) should equalWithTracing( true
  //    CTList(CTBoolean).wildcardErasedSubType sameTypeAs CTList(CTBoolean) should equalWithTracing( true
  //    CTList(CTBoolean).nullable.wildcardErasedSubType sameTypeAs CTList(CTBoolean).nullable should equalWithTracing( true
  //  }
  //
  //  it("handling wildcard types") {
  //    (CTAny superTypeOf CTWildcard) should equalWithTracing( true
  //    (CTWildcard superTypeOf CTVoid) should equalWithTracing( true
  //    (CTWildcard superTypeOf CTAny) should equalWithTracing( Maybe
  //    (CTVoid superTypeOf CTWildcard) should equalWithTracing( Maybe
  //
  //    (CTAny subTypeOf CTWildcard) should equalWithTracing( Maybe
  //    (CTWildcard subTypeOf CTVoid) should equalWithTracing( Maybe
  //    (CTWildcard subTypeOf CTAny) should equalWithTracing( true
  //    (CTVoid subTypeOf CTWildcard) should equalWithTracing( true
  //
  //    materialTypes.foreach { t =>
  //      (t union CTWildcard).wildcardErasedSuperType should equalWithTracing( CTAny
  //    }
  //    materialTypes.foreach { t =>
  //      (t intersect CTWildcard).wildcardErasedSubType should equalWithTracing( CTVoid
  //    }
  //
  //    materialTypes.foreach { t =>
  //      (t union CTWildcard.nullable).wildcardErasedSuperType should equalWithTracing( CTAny.nullable
  //    }
  //    materialTypes.foreach { t =>
  //      (t intersect CTWildcard.nullable).wildcardErasedSubType should equalWithTracing( CTVoid
  //    }
  //
  //    nullableTypes.foreach { t =>
  //      (t union CTWildcard.nullable).wildcardErasedSuperType should equalWithTracing( CTAny.nullable
  //    }
  //    nullableTypes.foreach { t =>
  //      (t intersect CTWildcard.nullable).wildcardErasedSubType should equalWithTracing( CTNull
  //    }
  //
  //    nullableTypes.foreach { t =>
  //      (t union CTWildcard).wildcardErasedSuperType should equalWithTracing( CTAny.nullable
  //    }
  //    nullableTypes.foreach { t =>
  //      (t intersect CTWildcard).wildcardErasedSubType should equalWithTracing( CTVoid
  //    }
  //  }
  //
  //  it("contains wildcard") {
  //    CTNode.containsWildcard should equalWithTracing( false
  //    CTWildcard.containsWildcard should equalWithTracing( true
  //    CTWildcard.nullable.containsWildcard should equalWithTracing( true
  //    CTList(CTAny).containsWildcard should equalWithTracing( false
  //    CTList(CTList(CTWildcard)).containsWildcard should equalWithTracing( true
  //    CTList(CTList(CTWildcard.nullable)).containsWildcard should equalWithTracing( true
  //  }
  //
  //  it("contains nullable") {
  //    CTNode.containsNullable should equalWithTracing( false
  //    CTNode.nullable.containsNullable should equalWithTracing( true
  //    CTWildcard.containsNullable should equalWithTracing( false
  //    CTWildcard.nullable.containsNullable should equalWithTracing( true
  //    CTList(CTAny).containsNullable should equalWithTracing( false
  //    CTList(CTList(CTWildcard)).containsNullable should equalWithTracing( false
  //    CTList(CTList(CTWildcard.nullable)).containsNullable should equalWithTracing( true
  //  }
  //
  //  it("is inhabited") {
  //    allTypes.foreach {
  //      case t @ CTAny      => t.isInhabited should be(true)
  //      case t @ CTVoid     => t.isInhabited should be(false)
  //      case t @ CTWildcard => t.isInhabited should be(Maybe)
  //      case t              => t.isInhabited should be(true)
  //    }
  //  }
  //
  //  it("as nullable as") {
  //    materialTypes.foreach { t =>
  //      materialTypes.foreach { m =>
  //        m.asNullableAs(t) should equalWithTracing(m)
  //      }
  //      nullableTypes.foreach { n =>
  //        n.asNullableAs(t) should equalWithTracing(n.material)
  //      }
  //    }
  //
  //    nullableTypes.foreach { t =>
  //      materialTypes.foreach { m =>
  //        m.asNullableAs(t) should equalWithTracing(m.nullable)
  //      }
  //      nullableTypes.foreach { n =>
  //        n.asNullableAs(t) should equalWithTracing(n)
  //      }
  //    }
  //  }
}
