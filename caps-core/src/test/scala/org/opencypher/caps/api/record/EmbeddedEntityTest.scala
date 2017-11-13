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
package org.opencypher.caps.api.record

import org.opencypher.caps.api.exception.CypherException
import org.opencypher.caps.test.BaseTestSuite

class EmbeddedEntityTest extends BaseTestSuite {

  test("Construct embedded node") {
    val given =
      EmbeddedNode("n" -> "id").build
        .withImpliedLabel("Person")
        .withOptionalLabel("Employee" -> "is_emp")
        .withProperty("name")
        .withPropertyKey("age" -> "YEARS")
        .withPropertyKey("age" -> "AGE")

    val actual = EmbeddedNode(
      "n",
      "id",
      Map("Person" -> None, "Employee" -> Some("is_emp")),
      Map("name" -> Set("name"), "age" -> Set("YEARS", "AGE"))
    )

    given should equal(actual)
    show(given) should equal(
      "Seq((AGE,n.age :: ?), (YEARS,n.age :: ?), (id,n :: :Person NODE), (is_emp,n:Employee :: BOOLEAN), (name,n.name :: ?))")
  }

  test("Construct embedded relationship with static type") {
    val given =
      EmbeddedRelationship("r")
        .from("src")
        .relType("KNOWS")
        .to("dst")
        .build
        .withProperty("name")
        .withPropertyKey("age" -> "YEARS")
        .withPropertyKey("age" -> "AGE")

    val actual = EmbeddedRelationship(
      "r",
      "r",
      "src",
      Right("KNOWS"),
      "dst",
      Map("name" -> Set("name"), "age" -> Set("YEARS", "AGE"))
    )

    given should equal(actual)
    show(given) should equal(
      "Seq((AGE,r.age :: ?), (YEARS,r.age :: ?), (dst,target(r :: :KNOWS RELATIONSHIP)), (name,r.name :: ?), (r,r :: :KNOWS RELATIONSHIP), (src,source(r :: :KNOWS RELATIONSHIP)))")
  }

  test("Construct embedded relationship with dynamic type") {
    val given =
      EmbeddedRelationship("r")
        .from("src")
        .relTypes("typ", "ADMIRES", "IGNORES")
        .to("dst")
        .build
        .withProperty("name")
        .withPropertyKey("age" -> "YEARS")
        .withPropertyKey("age" -> "AGE")

    val actual = EmbeddedRelationship(
      "r",
      "r",
      "src",
      Left("typ" -> Set("ADMIRES", "IGNORES")),
      "dst",
      Map("name" -> Set("name"), "age" -> Set("YEARS", "AGE"))
    )

    given should equal(actual)
    show(given) should equal(
      "Seq((AGE,r.age :: ?), (YEARS,r.age :: ?), (dst,target(r :: :ADMIRES|IGNORES RELATIONSHIP)), (name,r.name :: ?), (r,r :: :ADMIRES|IGNORES RELATIONSHIP), (src,source(r :: :ADMIRES|IGNORES RELATIONSHIP)), (typ,type(r) :: STRING))")
  }

  test("Refuses to use the same slot multiple times when constructing nodes") {
    raisesSlotReUse(EmbeddedNode("n" -> "the_slot").build.withOptionalLabel("Person" -> "the_slot").verify)
    raisesSlotReUse(EmbeddedNode("n" -> "the_slot").build.withPropertyKey("a" -> "the_slot").verify)
  }

  test("Refuses to use the same slot multiple times when constructing relationships") {
    raisesSlotReUse(EmbeddedRelationship("r").from("r").to("b").relType("KNOWS").build.verify)
    raisesSlotReUse(EmbeddedRelationship("r").from("a").to("r").relType("KNOWS").build.verify)
    raisesSlotReUse(EmbeddedRelationship("r").from("a").to("b").relTypes("r", "KNOWS").build.verify)
    raisesSlotReUse(
      EmbeddedRelationship("r" -> "the_slot")
        .from("a")
        .to("b")
        .relType("KNOWS")
        .build
        .withPropertyKey("a" -> "the_slot")
        .verify)
  }

  private def show(entity: VerifiedEmbeddedEntity[_]) = {
    val slots = entity.slots

    slots.keys.toSeq.sorted.map(k => k -> slots(k)).mkString("Seq(", ", ", ")")
  }

  private def raisesSlotReUse[T](f: => T): Unit = {
    an[CypherException] should be thrownBy f
  }
}
