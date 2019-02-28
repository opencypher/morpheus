///*
// * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// *
// * Attribution Notice under the terms of the Apache License 2.0
// *
// * This work was created by the collective efforts of the openCypher community.
// * Without limiting the terms of Section 6, any Derivative Work that is not
// * approved by the public consensus process of the openCypher Implementers Group
// * should not be described as “Cypher” (and Cypher® is a registered trademark of
// * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
// * proposals for change that have been documented or implemented should only be
// * described as "implementation extensions to Cypher" or as "proposed changes to
// * Cypher that are not yet approved by the openCypher community".
// */
//package org.opencypher.okapi.api.types
//
//import org.opencypher.okapi.ApiBaseTest
//
//class TernaryTest extends ApiBaseTest {
//
//  it("Ternary.toString") {
//    True.toString shouldBe "definitely true"
//    False.toString shouldBe "definitely false"
//    Maybe.toString shouldBe "maybe"
//  }
//
//  it("Ternary.isTrue") {
//    True.isTrue shouldBe true
//    False.isTrue shouldBe false
//    Maybe.isTrue shouldBe false
//  }
//
//  it("Ternary.maybeTrue") {
//    True.maybeTrue shouldBe true
//    False.isTrue shouldBe false
//    Maybe.maybeTrue shouldBe true
//  }
//
//  it("Ternary.isFalse") {
//    True.isFalse shouldBe false
//    False.isFalse shouldBe true
//    Maybe.isFalse shouldBe false
//  }
//
//  it("Ternary.maybeFalse") {
//    True.maybeFalse shouldBe false
//    False.maybeFalse shouldBe true
//    Maybe.maybeFalse shouldBe true
//  }
//
//  it("Ternary.isDefinite") {
//    True.isDefinite shouldBe true
//    False.isDefinite shouldBe true
//    Maybe.isDefinite shouldBe false
//  }
//
//  it("Ternary.isUnknown") {
//    True.isUnknown shouldBe false
//    False.isUnknown shouldBe false
//    Maybe.isUnknown shouldBe true
//  }
//
//  it("Ternary.negated") {
//    True.negated shouldBe False
//    False.negated shouldBe True
//    Maybe.negated shouldBe Maybe
//  }
//}
