/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.caps.api.value

import org.opencypher.caps.api.value.CypherValue._

class CAPSValueConversionTest extends CAPSValueTestSuite {

  import CAPSTestValues._

  //  test("PATH conversion") {
  //    val originalValues = PATH_valueGroups.flatten
  //    val scalaValues = originalValues.map(CAPSPath.contents).map(_.orNull)
  //    val newValues = scalaValues.map {
  //      case null             => null
  //      case elements: Seq[_] => CAPSPath(elements.asInstanceOf[Seq[CAPSEntityValue]])
  //    }
  //
  //    newValues should equal(originalValues)
  //
  //    originalValues.foreach { v =>
  //      CAPSPath.isOrContainsNull(v) should equal(v == null)
  //    }
  //  }

  test("RELATIONSHIP conversion") {
    val originalValues = RELATIONSHIP_valueGroups.flatten
    val scalaValues = originalValues.map(_.unwrap)
    val newValues = scalaValues.map {
      case scalaClass => CypherValue(scalaClass)
    }
    assert(newValues == originalValues)
    originalValues.foreach { v =>
      assert(v.isNull == (v == CypherNull))
    }
  }

  test("NODE conversion") {
    val originalValues = NODE_valueGroups.flatten
    val scalaValues = originalValues.map(_.unwrap)
    val newValues = scalaValues.map(CypherValue(_))
    assert(newValues == originalValues)
    originalValues.foreach { v =>
      assert(v.isNull == (v == CypherNull))
    }
  }

  test("MAP conversion") {
    val originalValues = MAP_valueGroups.flatten
    val scalaValues = originalValues.map(_.unwrap)
    val newValues = scalaValues.map(CypherValue(_))
    assert(newValues == originalValues)
    originalValues.foreach { v =>
      assert(v.isOrContainsNull == (v == CypherNull || v.as[CypherMap].get.unwrap.valuesIterator.exists(_ == null)))
    }
  }

//  test("LIST conversion") {
//    val originalValues = LIST_valueGroups.flatten
//    val scalaValues = originalValues.map(CAPSList.contents).map(_.orNull)
//    val newValues = scalaValues.map {
//      case null => null
//      case l: Seq[CypherValue] => CAPSList(l)
//    }
//
//    newValues should equal(originalValues)
//
//    originalValues.foreach { v =>
//      CAPSList.isOrContainsNull(v) should equal(v == null || CAPSList.unapply(v).exists(_.exists(_ == null)))
//    }
//  }
//
//  test("STRING conversion") {
//    val originalValues = STRING_valueGroups.flatten
//    val scalaValues = originalValues.map(CAPSString.contents).map(_.orNull)
//    val newValues = scalaValues.map {
//      case null => null
//      case s: java.lang.String => CAPSString(s)
//    }
//
//    newValues should equal(originalValues)
//
//    originalValues.foreach { v =>
//      CAPSString.isOrContainsNull(v) should equal(v == null)
//    }
//  }
//
//  test("BOOLEAN conversion") {
//    val originalValues = BOOLEAN_valueGroups.flatten
//    val scalaValues = originalValues.map(CAPSBoolean.contents)
//    val newValues = scalaValues.map {
//      case None => null
//      case Some(b) => CAPSBoolean(b)
//    }
//
//    newValues should equal(originalValues)
//
//    originalValues.foreach { v =>
//      CAPSBoolean.isOrContainsNull(v) should equal(v == null)
//    }
//  }
//
//  test("INTEGER conversion") {
//    val originalValues = INTEGER_valueGroups.flatten
//    val scalaValues = originalValues.map(CAPSInteger.contents).map(_.orNull)
//    val newValues = scalaValues.map {
//      case null => null
//      case l: java.lang.Long => CAPSInteger(l)
//    }
//
//    newValues should equal(originalValues)
//
//    originalValues.foreach { v =>
//      CAPSInteger.isOrContainsNull(v) should equal(v == null)
//    }
//  }
//
//  test("FLOAT conversion") {
//    val originalValues = FLOAT_valueGroups.flatten
//    val scalaValues = originalValues.map(CAPSFloat.contents).map(_.orNull)
//    val newValues = scalaValues.map {
//      case null => null
//      case d: java.lang.Double => CAPSFloat(d)
//    }
//
//    newValues should equal(originalValues)
//
//    originalValues.foreach { v =>
//      CAPSFloat.isOrContainsNull(v) should equal(v == null)
//    }
//  }
//
//  test("NUMBER conversion") {
//    val originalValues = NUMBER_valueGroups.flatten
//    val scalaValues = originalValues.map(CAPSNumber.contents).map(_.orNull)
//    val newValues = scalaValues.map {
//      case null => null
//      case l: java.lang.Long => CAPSInteger(l)
//      case d: java.lang.Double => CAPSFloat(d)
//    }
//
//    newValues should equal(originalValues)
//
//    originalValues.foreach { v =>
//      CAPSNumber.isOrContainsNull(v) should equal(v == null)
//    }
//  }
//
//  test("ALL conversion") {
//    val originalValues = ANY_valueGroups.flatten
//    val scalaValues = originalValues.map(CypherValue.contents).map(_.orNull)
//    val wut = originalValues.map(x => x -> CypherValue.contents(x))
//    val newValues = scalaValues.map {
//      case null => null
//      case b: java.lang.Boolean => CAPSBoolean(b)
//      case s: java.lang.String => CAPSString(s)
//      case l: java.lang.Long => CAPSInteger(l)
//      case r: RegularMap => CypherMap(r.properties)
//      case n: NodeContents => CAPSNode(n.id, n.labels, n.properties)
//      case r: RelationshipContents => CAPSRelationship(r.id, r.startId, r.endId, r.relationshipType, r.properties)
//      case d: java.lang.Double => CAPSFloat(d)
//      case l: Seq[_] if isPathLike(l) => CAPSPath(l.asInstanceOf[Seq[CAPSEntityValue]])
//      case l: Seq[_] => CAPSList(l.asInstanceOf[Seq[CypherValue]])
//    }
//
//    newValues should equal(originalValues)
//
//    originalValues.foreach { v =>
//      if (v == null) CypherValue.isOrContainsNull(v) should equal(true)
//    }
//  }
}
