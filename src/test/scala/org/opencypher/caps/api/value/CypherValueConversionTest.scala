/**
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
package org.opencypher.caps.api.value

class CypherValueConversionTest extends CypherValueTestSuite {

  import CypherTestValues._

  test("PATH conversion") {
    val originalValues = PATH_valueGroups.flatten
    val scalaValues = originalValues.map(CypherPath.contents).map(_.orNull)
    val newValues = scalaValues.map {
      case null             => null
      case elements: Seq[_] => CypherPath(elements.asInstanceOf[Seq[CypherEntityValue]]: _*)
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherPath.isIncomparable(v) should equal(v == null)
    }
  }

  test("RELATIONSHIP conversion") {
    val originalValues = RELATIONSHIP_valueGroups.flatten
    val scalaValues: Seq[RelationshipContents] = originalValues.map(CypherRelationship.contents).map(_.orNull)
    val newValues = scalaValues.map {
      case null     => null
      case contents => CypherRelationship(contents.id, contents.startId, contents.endId, contents.relationshipType, contents.properties)
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherRelationship.isIncomparable(v) should equal(v == null)
    }
  }

  test("NODE conversion") {
    val originalValues = NODE_valueGroups.flatten
    val scalaValues: Seq[NodeContents] = originalValues.map(CypherNode.contents).map(_.orNull)
    val newValues = scalaValues.map {
      case null     => null
      case contents => CypherNode(contents.id, contents.data.labels, contents.data.properties)
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherNode.isIncomparable(v) should equal(v == null)
    }
  }

  test("MAP conversion") {
    val originalValues = MAP_valueGroups.flatten
    val scalaValues = originalValues.map(CypherMap.contents).map(_.orNull)
    val newValues = scalaValues.map {
      case null     => null
      case contents => CypherMap(contents.properties)
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherMap.isIncomparable(v) should equal (v == null || CypherMap.unapply(v).map(_.properties.m).exists(_.values.exists(_ == null)))
    }
  }

  test("LIST conversion") {
    val originalValues = LIST_valueGroups.flatten
    val scalaValues = originalValues.map(CypherList.contents).map(_.orNull)
    val newValues = scalaValues.map {
      case null                 => null
      case l: Seq[CypherValue]  => CypherList(l)
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherList.isIncomparable(v) should equal (v == null || CypherList.unapply(v).exists(_.exists(_ == null)))
    }
  }

  test("STRING conversion") {
    val originalValues = STRING_valueGroups.flatten
    val scalaValues = originalValues.map(CypherString.contents).map(_.orNull)
    val newValues = scalaValues.map {
      case null                => null
      case s: java.lang.String => CypherString(s)
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherString.isIncomparable(v) should equal (v == null)
    }
  }

  test("BOOLEAN conversion") {
    val originalValues = BOOLEAN_valueGroups.flatten
    val scalaValues = originalValues.map(CypherBoolean.contents)
    val newValues = scalaValues.map {
      case None    => null
      case Some(b) => CypherBoolean(b)
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherBoolean.isIncomparable(v) should equal (v == null)
    }
  }

  test("INTEGER conversion") {
    val originalValues = INTEGER_valueGroups.flatten
    val scalaValues = originalValues.map(CypherInteger.contents).map(_.orNull)
    val newValues = scalaValues.map {
      case null              => null
      case l: java.lang.Long => CypherInteger(l)
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherInteger.isIncomparable(v) should equal (v == null)
    }
  }

  test("FLOAT conversion") {
    val originalValues = FLOAT_valueGroups.flatten
    val scalaValues = originalValues.map(CypherFloat.contents).map(_.orNull)
    val newValues = scalaValues.map {
      case null                => null
      case d: java.lang.Double => CypherFloat(d)
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherFloat.isIncomparable(v) should equal (v == null)
    }
  }

  test("NUMBER conversion") {
    val originalValues = NUMBER_valueGroups.flatten
    val scalaValues = originalValues.map(CypherNumber.contents).map(_.orNull)
    val newValues = scalaValues.map {
      case null                => null
      case l: java.lang.Long   => CypherInteger(l)
      case d: java.lang.Double => CypherFloat(d)
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherNumber.isIncomparable(v) should equal (v == null)
    }
  }

  test("ALL conversion") {
    val originalValues = ANY_valueGroups.flatten
    val scalaValues = originalValues.map(CypherValue.contents).map(_.orNull)
    val newValues = scalaValues.map {
      case null => null
      case b: java.lang.Boolean => CypherBoolean(b)
      case s: java.lang.String => CypherString(s)
      case l: java.lang.Long => CypherInteger(l)
      case r: RegularMap => CypherMap(r.properties)
      case n: NodeContents => CypherNode(n.id, n.labels, n.properties)
      case r: RelationshipContents => CypherRelationship(r.id, r.startId, r.endId, r.relationshipType, r.properties)
      case d: java.lang.Double => CypherFloat(d)
      case l: Seq[_] if isPathLike(l) => CypherPath(l.asInstanceOf[Seq[CypherEntityValue]]: _*)
      case l: Seq[_] => CypherList(l.asInstanceOf[Seq[CypherValue]])
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherValue.isIncomparable(v) should equal (v == null)
    }
  }
}
