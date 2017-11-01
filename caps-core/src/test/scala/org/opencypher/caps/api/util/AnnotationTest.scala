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
package org.opencypher.caps.api.util

import org.opencypher.caps.api.record._
import org.scalatest.{FunSuite, Matchers}

import scala.annotation.StaticAnnotation

case class NodeWithoutAnnotation(id: Long) extends Node

@Labels()
case class NodeWithEmptyAnnotation(id: Long) extends Node

@Labels("One")
case class NodeWithSingleAnnotation(id: Long) extends Node

@Labels("One", "Two", "Three")
case class NodeWithMultipleAnnotations(id: Long) extends Node

case class RelWithoutAnnotation(id: Long, source: Long, target: Long) extends Relationship

@Type("One")
case class RelWithAnnotation(id: Long, source: Long, target: Long) extends Relationship

@TestAnnotation("Foo")
case class TestAnnotation(foo: String) extends StaticAnnotation

class AnnotationTest extends FunSuite with Matchers {

  test("read node label annotation") {
    Annotation.labels[NodeWithoutAnnotation] should equal(
      Seq(classOf[NodeWithoutAnnotation].getSimpleName))
    Annotation.labels[NodeWithEmptyAnnotation] should equal(Seq.empty)
    Annotation.labels[NodeWithSingleAnnotation] should equal(Seq("One"))
    Annotation.labels[NodeWithMultipleAnnotations] should equal(Seq("One", "Two", "Three"))
  }

  test("read relationship type annotation") {
    Annotation.relType[RelWithoutAnnotation] should equal(
      classOf[RelWithoutAnnotation].getSimpleName.toUpperCase)
    Annotation.relType[RelWithAnnotation] should equal("One")
  }

  test("read more general static annotation") {
    Annotation.get[TestAnnotation, TestAnnotation] should equal(Some(TestAnnotation("Foo")))
  }

}
