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
    Annotation.labels[NodeWithoutAnnotation] should equal(Seq(classOf[NodeWithoutAnnotation].getSimpleName))
    Annotation.labels[NodeWithEmptyAnnotation] should equal(Seq.empty)
    Annotation.labels[NodeWithSingleAnnotation] should equal(Seq("One"))
    Annotation.labels[NodeWithMultipleAnnotations] should equal(Seq("One", "Two", "Three"))
  }

  test("read relationship type annotation") {
    Annotation.relType[RelWithoutAnnotation] should equal(classOf[RelWithoutAnnotation].getSimpleName.toUpperCase)
    Annotation.relType[RelWithAnnotation] should equal("One")
  }

  test("read more general static annotation") {
    Annotation.get[TestAnnotation, TestAnnotation] should equal(Some(TestAnnotation("Foo")))
  }

}
