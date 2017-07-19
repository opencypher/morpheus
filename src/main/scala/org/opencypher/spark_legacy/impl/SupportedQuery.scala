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
package org.opencypher.spark_legacy.impl

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

sealed trait Rel {
  def relType: String
  def cypher(name: String): String
  def source(frame: String): Column
  def target(frame: String): Column
}

final case class In(relType: String) extends Rel {
  override def cypher(name: String) = s"<-[$name:$relType]-"
  override def source(frame: String): Column = col(s"$frame.endId")
  override def target(frame: String): Column = col(s"$frame.startId")
}

final case class Out(relType: String) extends Rel {
  override def cypher(name: String) = s"-[$name:$relType]->"
  override def source(frame: String): Column = col(s"$frame.startId")
  override def target(frame: String): Column = col(s"$frame.endId")
}

sealed trait SupportedQuery {
  def colonList(tokens: IndexedSeq[String]) = s"${tokens.mkString(":", ":", "")}"
  def cypher: String

  override def toString: String = cypher
}

object BoundVariableLength {
  def apply(lowerBound: Int, upperBound: Int, startLabels: String*): BoundVariableLength =
    new BoundVariableLength(startLabels.toIndexedSeq, lowerBound, upperBound)
}

case class FixedLengthPattern(start: String, steps: Seq[(Rel, String)]) extends SupportedQuery {
  override def cypher: String = {
    val namedSteps = steps.zipWithIndex.map { case ((rel, label), index) => (s"r${index+1}", rel, label) }
    val expansion = namedSteps.map { case (name, rel, label) => s"${rel.cypher(name)}(:$label)" }.mkString
    val sum = namedSteps.map { case (name, _, _) => s"id($name)" }.mkString(" + ")
    s"MATCH (:$start)$expansion RETURN $sum"
  }
}

case class MidPattern(label1: String, type1: String, label2: String, type2: String, label3: String) extends SupportedQuery {

  override def cypher: String = {
    val sl = label1
    val t1 = type1
    val l = label2
    val t2 = type2
    val el = label3

    s"MATCH (:$sl)-[r1:$t1]->(:$l)-[r2:$t2]->(:$el) RETURN id(r1), id(r2)"
  }
}

case class BoundVariableLength(startLabels: IndexedSeq[String] = IndexedSeq.empty, lowerBound: Int, upperBound: Int) extends SupportedQuery {
  override def cypher: String = {
    val l = colonList(startLabels)
    val f = lowerBound
    val t = upperBound

    s"MATCH (a$l)-[r*$f..$t]->() RETURN r"
  }
}

case class NodeScan(labels: IndexedSeq[String] = IndexedSeq.empty) extends SupportedQuery {
  override def cypher: String = {
    val l = colonList(labels)

    s"MATCH (n$l) RETURN (n)"
  }
}

case class NodeScanWithProjection(labels: IndexedSeq[String] = IndexedSeq.empty,
                                  firstKey: Symbol = 'name, secondKey: Symbol = 'age) extends SupportedQuery {
  override def cypher: String = {
    val l = colonList(labels)
    val n1 = firstKey.name
    val n2 = secondKey.name

    s"MATCH (n$l) RETURN n.$n1, n.$n2"
  }
}

case class SimplePattern(startLabels: IndexedSeq[String] = IndexedSeq.empty, types: IndexedSeq[String] = IndexedSeq.empty,
                         endLabels: IndexedSeq[String] = IndexedSeq.empty) extends SupportedQuery {
  override def cypher: String = {
    val s = colonList(startLabels)
    val t = colonList(types)
    val e = colonList(endLabels)

    s"MATCH ($s)-[r$t]->($e) RETURN r"
  }
}

case class SimplePatternIds(startLabels: IndexedSeq[String] = IndexedSeq.empty, types: IndexedSeq[String] = IndexedSeq.empty,
                            endLabels: IndexedSeq[String] = IndexedSeq.empty) extends SupportedQuery {
  override def cypher: String = {
    val s = colonList(startLabels)
    val t = colonList(types)
    val e = colonList(endLabels)

    s"MATCH ($s)-[r$t]->($e) RETURN id(r)"
  }
}

case class SimpleUnionAll(lhsLabels: IndexedSeq[String] = IndexedSeq.empty, lhsKey: Symbol,
                          rhsLabels: IndexedSeq[String] = IndexedSeq.empty, rhsKey: Symbol) extends SupportedQuery {
  override def cypher: String = {
    val l = colonList(lhsLabels)
    val lk = lhsKey.name
    val r = colonList(rhsLabels)
    val rk = rhsKey.name

    s"MATCH (a$l) RETURN a.$lk AS name UNION ALL MATCH (b$r) RETURN b.$rk AS name"
  }
}

case class NodeScanIdsSorted(labels: IndexedSeq[String] = IndexedSeq.empty) extends SupportedQuery {
  override def cypher: String = {
    val l = colonList(labels)

    s"MATCH (n$l) RETURN id(n) AS id ORDER BY id DESC"
  }
}

case class CollectNodeProperties(labels: IndexedSeq[String] = IndexedSeq.empty, key: Symbol = 'name) extends SupportedQuery {
  override def cypher: String = {
    val l = colonList(labels)
    val k = key.name

    s"MATCH (a$l) RETURN collect(a.$k) AS names"
  }
}

case class CollectAndUnwindNodeProperties(labels: IndexedSeq[String] = IndexedSeq.empty, key: Symbol = 'name, column: Symbol) extends SupportedQuery {
  override def cypher: String = {
    val l = colonList(labels)
    val k = key.name
    val c = column.name

    s"MATCH (a$l) WITH collect(a.$k) AS names UNWIND names AS $c RETURN $c"
  }
}

case class MatchOptionalExpand(startLabels: IndexedSeq[String] = IndexedSeq.empty, types: IndexedSeq[String] = IndexedSeq.empty,
                               endLabels: IndexedSeq[String] = IndexedSeq.empty) extends SupportedQuery {
  override def cypher: String = {
    val s = colonList(startLabels)
    val t = colonList(types)
    val e = colonList(endLabels)

    s"MATCH (a$s) OPTIONAL MATCH (a)-[r$t]->(b$e) RETURN r"
  }
}
