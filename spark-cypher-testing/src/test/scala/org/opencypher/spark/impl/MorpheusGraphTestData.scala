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
package org.opencypher.spark.impl

object MorpheusGraphTestData {

  val `:Person`: String =
    """
      |CREATE (p1:Person {name: "Mats", luckyNumber: 23})
      |CREATE (p2:Person {name: "Martin", luckyNumber: 42})
      |CREATE (p3:Person {name: "Max", luckyNumber: 1337})
      |CREATE (p4:Person {name: "Stefan", luckyNumber: 9})
    """.stripMargin

  // required to test conflicting input data
  val `:Brogrammer`: String =
    """
      |CREATE (pb1:Person:Brogrammer {language: "Node"})
      |CREATE (pb2:Person:Brogrammer {language: "Coffeescript"})
      |CREATE (pb3:Person:Brogrammer {language: "Javascript"})
      |CREATE (pb4:Person:Brogrammer {language: "TypeScript"})
    """.stripMargin

  val `:Programmer`: String =
    """
      |CREATE (pp1:Person:Programmer {name: "Alice",luckyNumber: 42,language: "C"})
      |CREATE (pp2:Person:Programmer {name: "Bob",luckyNumber: 23,language: "D"})
      |CREATE (pp3:Person:Programmer {name: "Eve",luckyNumber: 84,language: "F"})
      |CREATE (pp4:Person:Programmer {name: "Carl",luckyNumber: 49,language: "R"})
    """.stripMargin

  val `:Book`: String =
    """
      |CREATE (b1:Book {title: "1984", year: 1949})
      |CREATE (b2:Book {title: "Cryptonomicon", year: 1999})
      |CREATE (b3:Book {title: "The Eye of the World", year: 1990})
      |CREATE (b4:Book {title: "The Circle", year: 2013})
    """.stripMargin

  val `:KNOWS`: String =
    """
      |CREATE (p1)-[:KNOWS {since: 2017}]->(p2)
      |CREATE (p1)-[:KNOWS {since: 2016}]->(p3)
      |CREATE (p1)-[:KNOWS {since: 2015}]->(p4)
      |CREATE (p2)-[:KNOWS {since: 2016}]->(p3)
      |CREATE (p2)-[:KNOWS {since: 2013}]->(p4)
      |CREATE (p3)-[:KNOWS {since: 2016}]->(p4)
    """.stripMargin

  val `:READS`: String =
    """
      |CREATE (p1)-[:READS {recommends :true}]->(b1)
      |CREATE (p2)-[:READS {recommends :true}]->(b4)
      |CREATE (p3)-[:READS {recommends :true}]->(b3)
      |CREATE (p4)-[:READS {recommends :false}]->(b2)
    """.stripMargin

  val `:INFLUENCES`: String =
    """
      |CREATE (b1)-[:INFLUENCES]->(b2)
    """.stripMargin
}
