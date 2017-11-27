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
package org.opencypher.caps.api.spark

object CAPSGraphGDLTestData {
  val `:Person` =
    """
      |(p1:Person:Swedish {name: "Mats", luckyNumber: 23L}),
      |(p2:Person {name: "Martin", luckyNumber: 42L}),
      |(p3:Person {name: "Max", luckyNumber: 1337L}),
      |(p4:Person {name: "Stefan", luckyNumber: 9L}),
    """.stripMargin

  // required to test conflicting input data
  val `:Brogrammer` =
    """
      |(pb1:Person:Brogrammer {language: "Node"}),
      |(pb2:Person:Brogrammer {language: "Coffeescript"}),
      |(pb3:Person:Brogrammer {language: "Javascript"}),
      |(pb4:Person:Brogrammer {language: "TypeScript"}),
    """.stripMargin

  val `:Programmer` =
    """
      |(pp1:Person:Programmer {name: "Alice",luckyNumber: 42,language: "C"}),
      |(pp2:Person:Programmer {name: "Bob",luckyNumber: 23,language: "D"}),
      |(pp3:Person:Programmer {name: "Eve",luckyNumber: 84,language: "F"}),
      |(pp4:Person:Programmer {name: "Carl",luckyNumber: 49,language: "R"}),
    """.stripMargin

  val `:Book` =
    """
      |(b1:Book {title: "1984", year: 1949l}),
      |(b2:Book {title: "Cryptonomicon", year: 1999l}),
      |(b3:Book {title: "The Eye of the World", year: 1990l}),
      |(b4:Book {title: "The Circle", year: 2013l}),
    """.stripMargin

  val `:KNOWS` =
    """
      |(p1)-[:KNOWS {since: 2017l}]->(p2),
      |(p1)-[:KNOWS {since: 2016l}]->(p3),
      |(p1)-[:KNOWS {since: 2015l}]->(p4),
      |(p2)-[:KNOWS {since: 2016l}]->(p3),
      |(p2)-[:KNOWS {since: 2013l}]->(p4),
      |(p3)-[:KNOWS {since: 2016l}]->(p4),
    """.stripMargin

  val `:READS` =
    """
      |(p1)-[:READS {recommends :true}]->(b1),
      |(p2)-[:READS {recommends :true}]->(b4),
      |(p3)-[:READS {recommends :true}]->(b3),
      |(p4)-[:READS {recommends :false}]->(b2),
    """.stripMargin

  val `:INFLUENCES` =
    """
      |(b1)-[:INFLUENCES]->(b2),
    """.stripMargin
}
