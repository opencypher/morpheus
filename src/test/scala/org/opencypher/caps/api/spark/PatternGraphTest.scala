package org.opencypher.caps.api.spark

import org.apache.spark.sql.Row
import org.opencypher.caps.api.record.{NodeScan, RecordHeader, RelationshipScan}
import org.opencypher.caps.api.types.{CTNode, CTRelationship}
import org.opencypher.caps.test.CAPSTestSuite

class PatternGraphTest extends CAPSTestSuite {

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

  test("Construct pattern graph from single node CAPSRecords") {
    val inputGraph = TestGraph(`:Person`).graph
    val inputNodes = inputGraph.nodes("n")

    val patternGraph = CAPSGraph.create(inputNodes, inputGraph.schema, inputGraph.tokens)
    val outputNodes = patternGraph.nodes("n")

    outputNodes.details.toDF().columns should equal(Array(
      "n",
      "____n:Person",
      "____n:Swedish",
      "____n_dot_nameSTRING",
      "____n_dot_luckyNumberINTEGER"
    ))

    outputNodes.details.toDF().collect().toSet should equal (Set(
      Row(0, true, true,    "Mats",   23),
      Row(1, true, false, "Martin",   42),
      Row(2, true, false,    "Max", 1337),
      Row(3, true, false, "Stefan",    9)
    ))
  }

  test("Construct graph from multiple node scans") {
    val inputGraph = TestGraph(`:Person` + `:Book`).graph
    val inputNodes = inputGraph.nodes("n")

    val patternGraph = CAPSGraph.create(inputNodes, inputGraph.schema, inputGraph.tokens)
    val outputNodes = patternGraph.nodes("n")

    outputNodes.details.toDF().columns should equal(Array(
      "n",
      "____n:Person",
      "____n:Swedish",
      "____n:Book",
      "____n_dot_nameSTRING",
      "____n_dot_luckyNumberINTEGER",
      "____n_dot_yearINTEGER",
      "____n_dot_titleSTRING"
    ))

    outputNodes.details.toDF().collect().toSet should equal(Set(
      Row(0,  true,  true,  false,   "Mats",   23, null,                   null),
      Row(1,  true,  false, false, "Martin",   42, null,                   null),
      Row(2,  true,  false, false,    "Max", 1337, null,                   null),
      Row(3,  true,  false, false, "Stefan",    9, null,                   null),
      Row(4, false,  false,  true,     null, null, 1949,                 "1984"),
      Row(5, false,  false,  true,     null, null, 1999,        "Cryptonomicon"),
      Row(6, false,  false,  true,     null, null, 1990, "The Eye of the World"),
      Row(7, false,  false,  true,     null, null, 2013,           "The Circle")
    ))
  }

  test("Construct graph from multiple connected nodes") {
    val inputGraph = TestGraph(`:Person` + `:Book` + `:READS`).graph

    val books = inputGraph.nodes("b", CTNode("Book"))
    val booksDf = books.details.toDF().as("b")
    val reads = inputGraph.relationships("r", CTRelationship("READS"))
    val readsDf = reads.details.toDF().as("r")
    val persons = inputGraph.nodes("p", CTNode("Person"))
    val personsDf = persons.details.toDF().as("p")

    val joinedDf = personsDf
      .join(readsDf, personsDf.col("p") === readsDf.col("____source(r)"))
      .join(booksDf, readsDf.col("____target(r)") === booksDf.col("b"))

    val slots = persons.details.header.slots ++ reads.details.header.slots ++ books.details.header.slots
    val joinHeader = RecordHeader.from(slots.map(_.content): _*)

    val baseRecords = CAPSRecords.create(joinHeader, joinedDf)

    val patternGraph = CAPSGraph.create(baseRecords, inputGraph.schema, inputGraph.tokens)
    val outputNodes = patternGraph.nodes("n")
    outputNodes.details.toDF().show()

    outputNodes.details.toDF().columns should equal(Array(
      "n",
      "____n:Person",
      "____n:Swedish",
      "____n:Book",
      "____n_dot_nameSTRING",
      "____n_dot_luckyNumberINTEGER",
      "____n_dot_yearINTEGER",
      "____n_dot_titleSTRING"
    ))

    outputNodes.details.toDF().collect().toSet should equal(Set(
      Row(0,  true,  true,  false,   "Mats",   23, null,                   null),
      Row(1,  true,  false, false, "Martin",   42, null,                   null),
      Row(2,  true,  false, false,    "Max", 1337, null,                   null),
      Row(3,  true,  false, false, "Stefan",    9, null,                   null),
      Row(4, false,  false,  true,     null, null, 1949,                 "1984"),
      Row(5, false,  false,  true,     null, null, 1999,        "Cryptonomicon"),
      Row(6, false,  false,  true,     null, null, 1990, "The Eye of the World"),
      Row(7, false,  false,  true,     null, null, 2013,           "The Circle")
    ))
  }

  test("Node scan over a node type from column with multiple node types") {
    val inputGraph = TestGraph(`:Person` + `:Book`).graph
    val inputNodes = inputGraph.nodes("n")

    val patternGraph = CAPSGraph.create(inputNodes, inputGraph.schema, inputGraph.tokens)
    val outputNodes = patternGraph.nodes("n", CTNode("Person"))

    outputNodes.details.toDF().columns should equal(Array(
      "n",
      "____n:Person",
      "____n:Swedish",
      "____n_dot_nameSTRING",
      "____n_dot_luckyNumberINTEGER"
    ))

    outputNodes.details.toDF().collect().toSet should equal(Set(
      Row(0,  true,  true,   "Mats",   23),
      Row(1,  true,  false, "Martin",   42),
      Row(2,  true,  false,    "Max", 1337),
      Row(3,  true,  false, "Stefan",    9)
    ))
  }

//
//  test("Construct graph from single node and single relationship scan") {
//    val graph = CAPSGraph.create(`:Person`, `:KNOWS`)
//    val rels  = graph.relationships("e")
//
//    rels.details.toDF().columns should equal(Array(
//      "____source(e)",
//      "e",
//      "____type(e)",
//      "____target(e)",
//      "____e_dot_sinceINTEGER"
//    ))
//
//    rels.details.toDF().collect().toSet should equal(Set(
//      Row(1, 1, 0, 2, 2017),
//      Row(1, 2, 0, 3, 2016),
//      Row(1, 3, 0, 4, 2015),
//      Row(2, 4, 0, 3, 2016),
//      Row(2, 5, 0, 4, 2013),
//      Row(3, 6, 0, 4, 2016)
//    ))
//  }
//
//  test("Extract all node scans") {
//    val graph = CAPSGraph.create(`:Person`, `:Book`)
//
//    val nodes = graph.nodes("n", CTNode())
//
//    nodes.details.toDF().columns should equal(Array(
//      "n",
//      "____n:Person",
//      "____n:Swedish",
//      "____n:Book",
//      "____n_dot_nameSTRING",
//      "____n_dot_lucky_bar_numberINTEGER",
//      "____n_dot_titleSTRING",
//      "____n_dot_yearINTEGER"
//    ))
//
//    nodes.details.toDF().collect().toSet should equal(Set(
//      Row( 1,  true,  true,  false,   "Mats",   23,                   null, null),
//      Row( 2,  true,  false, false, "Martin",   42,                   null, null),
//      Row( 3,  true,  false, false,    "Max", 1337,                   null, null),
//      Row( 4,  true,  false, false, "Stefan",    9,                   null, null),
//      Row(10, false,  false,  true,     null, null,                 "1984", 1949),
//      Row(20, false,  false,  true,     null, null,        "Cryptonomicon", 1999),
//      Row(30, false,  false,  true,     null, null, "The Eye of the World", 1990),
//      Row(40, false,  false,  true,     null, null,           "The Circle", 2013)
//    ))
//  }
//
//  test("Extract node scan subset") {
//    val graph = CAPSGraph.create(`:Person`, `:Book`)
//
//    val nodes = graph.nodes("n", CTNode("Person"))
//
//    nodes.details.toDF().columns should equal(Array(
//      "n",
//      "____n:Person",
//      "____n:Swedish",
//      "____n_dot_nameSTRING",
//      "____n_dot_lucky_bar_numberINTEGER"
//    ))
//
//    nodes.details.toDF().collect().toSet should equal (Set(
//      Row(1, true, true,    "Mats",   23),
//      Row(2, true, false, "Martin",   42),
//      Row(3, true, false,    "Max", 1337),
//      Row(4, true, false, "Stefan",    9)
//    ))
//  }
//
//  test("Extract all relationship scans") {
//    val graph = CAPSGraph.create(`:Person`, `:Book`, `:KNOWS`, `:READS`)
//
//    val rels  = graph.relationships("e")
//
//    rels.details.toDF().columns should equal(Array(
//      "____source(e)",
//      "e",
//      "____type(e)",
//      "____target(e)",
//      "____e_dot_sinceINTEGER",
//      "____e_dot_recommendsBOOLEAN"
//    ))
//
//    rels.details.toDF().collect().toSet should equal(Set(
//      // :KNOWS
//      Row(1, 1, 0, 2, 2017, null),
//      Row(1, 2, 0, 3, 2016, null),
//      Row(1, 3, 0, 4, 2015, null),
//      Row(2, 4, 0, 3, 2016, null),
//      Row(2, 5, 0, 4, 2013, null),
//      Row(3, 6, 0, 4, 2016, null),
//      // :READS
//      Row(1, 100, 1, 10, null, true),
//      Row(2, 200, 1, 40, null, true),
//      Row(3, 300, 1, 30, null, true),
//      Row(4, 400, 1, 20, null, false)
//    ))
//  }
//
//  test("Extract relationship scan subset") {
//    val graph = CAPSGraph.create(`:Person`, `:Book`, `:KNOWS`, `:READS`)
//
//    val rels  = graph.relationships("e", CTRelationship("KNOWS"))
//
//    rels.details.toDF().columns should equal(Array(
//      "____source(e)",
//      "e",
//      "____type(e)",
//      "____target(e)",
//      "____e_dot_sinceINTEGER"
//    ))
//
//    rels.details.toDF().collect().toSet should equal(Set(
//      Row(1, 1, 0, 2, 2017),
//      Row(1, 2, 0, 3, 2016),
//      Row(1, 3, 0, 4, 2015),
//      Row(2, 4, 0, 3, 2016),
//      Row(2, 5, 0, 4, 2013),
//      Row(3, 6, 0, 4, 2016)
//    ))
//  }
//
//  test("Extract relationship scan strict subset") {
//    val graph = CAPSGraph.create(`:Person`, `:Book`, `:KNOWS`, `:READS`, `:INFLUENCES`)
//
//    val rels  = graph.relationships("e", CTRelationship("KNOWS", "INFLUENCES"))
//
//    rels.details.toDF().columns should equal(Array(
//      "____source(e)",
//      "e",
//      "____type(e)",
//      "____target(e)",
//      "____e_dot_sinceINTEGER"
//    ))
//
//    rels.details.toDF().collect().toSet should equal(Set(
//      // :KNOWS
//      Row(1, 1, 0, 2, 2017),
//      Row(1, 2, 0, 3, 2016),
//      Row(1, 3, 0, 4, 2015),
//      Row(2, 4, 0, 3, 2016),
//      Row(2, 5, 0, 4, 2013),
//      Row(3, 6, 0, 4, 2016),
//      // :INFLUENCES
//      Row(10, 1000, 2, 20, null)
//    ))
//  }
//
//  test("Extract from scans with overlapping labels") {
//    val graph = CAPSGraph.create(`:Person`, `:Programmer`)
//
//    val nodes = graph.nodes("n", CTNode("Person"))
//
//    nodes.details.toDF().columns should equal(Array(
//      "n",
//      "____n:Person",
//      "____n:Swedish",
//      "____n:Programmer",
//      "____n_dot_languageSTRING",
//      "____n_dot_nameSTRING",
//      "____n_dot_lucky_bar_numberINTEGER"
//    ))
//
//    nodes.details.toDF().collect().toSet should equal (Set(
//      Row(1,   true, true,  false, null,   "Mats",   23),
//      Row(2,   true, false, false, null, "Martin",   42),
//      Row(3,   true, false, false, null,    "Max", 1337),
//      Row(4,   true, false, false, null, "Stefan",    9),
//      Row(100, true, false, true,   "C",  "Alice",   42),
//      Row(200, true, false, true,   "D",    "Bob",   23),
//      Row(300, true, false, true,   "F",    "Eve",   84),
//      Row(400, true, false, true,   "R",   "Carl",   49)
//    ))
//  }
//
//  test("Extract from scans with implied label but missing keys") {
//    val graph = CAPSGraph.create(`:Person`, `:Brogrammer`)
//
//    val nodes = graph.nodes("n", CTNode("Person"))
//
//    nodes.details.toDF().columns should equal(Array(
//      "n",
//      "____n:Person",
//      "____n:Swedish",
//      "____n:Brogrammer",
//      "____n_dot_nameSTRING",
//      "____n_dot_lucky_bar_numberINTEGER",
//      "____n_dot_languageSTRING"
//    ))
//
//    nodes.details.toDF().collect().toSet should equal(Set(
//      Row(1, true, true, false, "Mats", 23, null),
//      Row(2, true, false, false, "Martin", 42, null),
//      Row(3, true, false, false, "Max", 1337, null),
//      Row(4, true, false, false, "Stefan", 9, null),
//      Row(100, true, false, true, null, null, "Node"),
//      Row(200, true, false, true, null, null, "Coffeescript"),
//      Row(300, true, false, true, null, null, "Javascript"),
//      Row(400, true, false, true, null, null, "Typescript")
//    ))
//  }
}
