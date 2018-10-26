package org.opencypher.sql.ddl

import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.sql.ddl.Ddl.ColumnIdentifier
import org.scalatest.{FunSuite, Matchers}

class IrTest extends FunSuite with Matchers {

  case class Ir(
    graphs: Map[GraphName, Graph]
  )

  case class Graph(
    name: GraphName,
    graphType: GraphType,
    nodeMappings: Map[NodeViewKey, NodeMapping],
    edgeMappings: Map[EdgeViewKey, EdgeMapping]
  )

  case class GraphType(
    schema: Schema
  )

  sealed trait EntityMapping

  case class NodeMapping(
    nodeType: NodeType,
    view: ViewId,
    properties: PropertyMappings,
    environment: DbEnv
  ) extends EntityMapping {
    def key: NodeViewKey = NodeViewKey(nodeType, view)
  }

  case class EdgeMapping(
    edgeType: EdgeType,
    view: ViewId,
    start: EdgeSource,
    end: EdgeSource,
    properties: PropertyMappings,
    environment: DbEnv
  ) extends EntityMapping{
    def key: EdgeViewKey = EdgeViewKey(edgeType, view)
  }

  case class EdgeSource(
    nodes: NodeViewKey,
    joinPredicates: List[Join]
  )

  case class Join(
    nodeColumn: String,
    edgeColumn: String
  )

  case class DbEnv(
    dataSource: DataSourceConfig
  )

  case class DataSourceConfig()

  case class NodeViewKey(nodeType: Set[String], view: String)
  case class EdgeViewKey(edgeType: Set[String], view: String)

  type NodeType = Set[String]
  type EdgeType = Set[String]

  type ViewId = String
  type ColumnId = String

  type PropertyMappings = Map[String, String]
  type LabelKey = (String, Set[String])

  /*
   validate
   - name resolution
     x schema names
     x alias names
     - property names in mappings must exist
   - name conflicts
   - doubly mapped nodes/rels

   other
   - construct all property mappings (even if mapping to same name)

    */

  def transform(ddl: DdlDefinition): Ir = {

    val lookup = GraphDdl(ddl)
    val graphTypes = ddl.schemaDefinitions
      .mapValues(toGraphType(lookup))

    val inlineGraphTypes = ddl.graphDefinitions.keyBy(_.name)
      .mapValues(_.localSchemaDefinition)
      .mapValues(toGraphType(lookup))

    val graphs = ddl.graphDefinitions
      .map(toGraph(inlineGraphTypes, graphTypes)).keyBy(_.name)

    Ir(
      graphs = graphs
    )
  }

  def toGraphType(lookup: GraphDdl)(schema: SchemaDefinition): GraphType = {
    GraphType(
      schema = lookup.toSchema(schema)
    )
  }

  def toGraph(inlineTypes: Map[String, GraphType], graphTypes: Map[String, GraphType])(graph: GraphDefinition): Graph = {
    Graph(
      name = GraphName(graph.name),
      graphType = graph.maybeSchemaName
        .map(schemaName => graphTypes.getOrFail(schemaName, "Unresolved schema name"))
        .getOrElse(inlineTypes.getOrFail(graph.name, "Unresolved schema name")),
      nodeMappings = graph.nodeMappings.flatMap(toNodeMappings).keyBy(_.key),
      edgeMappings = graph.relationshipMappings.flatMap(toEdgeMappings).keyBy(_.key)
    )
  }

  def toNodeMappings(nmd: NodeMappingDefinition): Seq[NodeMapping] = {
    nmd.nodeToViewDefinitions.map { nvd =>
      NodeMapping(
        environment = DbEnv(DataSourceConfig()),
        nodeType = nmd.labelNames,
        view = nvd.viewName,
        properties = nvd.maybePropertyMapping.getOrElse(Map())
      )
    }
  }

  def toEdgeMappings(rmd: RelationshipMappingDefinition): Seq[EdgeMapping] = {
    rmd.relationshipToViewDefinitions.map { rvd =>
      EdgeMapping(
        environment = DbEnv(DataSourceConfig()),
        edgeType = Set(rmd.relType),
        view = rvd.viewDefinition.name,
        start = EdgeSource(
          nodes = NodeViewKey(
            nodeType = rvd.startNodeToViewDefinition.labelSet,
            view = rvd.startNodeToViewDefinition.viewDefinition.name
          ),
          joinPredicates = rvd.startNodeToViewDefinition.joinOn.joinPredicates.map(toJoin(
            nodeAlias = rvd.viewDefinition.alias,
            edgeAlias = rvd.startNodeToViewDefinition.viewDefinition.alias
          ))
        ),
        end = EdgeSource(
          nodes = NodeViewKey(
            nodeType = rvd.endNodeToViewDefinition.labelSet,
            view = rvd.endNodeToViewDefinition.viewDefinition.name
          ),
          joinPredicates = rvd.endNodeToViewDefinition.joinOn.joinPredicates.map(toJoin(
            nodeAlias = rvd.viewDefinition.alias,
            edgeAlias = rvd.endNodeToViewDefinition.viewDefinition.alias
          ))
        ),
        properties = rvd.maybePropertyMapping.getOrElse(Map())
      )
    }
  }

  def toJoin(nodeAlias: String, edgeAlias: String)(join: (ColumnIdentifier, ColumnIdentifier)): Join = {
    val (left, right) = join
    val (leftAlias, rightAlias) = (left.head, right.head)
    val (leftColumn, rightColumn) = (left.tail.mkString("."), right.tail.mkString("."))
    (leftAlias, rightAlias) match {
      case (`nodeAlias`, `edgeAlias`) => Join(nodeColumn = leftColumn, edgeColumn = rightColumn)
      case (`edgeAlias`, `nodeAlias`) => Join(nodeColumn = rightColumn, edgeColumn = leftColumn)
      case _                          =>
        val aliases = Set(nodeAlias, edgeAlias)
        if (!aliases.contains(leftAlias)) notFound("Unresolved alias", leftAlias, aliases)
        if (!aliases.contains(rightAlias)) notFound("Unresolved alias", rightAlias, aliases)
        failure(s"Unable to resolve aliases: $leftAlias, $rightAlias")
    }
  }

  def notFound(msg: String, needle: Any, haystack: Traversable[Any]): Nothing = ???

  def failure(msg: String): Nothing = ???

  implicit class ListOps[T](list: List[T]) {
    def keyBy[K](key: T => K): Map[K, T] = list.map(t => key(t) -> t).toMap
  }

  implicit class MapOps[K, V](map: Map[K, V]) {
    def getOrFail(key: K, msg: String): V = map.getOrElse(key, notFound(msg, key, map.keySet))
  }


  test("run") {

    val ddlString =
      s"""
         |SET SCHEMA dataSourceName.fooDatabaseName
         |
         |CREATE GRAPH SCHEMA fooSchema
         | LABEL (Person { name   : STRING })
         | LABEL (Book   { title  : STRING })
         | LABEL (READS  { rating : FLOAT  })
         | (Person)
         | (Book)
         | [READS]
         |
         |CREATE GRAPH fooGraph WITH GRAPH SCHEMA fooSchema
         |  NODE LABEL SETS (
         |    (Person) FROM personView ( person_name AS name )
         |    (Book)   FROM bookView   ( book_title AS title )
         |  )
         |  RELATIONSHIP LABEL SETS (
         |    (READS)
         |      FROM readsView e ( value AS rating )
         |        START NODES
         |          LABEL SET (Person) FROM personView p JOIN ON p.person_id = e.person
         |        END NODES
         |          LABEL SET (Book)   FROM bookView   b JOIN ON e.book = b.book_id
         |  )
     """.stripMargin

    import org.opencypher.okapi.api.types.{CTFloat, CTString}

    val ddl = DdlParser.parse(ddlString)
    val ir = transform(ddl)

    println(ir)

    val personKey = NodeViewKey(Set("Person"), "personView")
    val bookKey   = NodeViewKey(Set("Book"), "bookView")
    val readsKey  = EdgeViewKey(Set("READS"), "readsView")


    val expected = Ir(
      Map(
        GraphName("fooGraph") -> Graph(GraphName("fooGraph"),
          GraphType(Schema.empty
            .withNodePropertyKeys("Person")("name" -> CTString)
            .withNodePropertyKeys("Book")("title" -> CTString)
            .withRelationshipPropertyKeys("READS")("rating" -> CTFloat)
          ),
          Map(
            personKey -> NodeMapping(Set("Person"), "personView", Map("name" -> "person_name"), DbEnv(DataSourceConfig())),
            bookKey   -> NodeMapping(Set("Book"),   "bookView",   Map("title" -> "book_title"), DbEnv(DataSourceConfig()))
          ),
          Map(
            readsKey -> EdgeMapping(Set("READS"), "readsView",
              EdgeSource(personKey, List(Join("person", "person_id"))),
              EdgeSource(bookKey,   List(Join("book", "book_id"))),
              Map("rating" -> "value"),
              DbEnv(DataSourceConfig())
            )
          )
        )
      )
    )

    ir shouldEqual expected

  }

}
