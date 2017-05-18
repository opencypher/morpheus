import org.opencypher.spark.api.ir.Field
import org.opencypher.spark.api.record.RecordHeader


/*


  Stage       Tabular              Graphic

  Physical    Table                Graph

  Flat        Table Header         Graph Schema | nodes(v) -> nodes(v:Person)

  Logical     Var Signature        Graph Schema

  IR is DAG   Field Signature      Graph Schema




  Operator Layout Question:

  Variation 1: Nested Operators

  Graphic Operator: (G, T) => (G', T')
  - customized with -
  Tabular Operator: T => T'

  Pro: Naturally mirrors query structure
  Contra: Nesting, two classes of operators, cross graph operator optimization may be tricky?

  Variation 2: Uniform Operators

  Graphic Operator only: (G, T) => (G', T')

  Pro: Least change, Composeable
  Contra: Many operators will produce/expose a graph that is not used or need to union child graphs implicitly

  Variation 3: Fine grained/Multisorted operators

  Operators may take both table and graph childs and produce either a table or a graph

  Pro: Very precise, very high-level of control
  Contra: Everything is a DAG (!)

  Graph Registry:

  G0 := ...
  G1 := ..


  G2 := Expand(G0, TL, ...Plan...(G1))


   IN GRAPH g1
   MATCH (a)
   WHERE a:Label
   IN GRAPH g2
   MATCH (a)-[r]->(b)


   G1 ->
     Filter(a)
     NodeScan(a)
   G1 <-

   Then
   G1 <-
   Switch to G2
     Expand(a, r, b)
   G2 <-


   Blocks and their dependencies: Tabular structure only, parameterized with an explicit context graph

   block(-)
   block
   block(id_x)

   loadGraph(configuration) {
      default
   }



   id: matchblock(g4) {
      a = anyNode()
      b = anyNode()
      r = anyRel()
      connected(a, r, b)
   }

   id2: buildgraph {
    ...
    out_graph(g4)
   }

   default: schema
   g4: schema

   id3: matchblock(id2)


   Map { g2: schema, g4: schema }

   Expand@G2(Filter@G1(NodeScan@G1(a), a:Label), r, NodeScan@G2(b))

   LoadGraph()

   default
   MATCH....
   LOAD GRAPH ...
   MATCH
   LOAD GRAPH ...
   MATCH ..
   RETURN


   G1 : = ...

   MATCH (a)
   ...
   ...
   ..



   def plan(foo:logical.Thing) = foo match {

      case graphic(tabular, graph) =>
          plan(tabular, graph)

          flatGraphPlan(flatTabularPlan)



   }

   def planTabular(foo: logicalTAbluar, g: Graph) = foo match {

        case tabular =>
            // access graph

            flatTabularPlan

 */
