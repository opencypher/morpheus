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
package org.opencypher.okapi.relational.api.physical

import org.opencypher.okapi.api.graph.{PropertyGraph, QualifiedGraphName}
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.ir.api.block.SortItem
import org.opencypher.okapi.ir.api.expr.{Aggregator, Expr, Var}
import org.opencypher.okapi.logical.impl._
import org.opencypher.okapi.relational.impl.physical.{InnerJoin, JoinType}
import org.opencypher.okapi.relational.impl.table.{ProjectedExpr, ProjectedField, RecordHeader}

/**
  * Main interface to be implemented by custom (relational) back-ends to execute a Cypher query. Methods are being
  * called by [[org.opencypher.okapi.relational.impl.physical.PhysicalPlanner]], a default implementation for physical planning.
  *
  * @tparam P backend-specific physical operators
  * @tparam R backend-specific cypher records
  * @tparam G backend-specific property graph
  * @tparam C backend-specific runtime context
  */
trait PhysicalOperatorProducer[P <: PhysicalOperator[R, G, C], R <: CypherRecords, G <: PropertyGraph, C <: RuntimeContext[R, G]] {

  // Unary operators

  /**
    * Starts the query execution based on optional given records and an optional graph.
    *
    * @param qgnOpt qualified graph name of the input graph
    * @param in     backend-specific records
    * @return start operator
    */
  def planStart(qgnOpt: Option[QualifiedGraphName] = None, in: Option[R] = None): P

  /**
    * Scans the node set of the input graph and returns all nodes that match the given CTNode type.
    *
    * @param in      previous operator
    * @param inGraph graph to scan nodes from
    * @param v       node variable carrying the node type to scan for
    * @param header  resulting record header
    * @return node scan operator
    */
  def planNodeScan(in: P, inGraph: LogicalGraph, v: Var, header: RecordHeader): P

  /**
    * Scans the relationship set of the input graph and returns all relationships that match the given CTRelationship
    * type.
    *
    * @param in      previous operator
    * @param inGraph graph to scan relationships from
    * @param v       node variable carrying the relationship type to scan for
    * @param header  resulting record header
    * @return relationship scan operator
    */
  def planRelationshipScan(in: P, inGraph: LogicalGraph, v: Var, header: RecordHeader): P

  /**
    * Creates an empty record set thereby disregarding the input. The records are described by the given record header.
    *
    * @param in     previous operator
    * @param header record header describing the created records
    * @return empty records operator
    */
  def planEmptyRecords(in: P, header: RecordHeader): P

  /**
    * Renames the column identified by the given expression to the specified alias.
    *
    * @param in     previous operator
    * @param expr   expression to be aliased
    * @param alias  alias
    * @param header resulting record header
    * @return Alias operator
    */
  def planAlias(in: P, expr: Expr, alias: Var, header: RecordHeader): P

  /**
    * Drops the columns identified by the given expressions from the input records.
    * Expressions not present are ignored.
    *
    * @param in         previous operator
    * @param dropFields expressions to be dropped
    * @param header     resulting record header
    * @return Drop operator
    */
  def planDrop(in: P, dropFields: Seq[Expr], header: RecordHeader): P

  /**
    * The operator takes a set of (field, expression) aliases and renames the columns identified by a field to the
    * corresponding expression.
    *
    * @param in      previous operator
    * @param aliases set of aliases
    * @param header  resulting record header
    * @return remove aliases operator
    */
  def planRemoveAliases(in: P, aliases: Set[(ProjectedField, ProjectedExpr)], header: RecordHeader): P

  /**
    * Filters the incoming rows according to the specified expression.
    *
    * @param in     previous operator
    * @param expr   expression to be evaluated
    * @param header resulting record header
    * @return filter operator
    */
  def planFilter(in: P, expr: Expr, header: RecordHeader): P

  /**
    * Selects the specified fields from the given records.
    *
    * @param in     previous operator
    * @param fields fields to select from the records (i.e., as specified in the RETURN clause)
    * @param header resulting record header
    * @return select fields operator
    */
  def planSelectFields(in: P, fields: List[Var], header: RecordHeader): P

  /**
    * Returns the working graph
    *
    * @param in previous operator
    */
  def planReturnGraph(in: P): P

  /**
    * Use the specified graph.
    *
    * @param in    previous operator
    * @param graph graph to select from the catalog
    * @return select graph operator
    */
  def planFromGraph(in: P, graph: LogicalCatalogGraph): P

  /**
    * Evaluates the given expression and projects it to a new column in the input records.
    *
    * @param in     previous operator
    * @param expr   expression to evaluate
    * @param header resulting record header
    * @return project operator
    */
  def planProject(in: P, expr: Expr, header: RecordHeader): P

  /**
    * Creates a new record containing the specified entities (i.e. as defined in a construction pattern).
    *
    * @param table     table that contains cloned aliases and data for constructing new entities
    * @param onGraph   graph that we construct on
    * @param construct graph to construct
    * @return project pattern graph operator
    */
  def planConstructGraph(table: P, onGraph: P, construct: LogicalPatternGraph): P

  /**
    * Groups the underlying records by the specified expressions and evaluates the given aggregate functions.
    *
    * @param in           previous operator
    * @param group        vars to group records by
    * @param aggregations aggregate functions
    * @param header       resulting record header
    * @return aggregate operator
    */
  def planAggregate(in: P, group: Set[Var], aggregations: Set[(Var, Aggregator)], header: RecordHeader): P

  /**
    * Performs a distinct operation on the specified fields.
    *
    * @param in     previous operator
    * @param fields fields to compute distinct on
    * @return distinct operator
    */
  def planDistinct(in: P, fields: Set[Var]): P

  /**
    * Orders the underlying records by the given expressions.
    *
    * @param in        previous operator
    * @param sortItems fields to order records by
    * @param header    resulting record header
    * @return order by operator
    */
  def planOrderBy(in: P, sortItems: Seq[SortItem[Expr]], header: RecordHeader): P

  /**
    * Unwinds the given list of items into the specified var for each row in the input records.
    *
    * @param in     previous operator
    * @param list   list of items to unwind
    * @param item   var to project item to
    * @param header resulting record header
    * @return unwind operator
    */
  def planUnwind(in: P, list: Expr, item: Var, header: RecordHeader): P

  /**
    * Initializes the underlying records for a variable expand computation (e.g., (a)-[:A*1..3]->(b)).
    *
    * @param in       previous operator
    * @param source   variable to expand from (e.g. (a))
    * @param edgeList variable to identify column which later stores relationship identifiers of the computed paths
    * @param target   variable to expand into (e.g. (b))
    * @param header   resulting record header
    * @return init var expand operator
    */
  def planInitVarExpand(in: P, source: Var, edgeList: Var, target: Var, header: RecordHeader): P

  /**
    * Skips the given amount of rows in the input records. The number of rows is specified by an expression which can be
    * a literal or a query parameter.
    *
    * @param in     previous operator
    * @param expr   expression which contains or refers to the number of rows to skip
    * @param header resulting record header
    * @return skip operator
    */
  def planSkip(in: P, expr: Expr, header: RecordHeader): P

  /**
    * Limits the number of input records to the specified amount. The number of rows is specified by an expression which
    * can be a literal or a query parameter.
    *
    * @param in     previous operator
    * @param expr   expression which contains or refers to the maximum number of rows to return
    * @param header resulting record header
    * @return limit operator
    */
  def planLimit(in: P, expr: Expr, header: RecordHeader): P

  // Binary operators

  /**
    * Computes a cartesian product between the two input records.
    *
    * @param lhs    first previous operator
    * @param rhs    second previous operator
    * @param header resulting record header
    * @return cross operator
    */
  def planCartesianProduct(lhs: P, rhs: P, header: RecordHeader): P

  /**
    * Joins the two input records using the given expressions.
    *
    * @param lhs         first previous operator
    * @param rhs         second previous operator
    * @param joinColumns sequence of left and right join columns
    * @param header      resulting record header
    * @param joinType    type of the join
    * @return join operator
    */
  def planJoin(lhs: P, rhs: P, joinColumns: Seq[(Expr, Expr)], header: RecordHeader, joinType: JoinType = InnerJoin): P

  /**
    * Unions the input records.
    *
    * @param lhs first previous operator
    * @param rhs second previous operator
    * @return union operator
    */
  def planTabularUnionAll(lhs: P, rhs: P): P

  /**
    * Filters the rows of the first input by checking if there exists a corresponding row in the second input.
    *
    * @param lhs         first previous operator
    * @param rhs         second previous operator
    * @param targetField field that stores the (boolean) result of the evaluation
    * @param header      resulting record header
    * @return exists subquery operator
    */
  def planExistsSubQuery(lhs: P, rhs: P, targetField: Var, header: RecordHeader): P

  // Ternary operators

  /**
    * Performs a bounded variable length path expression.
    *
    * @param first          first previous operator
    * @param second         second previous operator
    * @param third          third previous operator
    * @param rel            relationship variable to expand from
    * @param edgeList       refers to the column in which the path is stored
    * @param target         node variable in the third input
    * @param initialEndNode initial end node
    * @param lower          lower bound
    * @param upper          upper bound
    * @param direction      path direction
    * @param header         resulting record header
    * @param isExpandInto   true, iff the target variable is solved and can be replaced by a filter
    * @return bounded var expand operator
    */
  def planBoundedVarExpand(
    first: P,
    second: P,
    third: P,
    rel: Var,
    edgeList: Var,
    target: Var,
    initialEndNode: Var,
    lower: Int,
    upper: Int,
    direction: Direction,
    header: RecordHeader,
    isExpandInto: Boolean): P

  // N-ary operators
  /**
    * Performs a UNION ALL over graphs.
    *
    * @param graphs graphs to perform UNION ALL over together
    * @param qgn    name for the union graph
    * @return union all operator
    */
  def planGraphUnionAll(graphs: List[P], qgn: QualifiedGraphName): P
}
