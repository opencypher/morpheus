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
package org.opencypher.spark.impl.acceptance

import org.scalatest.Suites

class CAPSScanGraphAcceptanceTest extends Suites(
  Aggregation_ScanGraph,
  BoundedVarExpand_ScanGraph,
  ExpandInto_ScanGraph,
  Expression_ScanGraph,
  Functions_ScanGraph,
  Match_ScanGraph,
  MultipleGraph_ScanGraph,
  OptionalMatch_ScanGraph,
  Predicate_ScanGraph,
  Return_ScanGraph,
  With_ScanGraph,
  Unwind_ScanGraph,
  CatalogDDL_ScanGraph
)

/**
  * Objects that allow ScalaTest to extract decent names for the behaviours.
  *
  * This is verbose, but I found no way to instantiate dynamic traits at compile time without resorting to macros.
  */
object Aggregation_ScanGraph extends AggregationBehaviour with ScanGraphInit

object BoundedVarExpand_ScanGraph extends BoundedVarExpandBehaviour with ScanGraphInit

object ExpandInto_ScanGraph extends ExpandIntoBehaviour with ScanGraphInit

object Expression_ScanGraph extends ExpressionBehaviour with ScanGraphInit

object Functions_ScanGraph extends FunctionsBehaviour with ScanGraphInit

object Match_ScanGraph extends MatchBehaviour with ScanGraphInit

object MultipleGraph_ScanGraph extends MultipleGraphBehaviour with ScanGraphInit

object OptionalMatch_ScanGraph extends OptionalMatchBehaviour with ScanGraphInit

object Predicate_ScanGraph extends PredicateBehaviour with ScanGraphInit

object Return_ScanGraph extends ReturnBehaviour with ScanGraphInit

object With_ScanGraph extends WithBehaviour with ScanGraphInit

object Unwind_ScanGraph extends UnwindBehaviour with ScanGraphInit

object CatalogDDL_ScanGraph extends CatalogDDLBehaviour with ScanGraphInit
