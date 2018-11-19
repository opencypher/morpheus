/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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

import org.junit.runner.RunWith
import org.scalatest.Suites
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SingleTableGraphAcceptanceTest extends Suites(
  Aggregation_SingleTableGraph,
  BoundedVarExpand_SingleTableGraph,
  ExpandInto_SingleTableGraph,
  Expression_SingleTableGraph,
  Functions_SingleTableGraph,
  Match_SingleTableGraph,
  MultipleGraph_SingleTableGraph,
  OptionalMatch_SingleTableGraph,
  Predicate_SingleTableGraph,
  Return_SingleTableGraph,
  With_SingleTableGraph,
  Unwind_SingleTableGraph,
  CatalogDDL_SingleTableGraph,
  DrivingTable_SingleTableGraph,
  Union_SingleTableGraph
)

/**
  * Objects that allow ScalaTest to extract decent names for the behaviours.
  *
  * This is verbose, but I found no way to instantiate dynamic traits at compile time without resorting to macros.
  */
object Aggregation_SingleTableGraph extends AggregationBehaviour with SingleTableGraphInit

object BoundedVarExpand_SingleTableGraph extends BoundedVarExpandBehaviour with SingleTableGraphInit

object DrivingTable_SingleTableGraph extends DrivingTableBehaviour with SingleTableGraphInit

object ExpandInto_SingleTableGraph extends ExpandIntoBehaviour with SingleTableGraphInit

object Expression_SingleTableGraph extends ExpressionBehaviour with SingleTableGraphInit

object Functions_SingleTableGraph extends FunctionsBehaviour with SingleTableGraphInit

object Match_SingleTableGraph extends MatchBehaviour with SingleTableGraphInit

object MultipleGraph_SingleTableGraph extends MultipleGraphBehaviour with SingleTableGraphInit

object OptionalMatch_SingleTableGraph extends OptionalMatchBehaviour with SingleTableGraphInit

object Predicate_SingleTableGraph extends PredicateBehaviour with SingleTableGraphInit

object Return_SingleTableGraph extends ReturnBehaviour with SingleTableGraphInit

object With_SingleTableGraph extends WithBehaviour with SingleTableGraphInit

object Unwind_SingleTableGraph extends UnwindBehaviour with SingleTableGraphInit

object CatalogDDL_SingleTableGraph extends CatalogDDLBehaviour with SingleTableGraphInit

object Union_SingleTableGraph extends UnionAllBehaviour with SingleTableGraphInit

