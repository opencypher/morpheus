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
package org.opencypher.spark.impl.acceptance

import org.junit.runner.RunWith
import org.scalatest.Suites
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class Aggregation_ScanGraph extends AggregationBehaviour with ScanGraphInit

@RunWith(classOf[JUnitRunner])
class BoundedVarExpand_ScanGraph extends BoundedVarExpandBehaviour with ScanGraphInit

@RunWith(classOf[JUnitRunner])
class DrivingTable_ScanGraph extends DrivingTableBehaviour with ScanGraphInit

@RunWith(classOf[JUnitRunner])
class ExpandInto_ScanGraph extends ExpandIntoBehaviour with ScanGraphInit

@RunWith(classOf[JUnitRunner])
class Expression_ScanGraph extends ExpressionBehaviour with ScanGraphInit

@RunWith(classOf[JUnitRunner])
class Functions_ScanGraph extends FunctionsBehaviour with ScanGraphInit

@RunWith(classOf[JUnitRunner])
class Match_ScanGraph extends MatchBehaviour with ScanGraphInit

@RunWith(classOf[JUnitRunner])
class MultipleGraph_ScanGraph extends MultipleGraphBehaviour with ScanGraphInit

@RunWith(classOf[JUnitRunner])
class OptionalMatch_ScanGraph extends OptionalMatchBehaviour with ScanGraphInit

@RunWith(classOf[JUnitRunner])
class Predicate_ScanGraph extends PredicateBehaviour with ScanGraphInit

@RunWith(classOf[JUnitRunner])
class Return_ScanGraph extends ReturnBehaviour with ScanGraphInit

@RunWith(classOf[JUnitRunner])
class With_ScanGraph extends WithBehaviour with ScanGraphInit

@RunWith(classOf[JUnitRunner])
class Unwind_ScanGraph extends UnwindBehaviour with ScanGraphInit

@RunWith(classOf[JUnitRunner])
class CatalogDDL_ScanGraph extends CatalogDDLBehaviour with ScanGraphInit

@RunWith(classOf[JUnitRunner])
class Union_ScanGraph extends UnionBehaviour with ScanGraphInit
