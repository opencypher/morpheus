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
package org.opencypher.spark

import org.apache.spark.sql.execution.SparkPlan
import org.opencypher.okapi.impl.util.Measurement
import org.opencypher.spark.testing.CAPSTestSuite

class SparkTests extends CAPSTestSuite {

  it("foo") {

  }

  // Reproduces https://issues.apache.org/jira/browse/SPARK-23855, which was relevant to Spark 2.2
  it("should correctly perform a join after a cross") {
    val df1 = sparkSession.createDataFrame(Seq(Tuple1(0L)))
      .toDF("a")

    val df2 = sparkSession.createDataFrame(Seq(Tuple1(1L)))
      .toDF("b")

    val df3 = sparkSession.createDataFrame(Seq(Tuple1(0L)))
      .toDF("c")

    val cross = df1.crossJoin(df2)
    cross.show()

    val joined = cross
      .join(df3, cross.col("a") === df3.col("c"))

    joined.show()

    val selected = joined.select("*")
    selected.show
  }

}
