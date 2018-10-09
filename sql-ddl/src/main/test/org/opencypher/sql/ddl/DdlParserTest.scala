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
package org.opencypher.sql.ddl

import fastparse.core.Parsed.{Failure, Success}
import org.opencypher.sql.ddl.DdlParser.ddl

object DdlParserTest extends App {

  ddl.parse(testDdl) match {
    case Success(v, _) => v.show()
    case Failure(p, index, extra) =>
      val i = extra.input
      val before = index - math.max(index - 20, 0)
      val after = math.min(index + 20, i.length) - index
      println(extra.input.slice(index - before, index + after).replace('\n', ' '))
      println("~" * before + "^" + "~" * after)
      println(s"failed parser: $p at index $index")
      println(s"stack=${extra.traced.stack}")
      println(extra.traced.trace)
  }

  def testDdl =
    """|CREATE GRAPH interactions WITH SCHEMA (
       |  LABELS
       |    Customer
       |      PROPERTIES (
       |        customerIdx INTEGER,
       |        customerId STRING,
       |        customerName STRING
       |      ),
       |    Interaction
       |      PROPERTIES (
       |        interactionId INTEGER,
       |        date STRING,
       |        type STRING,
       |        outcomeScore STRING
       |      ),
       |    AccountHolder
       |      PROPERTIES (
       |        accountHolderId STRING
       |      ),
       |    Policy
       |      PROPERTIES (
       |        policyAccountNumber STRING
       |      ),
       |    CustomerRep
       |      PROPERTIES (
       |        empNo INTEGER,
       |        empName STRING
       |      )
       |
       |  -- Rel labels
       |  LABELS
       |    CUSTOMER_REP,
       |    CUSTOMER,
       |    POLICY,
       |    ACCOUNT_HOLDER
       |
       |  (Customer), (Interaction), (AccountHolder), (Policy), (CustomerRep),
       |  [CUSTOMER], [CUSTOMER_REP], [POLICY], [ACCOUNT_HOLDER]
       |
       |  (Interaction)-[CUSTOMER]-> <1> (Customer),
       |  (Interaction)-[ACCOUNT_HOLDER]-> <1> (AccountHolder),
       |  (Interaction)-[CUSTOMER_REP]-> <1> (CustomerRep),
       |  (Interaction)-[POLICY]-> <1> (Policy)
       |)
       |
       |    NODES (Customer) FROM interactions
       |    NODES (AccountHolder) FROM interactions
       |    NODES (CustomerRep) FROM interactions
       |    NODES (Policy) FROM interactions
       |    NODES (Interaction) FROM interactions
       |
       |    RELATIONSHIPS [CUSTOMER]
       |    FROM interactions
       |    MAPPING interactionId ONTO interactionId
       |    FOR START NODES (Interaction)
       |    MAPPING customerId ONTO customerId
       |    FOR END NODES (Customer)
       |
       |    RELATIONSHIPS [ACCOUNT_HOLDER]
       |    FROM interactions
       |    MAPPING interactionId ONTO interactionId
       |    FOR START NODES (Interaction)
       |    MAPPING accountHolderId ONTO accountHolderId
       |    FOR END NODES (AccountHolder)
       |
       |    RELATIONSHIPS [CUSTOMER_REP]
       |    FROM interactions
       |    MAPPING interactionId ONTO interactionId
       |    FOR START NODES (Interaction)
       |    MAPPING empNo ONTO empNo
       |    FOR END NODES (CustomerRep)
       |
       |    RELATIONSHIPS [POLICY]
       |    FROM interactions
       |    MAPPING interactionId ONTO interactionId
       |    FOR START NODES (Interaction)
       |    MAPPING policyAccountNumber ONTO policyAccountNumber
       |    FOR END NODES (Policy)
       |
       |""".stripMargin

}
