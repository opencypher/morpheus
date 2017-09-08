/**
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
package org.opencypher.caps

import org.opencypher.caps.api.ir.global.TokenRegistry
import org.opencypher.caps.api.spark.CAPSSession
import org.scalatest.BeforeAndAfterEach

object CAPSTestSession {
  trait Fixture {
    self: SparkTestSession.Fixture with BeforeAndAfterEach =>

    implicit lazy val caps: CAPSSession = initCAPSSessionBuilder.build

    def initCAPSSessionBuilder: CAPSSession.Builder = CAPSSession.builder(session)

    def initialTokens: TokenRegistry = {
      TokenRegistry.empty
    }

    override protected def afterEach(): Unit = caps.unmountAll()
  }
}
