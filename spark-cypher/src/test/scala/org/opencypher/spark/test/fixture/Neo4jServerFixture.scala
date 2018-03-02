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
 */
package org.opencypher.spark.test.fixture

import org.neo4j.harness.{ServerControls, TestServerBuilders}
import org.opencypher.okapi.test.BaseTestSuite
import org.opencypher.okapi.test.fixture.BaseTestFixture
import org.opencypher.spark.api.io.neo4j.Neo4jConfig

trait Neo4jServerFixture extends BaseTestFixture {
  self: SparkSessionFixture with BaseTestSuite =>

  var neo4jServer: ServerControls = _

  def neo4jConfig =
    Neo4jConfig(neo4jServer.boltURI(), user = "anonymous", password = Some("password"), encrypted = false)

  def neo4jHost: String = {
    val scheme = neo4jServer.boltURI().getScheme
    val userInfo = s"${neo4jConfig.user}:${neo4jConfig.password.get}@"
    val host = neo4jServer.boltURI().getAuthority
    s"$scheme://$userInfo$host"
  }

  def userFixture: String = "CALL dbms.security.createUser('anonymous', 'password', false)"

  def dataFixture: String

  abstract override def beforeAll(): Unit = {
    super.beforeAll()
    neo4jServer = TestServerBuilders
      .newInProcessBuilder()
      .withConfig("dbms.security.auth_enabled", "true")
      .withFixture(userFixture)
      .withFixture(dataFixture)
      .newServer()
  }

  abstract override def afterAll(): Unit = {
    neo4jServer.close()
    super.afterAll()
  }
}
