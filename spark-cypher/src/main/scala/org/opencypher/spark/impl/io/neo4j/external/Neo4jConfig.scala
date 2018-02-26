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
package org.opencypher.spark.impl.io.neo4j.external

import java.net.URI

import org.neo4j.driver.v1.{AuthTokens, Config, Driver, GraphDatabase}

case class Neo4jConfig(uri: URI,
                       user: String = "",
                       password: Option[String] = None,
                       encrypted: Boolean = true) {

  def boltConfig(): Config = {
    val builder = Config.build

    if(encrypted)
      builder.withEncryption().toConfig
    else
      builder.withoutEncryption().toConfig
  }

  def driver(config: Neo4jConfig) : Driver = config.password match {
    case Some(pwd) => GraphDatabase.driver(config.uri, AuthTokens.basic(config.user, pwd), boltConfig())
    case _ => GraphDatabase.driver(config.uri, boltConfig())
  }

  def driver() : Driver = driver(this)

  def driver(url: String): Driver = GraphDatabase.driver(url, boltConfig())
}
