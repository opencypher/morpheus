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
package org.opencypher.caps.impl.io

import java.net.URI

import org.opencypher.caps.api.io.{GraphSource, GraphSourceFactory, GraphSourceFactoryCompanion}
import org.opencypher.caps.api.spark.CAPSSession
import org.opencypher.caps.impl.exception.Raise

abstract class GraphSourceFactoryImpl[Source <: GraphSource](val companion: GraphSourceFactoryCompanion)
  extends GraphSourceFactory {

  override final val name: String = getClass.getSimpleName

  override final def schemes: Set[String] = companion.supportedSchemes

  override final def sourceFor(uri: URI)(implicit capsSession: CAPSSession): Source =
    if (schemes.contains(uri.getScheme)) sourceForURIWithSupportedScheme(uri)
    else Raise.graphSourceSchemeNotSupported(uri, schemes)

  protected def sourceForURIWithSupportedScheme(uri: URI)(implicit capsSession: CAPSSession): Source
}
