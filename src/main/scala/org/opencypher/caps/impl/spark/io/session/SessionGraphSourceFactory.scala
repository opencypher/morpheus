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
package org.opencypher.caps.impl.spark.io.session

import java.net.URI

import org.opencypher.caps.api.spark.io._
import org.opencypher.caps.api.spark.CAPSSession
import org.opencypher.caps.impl.spark.io.CAPSGraphSourceFactoryImpl
import org.opencypher.caps.impl.spark.exception.Raise

import scala.collection.mutable

case object SessionGraphSourceFactory extends CAPSGraphSourceFactoryCompanion("session")

case class SessionGraphSourceFactory(mountPoints: mutable.Map[String, CAPSGraphSource] = mutable.Map.empty)
  extends CAPSGraphSourceFactoryImpl[CAPSGraphSource](SessionGraphSourceFactory) {

  def mountSourceAt(existingSource: CAPSGraphSource, uri: URI)(implicit capsSession: CAPSSession): Unit =
    if (schemes.contains(uri.getScheme))
      withValidPath(uri) { (path: String) =>
        mountPoints.get(path) match {
          case Some(source) =>
            Raise.graphAlreadyExists(uri)

          case _ =>
            mountPoints.put(path, existingSource)
        }
      }
    else
      Raise.graphSourceSchemeNotSupported(uri, schemes)

  def unmountAll(implicit capsSession: CAPSSession): Unit =
    mountPoints.clear()

  override protected def sourceForURIWithSupportedScheme(uri: URI)(implicit capsSession: CAPSSession): CAPSGraphSource =
    withValidPath(uri) { (path: String) =>
      mountPoints.get(path) match {
        case Some(source) =>
          source

        case _ =>
          val newSource = SessionGraphSource(path)
          mountPoints.put(path, newSource)
          newSource
      }
    }

  private def withValidPath[T](uri: URI)(f: String => T): T = {
    val path = uri.getPath
    if (uri.getUserInfo != null ||
      uri.getHost != null ||
      uri.getPort != -1 ||
      uri.getQuery != null ||
      uri.getAuthority != null ||
      uri.getFragment != null ||
      path == null ||
      !path.startsWith("/"))
      Raise.graphURIMalformedForUseBy(uri, "session graph source factory")
    else
      f(path)
  }
}


