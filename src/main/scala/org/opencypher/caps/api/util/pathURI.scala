package org.opencypher.caps.api.util

import java.net.URI

import org.opencypher.caps.impl.spark.io.session.SessionGraphSourceFactory

case object pathURI extends (String => URI) {
  def apply(path: String): URI =
    URI.create(s"${SessionGraphSourceFactory.defaultScheme}:$path")
}
