package org.opencypher.caps.api.io

import java.net.URI

import org.opencypher.caps.api.spark.CAPSGraph

trait GraphSource {

  def get: CAPSGraph
}

trait GraphSourceFactory {

  def protocol: String

  def fromURI(uri: URI): GraphSource
}