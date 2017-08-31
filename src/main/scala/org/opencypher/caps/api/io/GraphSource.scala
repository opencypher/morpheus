package org.opencypher.caps.api.io

import java.net.URI

import org.opencypher.caps.api.spark.{CAPSGraph, CAPSSession}
import org.opencypher.caps.impl.exception.Raise

trait GraphSource {

  def handles(uri: URI): Boolean

  def get(implicit capsSession: CAPSSession): CAPSGraph
}

trait GraphSourceFactory {

  def protocol: String

  def fromURI(uri: URI): GraphSource
}

case class GraphSourceHandler(graphSourceFactories: Set[GraphSourceFactory],
                              mountPoints: Map[String, GraphSource],
                              graphSources: Set[GraphSource]) {

  def withGraphAt(uri: URI, alias: String)(implicit capsSession: CAPSSession): CAPSGraph =
    if (uri.getScheme != null) loadFromURI(uri) else loadFromMountPoint(uri)

  private def loadFromURI(uri: URI)(implicit capsSession: CAPSSession): CAPSGraph =
    graphSources.find(_.handles(uri)).getOrElse {
      graphSourceFactories.find(_.protocol == uri.getScheme)
        .getOrElse(Raise.invalidArgument(graphSourceFactories.map(_.protocol).mkString("[", ",", "]"), uri.getScheme))
        .fromURI(uri)
    }.get

  private def loadFromMountPoint(uri: URI)(implicit capsSession: CAPSSession): CAPSGraph =
    mountPoints
      .getOrElse(uri.getPath, Raise.invalidArgument(mountPoints.keySet.mkString("[", ",", "]"), uri.getPath))
      .get

  def mount(path: String, uri: URI): Unit = ???

  def mount(path: String, graphSource: GraphSource): Unit = ???

  def mount(path: String, graphSourceFactory: GraphSourceFactory): Unit = ???
}