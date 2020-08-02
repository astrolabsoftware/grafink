/*
 * Copyright 2020 AstroLab Software
 * Author: Yash Datta
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
package com.astrolabsoftware.grafink.shell

import ammonite.runtime.Storage
import pureconfig.{ CamelCase, ConfigFieldMapping, ConfigReader, ConfigSource }
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._
import pureconfig.generic.semiauto.deriveEnumerationReader

import com.astrolabsoftware.grafink.{ Boot, BuildInfo, QueryHelper }
import com.astrolabsoftware.grafink.models.{ Format, GrafinkConfiguration, GrafinkJanusGraphConfig }

object Main extends App {

  // For pure config to be able to use camel case
  implicit def hint[T]: ProductHint[T]          = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))
  implicit val formatHint: ConfigReader[Format] = deriveEnumerationReader[Format]

  def shell(): Unit = {
    val commandLineArgs = Boot.parseArgs(args)
    val conf = commandLineArgs match {
      case Some(config) =>
        ConfigSource.file(config.confFile).loadOrThrow[GrafinkConfiguration]
      case _ => throw new RuntimeException(s"No valid configuration found")
    }
    val janusConfig = conf.janusgraph
    // Create graph
    val graph = QueryHelper.getGraph(GrafinkJanusGraphConfig(conf.job, conf.janusgraph))
    val g     = graph.traversal()

    val initCode =
      s"""
         |@ repl.prompt() = "grafink>"
         |@ println(io.leego.banana.BananaUtils.bananaify("Grafink", "Roman"))
         |@ import scala.collection.JavaConverters._, com.astrolabsoftware.grafink.QueryHelper._, org.janusgraph.core._, org.apache.tinkerpop.gremlin.structure._
         |""".stripMargin
    ammonite
      .Main(
        predefCode = initCode,
        remoteLogging = false,
        // Since file storage is not thread safe and can lead to issues if multiple users login from
        // the same machine, turning it off
        storageBackend = Storage.InMemory(),
        welcomeBanner = Some(
          s"" +
            s"Welcome to Grafink Shell ${BuildInfo.version}\nJanusGraphConfig available as janusConfig\nJanusGraph available as graph, traversal as g"
        )
      )
      .run(
        "janusConfig" -> janusConfig,
        "graph"       -> graph,
        "g"           -> g
      )
    g.close()
    graph.close()
  }
  shell()
}
