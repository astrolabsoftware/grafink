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
package com.astrolabsoftware.grafink.api.cache

import java.util.concurrent.ConcurrentHashMap

import org.janusgraph.core.JanusGraph
import zio.{ IO, Ref, UIO, ZRef }

import com.astrolabsoftware.grafink.models.GrafinkException.ConnectionLimitReachedException

class SimpleCache(private val capacity: Int, private val mapRef: Ref[ConcurrentHashMap[String, JanusGraph]]) {

  def get(key: String): IO[Throwable, Option[JanusGraph]] =
    for {
      cache <- mapRef.get
      graph = if (cache.containsKey(key)) Some(cache.get(key)) else None
    } yield graph

  def put(key: String, graph: JanusGraph): IO[Throwable, Boolean] =
    for {
      cache <- mapRef.get
      _ <- if (cache.size == capacity) {
        // Map is full, lets not create more connections
        throw new ConnectionLimitReachedException(s"API already is caching maximum number of cached")
      } else {
        mapRef.update { m =>
          m.put(key, graph)
          m
        }
      }
    } yield (true)

}

object SimpleCache {

  def make(capacity: Int): UIO[SimpleCache] =
    for {
      c <- ZRef.make(new java.util.concurrent.ConcurrentHashMap[String, JanusGraph](capacity))
    } yield {
      new SimpleCache(capacity, c)
    }
}
