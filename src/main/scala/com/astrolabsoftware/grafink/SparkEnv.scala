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
package com.astrolabsoftware.grafink

import buildinfo.BuildInfo
import org.apache.spark.sql.SparkSession
import zio.{ Has, ZLayer }
import zio.blocking.Blocking

object SparkEnv {

  trait Service {
    def sparkEnv: SparkSession
  }

  def make(session: => SparkSession): ZLayer[Blocking, Throwable, Has[Service]] =
    ZLayer.fromFunctionManyM { blocking =>
      blocking.get
        .effectBlocking(session)
        .map { sparkSession =>
          Has(new Service {
            override def sparkEnv: SparkSession = sparkSession
          })
        }
    }

  def local(): ZLayer[Blocking, Throwable, Has[Service]] =
    make {
      SparkSession.builder().appName(BuildInfo.name).master("local[*]").getOrCreate()
    }

  def cluster(): ZLayer[Blocking, Throwable, Has[Service]] =
    make {
      SparkSession
        .builder()
        .appName(BuildInfo.name)
        .getOrCreate()
    }
}
