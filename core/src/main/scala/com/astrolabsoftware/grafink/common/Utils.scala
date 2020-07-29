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
package com.astrolabsoftware.grafink.common

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import zio.ZIO
import zio.logging.log

object Utils {

  /**
   * Converts from spark sql datatype to Janusgraph data type
   * TODO: Add support for all the missing types
   */
  val getClassTag: DataType => Class[_] = {
    case BinaryType  => classOf[String]
    case ByteType    => classOf[java.lang.Byte]
    case IntegerType => classOf[Integer]
    case LongType    => classOf[java.lang.Long]
    case FloatType   => classOf[java.lang.Float]
    case DoubleType  => classOf[java.lang.Double]
    case StringType  => classOf[String]
    case BooleanType => classOf[java.lang.Boolean]
    case _           => classOf[String]
  }

  val getClassTagFromString: String => Class[_] = {
    case "binary" => classOf[String]
    case "byte"   => classOf[java.lang.Byte]
    case "int"    => classOf[Integer]
    case "long"   => classOf[java.lang.Long]
    case "float"  => classOf[java.lang.Float]
    case "double" => classOf[java.lang.Double]
    case "string" => classOf[String]
    case "bool"   => classOf[java.lang.Boolean]
    case _        => classOf[String]
  }

  def withFileSystem[R, A](f: FileSystem => ZIO[R, Throwable, A])(implicit spark: SparkSession): ZIO[R, Throwable, A] =
    ZIO
      .effect(FileSystem.get(spark.sparkContext.hadoopConfiguration))
      .bracket(fs =>
        ZIO
          .effect(fs.close())
          .fold(failure => log.error(s"Error closing filesystem: $failure"), _ => ZIO.succeed(Unit))
      )(fs => f(fs))

}
