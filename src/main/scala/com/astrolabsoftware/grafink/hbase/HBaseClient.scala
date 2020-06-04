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
package com.astrolabsoftware.grafink.hbase

import scala.util.Try

import org.apache.hadoop.hbase.{ HBaseConfiguration, TableName, TableNotFoundException }
import org.apache.hadoop.hbase.client.{
  ColumnFamilyDescriptorBuilder,
  Connection,
  ConnectionFactory,
  Get,
  HBaseAdmin,
  Put,
  Result,
  TableDescriptor,
  TableDescriptorBuilder
}
import org.apache.hadoop.hbase.util.Bytes
import zio.{ Has, RIO, Task, UIO, ZIO, ZLayer, ZManaged }
import zio.internal.Executor
import zio.logging.{ log, Logging }

import com.astrolabsoftware.grafink.models.{ HBaseConfig, IDManagerConfig }
import com.astrolabsoftware.grafink.models.config.Config
import com.astrolabsoftware.grafink.services.IDManager.IDType

object HBaseClientService {

  type HBaseClientService = Has[HBaseClientService.Service]

  trait Service {
    def putToTable(p: Put, table: String): Task[Unit]
    def getFromTable(
      rowKey: String,
      cf: String,
      cQualifier: String,
      table: String
    ): ZIO[Logging, Throwable, Option[String]]
    def createTableIfNotExists(table: String, cfName: String): ZIO[Logging, Throwable, Unit]
    def createTable(t: TableDescriptor): RIO[Logging, Unit]
  }

  val live: Executor => ZLayer[Logging with Has[HBaseConfig], Throwable, HBaseClientService] = executor =>
    ZLayer.fromManaged[Logging with Has[HBaseConfig], Throwable, Service] {
      make(executor)
    }

  def make(executor: Executor): ZManaged[Has[HBaseConfig], Throwable, HBaseClient] =
    ZManaged.make(
      for {
        conf <- Config.hbaseConfig
        hConf      = HBaseConfiguration.create()
        _          = hConf.set("hbase.zookeeper.quorum", conf.zookeeper.quoram)
        connection = ConnectionFactory.createConnection(hConf)
      } yield new HBaseClient(connection, executor)
    )(client => client.close())

  def putToTable(p: Put, table: String): ZIO[HBaseClientService, Throwable, Unit] =
    ZIO.accessM(_.get.putToTable(p, table))
  def getFromTable(
    rowKey: String,
    cf: String,
    cQualifier: String,
    table: String
  ): ZIO[HBaseClientService with Logging, Throwable, Option[String]] =
    ZIO.accessM(_.get.getFromTable(rowKey, cf, cQualifier, table))
  def createTableIfNotExists(table: String, cfName: String): RIO[HBaseClientService with Logging, Unit] =
    ZIO.accessM(_.get.createTableIfNotExists(table, cfName))
  def createTable(t: TableDescriptor): ZIO[HBaseClientService with Logging, Throwable, Unit] =
    ZIO.accessM(_.get.createTable(t))
}

final class HBaseClient(connection: Connection, executor: Executor) extends HBaseClientService.Service {

  def putToTable(p: Put, table: String): Task[Unit] =
    ZIO
      .effect(connection.getTable(TableName.valueOf(table)))
      .bracket(t => UIO.effectTotal(t.close()))(t => Task(t.put(p)))
      .lock(executor)

  def getFromTable(
    rowKey: String,
    cf: String,
    cQualifier: String,
    table: String
  ): ZIO[Logging, Throwable, Option[String]] =
    ZIO
      .effect(connection.getTable(TableName.valueOf(table)))
      .catchSome {
        // Create the table if it does not exist
        case _: TableNotFoundException =>
          for {
            _ <- createTableIfNotExists(table, s"$cf:$cQualifier")
          } yield connection.getTable(TableName.valueOf(table))
      }
      .bracket(t => UIO.effectTotal(t.close())) { t =>
        val columnFamily = Bytes.toBytes(cf)
        val qualifier    = Bytes.toBytes(cQualifier)
        val g            = new Get(Bytes.toBytes(rowKey)).addColumn(columnFamily, qualifier)
        for {
          result <- Task(t.get(g))
          r = if (result.isEmpty) None else Try(Bytes.toString(result.getValue(columnFamily, qualifier))).toOption
        } yield r
      }
      .lock(executor)

  def createTableIfNotExists(table: String, cfName: String): RIO[Logging, Unit] =
    for {
      admin  <- Task(connection.getAdmin)
      exists <- Task(admin.tableExists(TableName.valueOf(table)))
      _ <- if (exists) {
        log.info(s"Table $table already exists")
      } else {
        val cf = ColumnFamilyDescriptorBuilder.newBuilder(cfName.getBytes).build()
        val tableDescriptor =
          TableDescriptorBuilder
            .newBuilder(TableName.valueOf(table))
            .setColumnFamily(cf)
            .build()
        createTable(tableDescriptor)
      }
    } yield ()

  def createTable(t: TableDescriptor): RIO[Logging, Unit] =
    for {
      admin <- Task(connection.getAdmin)
      _ <- ZIO
        .effect(admin.createTable(t))
        .tap(_ => log.info(s"Successfully created table ${t.getTableName.getNameAsString}"))
    } yield ()

  def close(): UIO[Unit] = UIO.effectTotal(connection.close())
}
