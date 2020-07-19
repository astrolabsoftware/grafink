package com.astrolabsoftware.grafink.hbase

import org.apache.hadoop.hbase.client.{ Put, TableDescriptor }
import zio._
import zio.logging.Logging
import zio.test.mock.{ Mock, Proxy }

import com.astrolabsoftware.grafink.hbase.HBaseClientService.HBaseClientService

object HBaseClientServiceMock extends Mock[HBaseClientService] {

  object PutToTable             extends Effect[Put, Nothing, Unit]
  object GetFromTable           extends Effect[String, Nothing, Option[String]]
  object CreateTableIfNotExists extends Effect[String, Nothing, Unit]
  object CreateTable            extends Effect[TableDescriptor, Nothing, Unit]

  val compose: URLayer[Has[Proxy], HBaseClientService] =
    ZLayer.fromService { proxy =>
      new HBaseClientService.Service {
        def putToTable(p: Put, table: String): Task[Unit] = proxy(PutToTable, p)
        def getFromTable(
          rowKey: String,
          cf: String,
          cQualifier: String,
          table: String
        ): ZIO[Logging, Throwable, Option[String]] = proxy(GetFromTable, rowKey)
        def createTableIfNotExists(table: String, cfName: String): RIO[Logging, Unit] =
          proxy(CreateTableIfNotExists, table)
        def createTable(t: TableDescriptor): RIO[Logging, Unit] = proxy(CreateTable, t)
      }
    }
}
