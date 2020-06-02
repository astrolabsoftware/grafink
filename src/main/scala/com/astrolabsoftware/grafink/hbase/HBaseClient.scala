package com.astrolabsoftware.grafink.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ Connection, ConnectionFactory, Get, Put, Result }
import zio.{ Has, Task, UIO, ZIO, ZLayer, ZManaged }
import zio.blocking.Blocking

import com.astrolabsoftware.grafink.models.HBaseConfig
import com.astrolabsoftware.grafink.models.config.Config

final class HBaseClient(connection: Connection) {

  def putToTable(p: Put, table: String): Task[Unit] =
    Task(connection.getTable(TableName.valueOf(table)).put(p))

  def getFromTable(g: Get, table: String): Task[Result] =
    Task(connection.getTable(TableName.valueOf(table)).get(g))

  def close(): UIO[Unit] = UIO.effectTotal(connection.close())
}

object HBaseClient {

  def make(): ZManaged[Has[HBaseConfig], Throwable, HBaseClient] =
    ZManaged.make(
      for {
        conf <- Config.hbaseConfig
        hConf      = HBaseConfiguration.create()
        _          = hConf.set("hbase.zookeeper.quorum", conf.zookeeper.quoram)
        connection = ConnectionFactory.createConnection(hConf)
      } yield (new HBaseClient(connection))
    )(client => client.close())
}
