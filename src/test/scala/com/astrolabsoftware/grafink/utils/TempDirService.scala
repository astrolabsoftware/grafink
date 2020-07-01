package com.astrolabsoftware.grafink.utils

import java.io.File

import zio.{ console, Has, Task, ZIO, ZLayer }
import zio.console.Console

object TempDirService {

  type TempDirService = Has[TempDirService.Service]

  trait Service {
    def createTempDir(): ZIO[Console, Throwable, File]
    def removeTempDir(t: File): ZIO[Console, Throwable, Unit]
  }

  val test: ZLayer[Console, Throwable, TempDirService] =
    ZLayer.succeed(
      new Service {
        override def createTempDir(): ZIO[Console, Throwable, File] =
          for {
            tempDir <- ZIO.effect(File.createTempFile("com-astrolabsoftware-grafink", ""))
            _       <- Task(tempDir.delete)
            _       <- Task(tempDir.mkdir)
            _       <- console.putStrLn(s"Created temp dir: ${tempDir.getAbsolutePath}")
          } yield (tempDir)

        override def removeTempDir(t: File): ZIO[Console, Throwable, Unit] =
          for {
            _ <- rm(t)
            _ <- console.putStrLn(s"Deleted temp dir: ${t.getAbsolutePath}")
          } yield ()

        private def rm(file: File): ZIO[Any, Throwable, Unit] =
          if (file.isDirectory) {
            ZIO.effect(Option(file.listFiles).map(_.toList).getOrElse(Nil).foreach(rm))
          } else {
            ZIO.effect(file.delete)
          }
      }
    )

  def createTempDir(): ZIO[TempDirService with Console, Throwable, File]        = ZIO.accessM(_.get.createTempDir)
  def removeTempDir(t: File): ZIO[TempDirService with Console, Throwable, Unit] = ZIO.accessM(_.get.removeTempDir(t))
}
