package com.astrolabsoftware.grafink.models

sealed abstract class Format {
  def toString: String
}

case object Csv     extends Format { override def toString: String = "csv"     }
case object Json    extends Format { override def toString: String = "json"    }
case object Parquet extends Format { override def toString: String = "parquet" }
