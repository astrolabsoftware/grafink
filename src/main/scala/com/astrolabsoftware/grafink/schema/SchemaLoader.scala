package com.astrolabsoftware.grafink.schema

import zio.Has

object SchemaLoader {

  type SchemaLoaderService = Has[SchemaLoader.Service]

  trait Service {}

}
