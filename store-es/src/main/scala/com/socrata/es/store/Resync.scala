package com.socrata.es.store

import com.socrata.datacoordinator.truth.DataReadingContext
import com.socrata.datacoordinator.common.soql.PostgresSoQLDataContext

class SecondaryResync(val dataContext: DataReadingContext) {

  def resync(id: String): Boolean = {
    val res = for {
      ctxOpt <- dataContext.datasetReader.openDataset(id, latest = true)
      ctx <- ctxOpt
    } yield {
      import ctx._
      withRows { it =>
        ESSecondary.resyncSecondary(id, schema, dataContext, it)
      }
    }
    res.isDefined
  }
}
