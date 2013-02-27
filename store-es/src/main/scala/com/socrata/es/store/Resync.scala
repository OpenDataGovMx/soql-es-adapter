package com.socrata.es.store

import com.socrata.datacoordinator.truth.DataReadingContext
import com.socrata.datacoordinator.common.soql.PostgresSoQLDataContext
import scalaz.effect.IO

class SecondaryResync(val dataContext: DataReadingContext with PostgresSoQLDataContext) {

  def resync(id: String): Boolean = {
    val res = dataContext.datasetReader.withDataset(id, latest = true) {
      import dataContext.datasetReader._
      for {
        s <- schema
        _ <- dataContext.datasetReader.withRows { it =>
          IO { ESSecondary.resyncSecondary(id, s, dataContext.dataSource.getConnection, it) }
        }
      } yield ()
    }
    res.unsafePerformIO().isDefined
  }
}
