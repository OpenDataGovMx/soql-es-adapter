package com.socrata.es.meta

import com.rojoma.json.codec.JsonCodec

case class DatasetMeta(copyId: Long, version: Long)

object DatasetMeta {

  implicit val datasetMetaJCodec = new JsonCodec[DatasetMeta] {

    import com.rojoma.json.ast._
    import com.rojoma.json.matcher._

    private val copyIdVar = Variable[Long]()
    private val versionVar = Variable[Long]()

    private val InPattern =
      PObject(
        // there are system fields that we just ignore when we get data out.
        // _index, _type, _id, _version, _exists
        "_source" -> PObject(
          "copy_id" -> copyIdVar,
          "version" -> versionVar
        )
      )

    private val OutPattern =
      PObject(
        "copy_id" -> copyIdVar,
        "version" -> versionVar
      )

    def encode(datasetMeta: DatasetMeta): JValue = {
      OutPattern.generate(
        copyIdVar := datasetMeta.copyId,
        versionVar := datasetMeta.version
      )
    }

    def decode(in: JValue): Option[DatasetMeta] = {
      in match {
        case InPattern(results) =>
          Some(DatasetMeta(copyIdVar(results), versionVar(results)))
        case _ =>
          None
      }
    }
  }
}
