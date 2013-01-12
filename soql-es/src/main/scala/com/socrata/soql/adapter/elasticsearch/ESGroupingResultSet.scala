package com.socrata.soql.adapter.elasticsearch

import com.rojoma.json.io._
import java.io.InputStream
import java.nio.charset.Charset
import annotation.tailrec
import com.rojoma.json.ast.{JValue, JObject}
import com.socrata.soql.collection.OrderedMap
import com.rojoma.json.codec.JsonCodec

class ESGroupingResultSet(inputStream: InputStream, charset: Charset)
  extends ESPlainResultSet(inputStream, charset) {

  import ESResultSet._
  import ESGroupingResultSet._

  /**
   * Unlike regular query, there is no free total row count for facet.
   */
  override def rowStream(): Tuple2[Option[Long], Stream[JObject]] = {
    skipToField(lexer, "facets")
    if (!lexer.hasNext) throw new JsonParserEOF(lexer.head.position)
    val (total, facetsResult) = parseFacets(lexer)
    // TODO: Facets result may not be sorted in the right order yet.
    (total, facetsResult.values.toStream)
  }

  @tailrec
  private def parseFacets(lexer: JsonEventIterator, total: Option[Long] = None, acc: OrderedMap[JValue, JObject] = OrderedMap.empty):
    Tuple2[Option[Long], OrderedMap[JValue, JObject]] = {

    lexer.head match {
      case o: EndOfObjectEvent =>
        lexer.next() // consume }
        (total, acc)
      case _ =>
        val (total, groupRow) = parseOneFacet(lexer)
        val combinedRow = groupRow.map {
          case (k, v) =>
            val combinedResult = JObject(v.toMap ++ acc.get(k).getOrElse(JObject(Map.empty)).toMap)
            (k -> combinedResult)
        }
        parseFacets(lexer, total, combinedRow)
    }
  }

  private def parseOneFacet(lexer: JsonEventIterator): Tuple2[Option[Long], OrderedMap[JValue, JObject]] = {

    lexer.next() match {
      case o: StartOfObjectEvent =>
        parseOneFacet(lexer)
      case FieldEvent(FacetName(facet)) =>
        // dataKey is different depending on the type of facet which can be inferred by facet name
        val facetType = facetDataKey(facet)
        parseTotalOptionFacetEntries(lexer, facet, facetType, None)
      case unexpected =>
        throw new ESJsonBadParse(lexer.head, FieldEvent("some_facet_name"))
    }
  }

  /**
   * @param facetType can be columns for multi-columns and terms-stats for single column
   * @return true if we found facet, false means we find facet content
   */
  @tailrec
  private def parseTotalOptionFacetEntries(lexer: JsonEventIterator, facet: FacetName, facetType: String, total: Option[Long]):
    Tuple2[Option[Long], OrderedMap[JValue, JObject]] = {

    // total is only available in columns facet, non-exist in terms-stats facet.
    skipToField(lexer, Set("total", facetType)) match {
      case Some("total") =>
        val ttl = JsonCodec.fromJValue[Long](jsonReader.read())
        parseTotalOptionFacetEntries(lexer, facet, facetType, ttl) // keep going
      case Some(field) if field == facetType =>
        lexer.next() match {
          case sa: StartOfArrayEvent =>
            val groupRow = facetToGroupRow(jsonReader, facet, facet.groupKey, facet.groupValue)
            lexer.next() // consume }
            (total, groupRow)
          case unexpected =>
            throw new ESJsonBadParse(lexer.head, StartOfArrayEvent())
        }
      case Some(_) => // ignore any fields other than "total" and facetType
        lexer.dropNextDatum()
        parseTotalOptionFacetEntries(lexer, facet, facetType, total)
      case None =>
        throw new ESJsonBadParse(lexer.head, StartOfArrayEvent())
    }
  }

  @tailrec
  private def facetToGroupRow(reader: JsonReader, facetName: FacetName, groupCol: String, aggregateCol: String, acc: OrderedMap[JValue, JObject] = OrderedMap.empty): OrderedMap[JValue, JObject] = {

    reader.lexer.head match {
      case _: EndOfArrayEvent =>
        reader.lexer.next() // consume ]
        acc
      case _ =>
        reader.read() match {
          case facet@JObject(fields) =>
            val groupKey = facet(facetGroupKey(facetName)) // group key is
            // term property is removed and re-added using group col.
            // for the rest of the properties, we append aggregate col to the key.
            val groupRow = (fields -- unwantedFields).map {
              case (k, v) => (aggregateFnMap(k) + "_" + aggregateCol, v)
            } + (groupCol -> groupKey)
            val acc2 = acc + (groupKey -> JObject(groupRow))
            facetToGroupRow(reader, facetName, groupCol, aggregateCol, acc2)
          case _ =>
            throw new ESJsonBadParse(reader.lexer.head, StartOfObjectEvent())
        }
    }
  }
}

object ESGroupingResultSet {

  // Field either we do not want or field is not a constant translation that require remove and re-add
  val unwantedFields = Seq("term", "total_count", "keys")

  val aggregateFnMap = Map(
    "total" -> "sum",
    "mean" -> "avg",
    "min" -> "min",
    "max" -> "max",
    "count" -> "count"
  )

  private def facetDataKey(facetName: FacetName) =
    if (facetName.isMultiColumn) "entries" else "terms"

  private def facetGroupKey(facetName: FacetName) =
    if (facetName.isMultiColumn) "keys" else "term"
}