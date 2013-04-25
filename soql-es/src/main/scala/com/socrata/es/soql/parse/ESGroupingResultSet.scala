package com.socrata.es.soql.parse

import com.rojoma.json.io._
import java.io.InputStream
import java.nio.charset.Charset
import annotation.tailrec
import com.rojoma.json.ast.{JArray, JValue, JObject}
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.types.SoQLAnalysisType
import com.socrata.soql.typed.CoreExpr
import com.socrata.es.facet.parse.FacetResponseParser
import com.socrata.es.soql.query.FacetName

class ESGroupingResultSet(analysis: SoQLAnalysis[SoQLAnalysisType], inputStream: InputStream, charset: Charset)
  extends ESPlainResultSet(analysis, inputStream, charset) {

  import ESResultSet._
  import ESGroupingResultSet._


  import ExprGroupResultName._

  protected val isMultiGroup: Boolean = analysis.groupBy.map(_.size > 1).getOrElse(false)

  protected val groupIndexMap = {
    val exprIndexs = analysis.groupBy.map(_.zipWithIndex).getOrElse(Seq.empty[Tuple2[CoreExpr[SoQLAnalysisType], Int]])
    analysis.selection.map { case (colName, expr) =>
      colName -> exprIndexs.find { (exprIdx) => exprIdx._1 == expr }.map { (exprIdx) => exprIdx._2 }
    }
  }

  /**
   * Make output from single - group (statFacet) and multi - groups (columnFacet) output
   * to look like select output
   * Multi group keys are in array like { ... _multi : [ group_col1_val, group_col2_val, ... ] ... }
   * TODO: scripts/expressions are not handled yet because it may require a totally different implementation throughout the stack
   */
  override def select(map: Map[String, JValue]): Map[String, JValue] = {

    analysis.selection.map { case (colName, expr) =>
      if (isMultiGroup) {
        groupIndexMap(colName) match {
          case Some(groupIdx) =>
            colName.name -> map("_multi").asInstanceOf[JArray](groupIdx)
          case None =>
            colName.name -> map(expr.toName)
        }
      } else {
        colName.name -> map(expr.toName)
      }
    }
  }

  /**
   * Unlike regular query, there is no free total row count for facet.
   */
  override def rowStream(): Tuple2[Option[Long], Stream[JObject]] = {
    skipToField(lexer, "facets")
    if (!lexer.hasNext) throw new JsonParserEOF(lexer.head.position)
    val (total, facetsResult) = parseFacets(lexer)
    // TODO: Facets result may not be sorted in the right order yet.
    val selectedColumnsResult =
      if (honorSelect) facetsResult.values.view.map { jo => JObject(select(jo.toMap)) }
      else facetsResult.values
    (total, selectedColumnsResult.toStream)
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

  protected def parseOneFacet(lexer: JsonEventIterator): Tuple2[Option[Long], OrderedMap[JValue, JObject]] = {

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
   * Wrap the tailrec version so that this can be override by sub-class
   */
  protected def parseTotalOptionFacetEntries(lexer: JsonEventIterator, facetName: FacetName, facetType: String, total: Option[Long]):
  Tuple2[Option[Long], OrderedMap[JValue, JObject]] = {

    val facetParser = FacetResponseParser(facetName)
    facetParser.parseFacet(jsonReader, facetName, facetType, total)
  }

  protected def facetToGroupRow(reader: JsonReader, facetName: FacetName, groupCol: String, aggregateCol: String, acc: OrderedMap[JValue, JObject] = OrderedMap.empty, total: Option[Long]): OrderedMap[JValue, JObject] = {

    val facetParser = FacetResponseParser(facetName)
    facetParser.parseEntries(reader, facetName, groupCol, aggregateCol, acc, total)
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

  def facetDataKey(facetName: FacetName) =
    if (facetName.isMultiColumn) "entries" else "terms"

  def facetGroupKey(facetName: FacetName) =
    if (facetName.isMultiColumn) "keys" else "term"
}