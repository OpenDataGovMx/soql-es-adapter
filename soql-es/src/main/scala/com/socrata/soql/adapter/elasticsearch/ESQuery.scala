package com.socrata.soql.adapter.elasticsearch

import com.socrata.soql.typed._
import com.rojoma.json.ast._
import com.socrata.soql.adapter.{NotImplementedException, SoqlAdapter}
import com.socrata.rows.{ESGateway, ESHttpGateway}
import com.socrata.collection.{OrderedSet, OrderedMap}
import com.socrata.soql.names.{FunctionName, ColumnName}
import com.socrata.soql.types._
import com.socrata.soql.{SoQLAnalyzer, DatasetContext}
import com.socrata.soql.typed.OrderBy
import com.socrata.soql.typed.StringLiteral
import com.rojoma.json.ast.JObject
import com.rojoma.json.ast.JArray
import com.socrata.soql.typed.NumberLiteral
import com.socrata.soql.typed.ColumnRef
import com.socrata.soql.typed.FunctionCall
import com.rojoma.json.ast.JString

class ESQuery(val resource: String, val esGateway: ESGateway) extends SoqlAdapter[String] {

  import ESQuery._

  private lazy val dsCtx = esGateway.getDataContext()

  def full(soql: String) = toQuery(dsCtx, soql)

  def select(selection : OrderedMap[ColumnName, TypedFF[SoQLType]]) = ""

  def where(filter: TypedFF[SoQLType], xlateCtx: Map[String, AnyRef]) =
    toESFilter(filter, xlateCtx).toString()

  def orderBy(orderBys: Seq[OrderBy[SoQLType]]) =
    toESOrderBy(orderBys).toString()

  def groupBy(groupBys: Seq[TypedFF[SoQLType]], cols: OrderedMap[ColumnName, TypedFF[SoQLType]]) =
    toESGroupBy(groupBys, cols).toString

  def offset(offset: Option[BigInt]) = offset.map(toESOffset).toString

  def limit(limit: Option[BigInt]) = limit.map(toESLimit).toString

  private def toQuery(datasetCtx: DatasetContext[SoQLType], soql: String): String = {
    implicit val ctx: DatasetContext[SoQLType] = datasetCtx

    val analysis = analyzer.analyzeFullQuery(soql)
    val esFilter = analysis.where.map(toESFilter(_, Map.empty[String, AnyRef]))
    val esFilterOrQuery = esFilter.map { filter =>
    // A top filter apply independently of facets
    // Must wrap in a query in order to affect facets.
    // filtered query must have query but we don't use it because it cannot handle and/or.
    // We fool it be puting match_all there.
      if (analysis.groupBy.isDefined) JObject(Map("filtered"-> JObject(OrderedMap("query" -> JObject1("match_all", JObject0), "filter" -> filter))))
      else filter
    }

    // For group by, we don't want any individual rows to be returned.  Setting limit to 0 accomplish that.
    val esLimit =
      if (analysis.isGrouped) Some(toESLimit(0))
      else analysis.limit.map(toESLimit)

    val esObjectPropsSomeValuesMayBeEmpty =
      OrderedMap(
        (if (analysis.groupBy.isDefined) "query" else "filter") -> esFilterOrQuery,
        "sort" -> analysis.orderBy.map(toESOrderBy),
        "facets" -> analysis.groupBy.map(toESGroupBy(_, analysis.selection)),
        "from" -> analysis.offset.map(toESOffset),
        "size" -> esLimit
      )

    val esObjectProps = esObjectPropsSomeValuesMayBeEmpty.collect { case (k, v) if v.isDefined => (k, v.get) }
    val esObject = JObject(esObjectProps)
    esObject.toString //.replace("\n", " ")
  }

  private def toESOffset(offset: BigInt) = JNumber(offset)

  private def toESLimit(limit: BigInt) = JNumber(limit)

  private def toESOrderBy(orderBys: Seq[OrderBy[SoQLType]]): JValue = {
    JArray(orderBys.map { ob =>
      ob.expression match {
        case col: ColumnRef[_] =>
          JObject(Map(col.column.name -> JString(if (ob.ascending) "asc" else "desc")))
        case _ =>
          throw new NotImplementedException("Sort by expressiion is not implemented", ob.expression.position)
      }
    })
  }

  private def toESGroupBy(groupBys: Seq[TypedFF[SoQLType]], cols: OrderedMap[ColumnName, TypedFF[SoQLType]]): JObject = {

    val esGroupBys = groupBys.collect { // ignore expression that aren't simple column
      case col: ColumnRef[_] =>
        val aggregateColumns = cols.foldLeft(OrderedSet.empty[ColumnRef[_]]) { (aggregateCols, outputCol) => outputCol match {
          case (columnName, expr) =>
            expr match {
              case FunctionCall(function, (arg : ColumnRef[_]) :: Nil) if function.isAggregate =>
                aggregateCols + arg
              case FunctionCall(function, arg) if function.isAggregate =>
                // require facet script value
                throw new NotImplementedException("Aggregate on expression not implemented", expr.position)
              case _ =>
                // should never happen
                aggregateCols
            }
        }}

        JObject(aggregateColumns.map { aggregateValue =>
             val facetKeyVal = JObject(Map("key_field" -> JString(col.column.name), "value_field" -> JString(aggregateValue.column.name)))
             val termsStats = JObject(Map("terms_stats" -> facetKeyVal))
             val facetName = "fc_%s_%s".format(col.column.name, aggregateValue.column.name)
             val facet = (facetName, termsStats)
             facet
        }.toMap)
    }
    if (esGroupBys.size > 1) throw new NotImplementedException("Cannot handle multiple groups", groupBys(1).position)
    esGroupBys.head
  }

  private def toESFilter(filter: TypedFF[SoQLType], xlateCtx: Map[String, AnyRef], level: Int = 0): JValue = {
    val result = filter match {
      case col: ColumnRef[_] =>
        col.typ match {
          case x: SoQLNumber.type => JString(col.column.name)
          case x: SoQLText.type => JString(col.column.name)
          case x => JString(col.column.name)
        }
      case lit: StringLiteral[_] =>
        JString(lit.value)
      case lit: NumberLiteral[_] =>
        JNumber(lit.value)
      case fn: FunctionCall[_] =>
        val esFn = fn.function.function match {
          case SoQLFunctions.Eq  | SoQLFunctions.EqEq => // soql = to adaptor term
            val cols = fn.parameters.filter{ (param: TypedFF[_]) =>
              param match {
                case col: ColumnRef[_] => true
                case _ => false
              }}
            val lits = fn.parameters.filter{ (param: TypedFF[_]) =>
              param match {
                case lit: TypedLiteral[_] => true
                case _ => false
              }}
            if (cols.size == 1 && lits.size == 1) {
              val lhs = toESFilter(cols.head, xlateCtx, level+1)
              val rhs = toESFilter(lits.head, xlateCtx, level+1)
              JObject(Map("term" -> JObject(Map(lhs.asInstanceOf[JString].string -> rhs))))
            } else {
              // require ES scripted filter
              throw new NotImplementedException("Only support simple field = literal.",  fn.position)
            }
          case SoQLFunctions.And | SoQLFunctions.Or =>
            val lhs = toESFilter( fn.parameters(0), xlateCtx, level+1)
            val rhs = toESFilter( fn.parameters(1), xlateCtx, level+1)
            JObject(Map(fn.function.name.name.substring(3) -> JArray(Seq(lhs, rhs))))
          case SoQLFunctions.UnaryMinus =>
            toESFilter(fn.parameters(0), xlateCtx, level+1) match {
              case JNumber(x) => JNumber(-x)
              case JString(x) => JString("-" + x)
              case x =>
                throw new Exception("should never get here - negate on " + x.getClass.getName)
            }
          case SoQLFunctions.In =>
            toESFilter(fn.parameters.head, xlateCtx, level+1) match {
              case JString(col) =>
                JObject1("terms", JObject1(col, JArray(fn.parameters.tail.map(toESFilter(_, xlateCtx, level+1)))))
              case _ =>
                throw new NotImplementedException("Lhs of in must be a column.", fn.functionNamePosition)
            }
          case SoQLFunctions.NotIn =>
            toESFilter(fn.parameters.head, xlateCtx, level+1) match {
              case JString(col) =>
                JObject1("not", JObject1("terms", JObject1(col, JArray(fn.parameters.tail.map(toESFilter(_, xlateCtx, level+1))))))
              case _ =>
                throw new NotImplementedException("Lhs of not in must be a column.", fn.functionNamePosition)
            }
          case SoQLFunctions.WithinCircle =>
            val latLon = JObject(Map(
              "lat" -> toESFilter(fn.parameters(1), xlateCtx, level+1),
              "lon" -> toESFilter(fn.parameters(2), xlateCtx, level+1)) )
            JObject1("geo_distance", JObject(Map(
              "distance" -> toESFilter(fn.parameters(3), xlateCtx, level+1),
              "location" -> latLon)))
          case SoQLFunctions.WithinBox =>
            val nwLatLon = JObject(Map(
              "lat" -> toESFilter(fn.parameters(1), xlateCtx, level+1),
              "lon" -> toESFilter(fn.parameters(2), xlateCtx, level+1)) )
            val seLatLon = JObject(Map(
              "lat" -> toESFilter(fn.parameters(3), xlateCtx, level+1),
              "lon" -> toESFilter(fn.parameters(4), xlateCtx, level+1)) )
            toESFilter(fn.parameters(0), xlateCtx, level+1) match {
              case JString(locCol) =>
                JObject1("geo_bounding_box",
                  JObject1(locCol,
                    JObject(Map(
                      "top_left" -> nwLatLon,
                      "bottom_right" -> seLatLon))))
              case _ =>
                throw new NotImplementedException("First argument to within_box must be a location column.", fn.functionNamePosition)
            }
          case SoQLFunctions.Between =>
            fn.parameters match {
              case SimpleColumnLiteralExpression(colLit) if (colLit.lhsIsColumn) =>
                toESFilter(colLit.col, xlateCtx, level+1) match {
                  case JString(col) =>
                    JObject1("range", JObject1(col,
                      JObject(Map(
                        "from" -> toESFilter(colLit.lit, xlateCtx, level+1),
                        "to" -> toESFilter(colLit.lit2.get, xlateCtx, level+1),
                        "include_upper" -> JBoolean(true),
                        "include_lower" -> JBoolean(true)))))
                  case _ =>
                    throw new NotImplementedException("Expression must be column and literal.", fn.functionNamePosition)
                }
              case _ =>
                throw new NotImplementedException("Expression must be column and literal.", fn.functionNamePosition)
            }
          case SoQLFunctions.Lte | SoQLFunctions.Lt | SoQLFunctions.Gte | SoQLFunctions.Gt  =>
            val inclusive = (fn.function.function == SoQLFunctions.Lte || fn.function.function == SoQLFunctions.Gte)
            fn.parameters match {
              case SimpleColumnLiteralExpression(colLit) =>
                val lhsIsColumn =
                  if (fn.function.function == SoQLFunctions.Lte || fn.function.function == SoQLFunctions.Lt) colLit.lhsIsColumn
                  else !colLit.lhsIsColumn
                toESFilter(colLit.col, xlateCtx, level+1) match {
                  case JString(col) =>
                    val toOrFrom = if (lhsIsColumn) "to" else "from"
                    val upperOrLower = if (lhsIsColumn) "include_upper" else "include_lower"
                    JObject1("range", JObject1(col,
                        JObject(Map(
                          toOrFrom -> toESFilter(colLit.lit, xlateCtx, level+1),
                          upperOrLower -> JBoolean(inclusive)))))
                  case _ =>
                    throw new NotImplementedException("Expression must be column and literal.", fn.functionNamePosition)
                }
              case _ =>
                throw new NotImplementedException("Expression must be column and literal.", fn.functionNamePosition)
            }
          case notImplemented =>
            println(notImplemented.getClass.getName)
            throw new NotImplementedException("Expression not implemented " + notImplemented.name, fn.functionNamePosition)
        }
        esFn
      case notImplemented =>
        throw new NotImplementedException("Expression not implemented " + notImplemented.toString, notImplemented.position)
    }

    result
  }
}

object ESQuery {

  private val JObject0 = JObject(Map.empty)

  private def JObject1(k: String, v: JValue): JObject = JObject(Map(k -> v))

  private val analyzer = new SoQLAnalyzer(SoQLTypeInfo)
}
