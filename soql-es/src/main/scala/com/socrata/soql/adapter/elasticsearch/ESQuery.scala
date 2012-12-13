package com.socrata.soql.adapter.elasticsearch

import com.socrata.soql.typed._
import com.rojoma.json.ast._
import com.socrata.soql.adapter.{SoQLAdapterException, XlateCtx, NotImplementedException, SoqlAdapter}
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
import com.socrata.soql.functions.MonomorphicFunction
import util.parsing.input.Position
import com.blist.rows.format.DataTypeConverter
import com.blist.models.views.DataType

class ESQuery(val resource: String, val esGateway: ESGateway) extends SoqlAdapter[String] {

  class RequireScriptFilter(message: String, position: Position) extends SoQLAdapterException(message, position)

  import ESQuery._

  private lazy val dsCtx = esGateway.getDataContext()

  def full(soql: String) = toQuery(dsCtx, soql)

  def select(selection : OrderedMap[ColumnName, TypedFF[SoQLType]]) = ""

  def where(filter: TypedFF[SoQLType], xlateCtx: Map[XlateCtx.Value, AnyRef] = Map.empty) =
    toESFilter(filter, xlateCtx, canScript = true).toString()

  def orderBy(orderBys: Seq[OrderBy[SoQLType]]) =
    toESOrderBy(orderBys).toString()

  def groupBy(groupBys: Seq[TypedFF[SoQLType]], cols: OrderedMap[ColumnName, TypedFF[SoQLType]]) =
    toESGroupBy(groupBys, cols).toString

  def offset(offset: Option[BigInt]) = offset.map(toESOffset).toString

  def limit(limit: Option[BigInt]) = limit.map(toESLimit).toString

  private def toQuery(datasetCtx: DatasetContext[SoQLType], soql: String): Tuple2[String, SoQLAnalyzer[SoQLType]#Analysis] = {
    implicit val ctx: DatasetContext[SoQLType] = datasetCtx

    val analysis = analyzer.analyzeFullQuery(soql)
    val esFilter = analysis.where.map(toESFilter(_, Map.empty[XlateCtx.Value, AnyRef], canScript = true))
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
        "facets" -> analysis.groupBy.map(toESGroupBy(_, analysis.selection, analysis.offset, analysis.limit)),
        "from" -> analysis.offset.map(toESOffset),
        "size" -> esLimit
      )

    val esObjectProps = esObjectPropsSomeValuesMayBeEmpty.collect { case (k, v) if v.isDefined => (k, v.get) }
    val esObject = JObject(esObjectProps)
    Tuple2(esObject.toString, analysis) //.replace("\n", " ")
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

  private def toESGroupBy(groupBys: Seq[TypedFF[SoQLType]], cols: OrderedMap[ColumnName, TypedFF[SoQLType]],
                          offset: Option[BigInt] = None, limit: Option[BigInt] = None): JObject = {

    val esGroupBys = groupBys.collect { // ignore expression that aren't simple column
      case col: ColumnRef[_] =>
        val aggregateColumns = cols.foldLeft(OrderedSet.empty[ColumnRef[_]]) { (aggregateCols, outputCol) => outputCol match {
          case (columnName, expr) =>
            expr match {
              case FunctionCall(function, (arg : ColumnRef[_]) :: Nil) if function.isAggregate =>
                // ColumnRef contains position info which prevents us from directly using ColumnRef equal.
                if (aggregateCols.find(_.column == arg.column).isDefined) aggregateCols
                else aggregateCols + arg
              case FunctionCall(function, arg) if function.isAggregate =>
                // require facet script value
                throw new NotImplementedException("Aggregate on expression not implemented", expr.position)
              case _ =>
                // should never happen
                aggregateCols
            }
        }}

        JObject(aggregateColumns.map { aggregateValue =>
          val facetVal = aggregateValue.typ match {
            case SoQLNumber =>
              ("value_field", JString(aggregateValue.column.name))
            case _ =>
              // terms stat only works on numeric fields.  Use value script to at least get sensible result for count(text)
              ("value_script", JString("doc[%s].empty ? 0 : 1".format(JString(aggregateValue.column.name))))
          }
          val size: BigInt = if (limit.isEmpty || offset.isEmpty) 0 else limit.get + offset.get
          val facetKeyVal = JObject(Map(
            "key_field" -> JString(col.column.name), facetVal._1 -> facetVal._2,
            "order" -> JString("term"), // TODO - allow different orderings
            "size" -> JNumber(size)
          ))
          val termsStats = JObject(Map("terms_stats" -> facetKeyVal))
          val facetName = FacetName(col.column.name, aggregateValue.column.name)
          val facet = (facetName.toString(), termsStats)
          facet
        }.toMap)
    }
    if (esGroupBys.size > 1) throw new NotImplementedException("Cannot handle multiple groups", groupBys(1).position)
    esGroupBys.head
  }

  private def toESFilter(filter: TypedFF[SoQLType], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    try {
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
        case FunctionCall(MonomorphicFunction(SoQLFunctions.TextToFloatingTimestamp, _), (param: StringLiteral[_]) :: Nil) =>
          val dateLit = StringLiteral(DataTypeConverter.cast(param.value, DataType.Type.CALENDAR_DATE).asInstanceOf[String], SoQLType)
          toESFilter(dateLit.asInstanceOf[TypedFF[SoQLType]], xlateCtx, level + 1)
        case FunctionCall(MonomorphicFunction(SoQLFunctions.IsNull, _), (col: ColumnRef[_]) :: Nil) =>
          JObject1("missing", JObject1("field", toESFilter(col, xlateCtx, level+1)))
        case FunctionCall(MonomorphicFunction(SoQLFunctions.IsNotNull, _), (col: ColumnRef[_]) :: Nil) =>
          JObject1("exists", JObject1("field", toESFilter(col, xlateCtx, level+1)))
        case FunctionCall(MonomorphicFunction(SoQLFunctions.Not, _), arg :: Nil) =>
          JObject1("not", toESFilter(arg, xlateCtx, level+1, canScript))
        case FunctionCall(MonomorphicFunction(SoQLFunctions.In, _), (col: ColumnRef[_]) :: args) =>
          toESFilter(col, xlateCtx, level + 1) match {
            case JString(col) => JObject1("terms", JObject1(col, JArray(args.map(toESFilter(_, xlateCtx, level+1)))))
            case _ => throw new Exception("should never happen")
          }
        case fn@FunctionCall(MonomorphicFunction(SoQLFunctions.NotIn, _), (col: ColumnRef[_]) :: args) =>
          val deNegFn = FunctionCall(MonomorphicFunction(SoQLFunctions.In, fn.function.bindings), fn.parameters)
          JObject1("not", toESFilter(deNegFn, xlateCtx, level, canScript))
        case fn: FunctionCall[_] =>
          val esFn = fn.function.function match {
            case SoQLFunctions.Eq  | SoQLFunctions.EqEq => // soql = to adaptor term
              fn.parameters match {
                case SimpleColumnLiteralExpression(colLit) =>
                  val lhs = toESFilter(colLit.col, xlateCtx, level+1)
                  val rhs = toESFilter(colLit.lit, xlateCtx, level+1)
                  JObject(Map("term" -> JObject(Map(lhs.asInstanceOf[JString].string -> rhs))))
                case _ =>
                  // require ES scripted filter
                  toESScriptFilter(filter, xlateCtx, level)
              }
            case SoQLFunctions.Neq | SoQLFunctions.BangEq =>
              val deNegFn = FunctionCall(MonomorphicFunction(SoQLFunctions.Eq, fn.function.bindings), fn.parameters)
              JObject1("not", toESFilter(deNegFn, xlateCtx, level+1, canScript))
            case SoQLFunctions.And | SoQLFunctions.Or =>
              val lhs = toESFilter( fn.parameters(0), xlateCtx, level+1, canScript)
              val rhs = toESFilter( fn.parameters(1), xlateCtx, level+1, canScript)
              JObject(Map(fn.function.name.name.substring(3) -> JArray(Seq(lhs, rhs))))
            case SoQLFunctions.UnaryMinus =>
              toESFilter(fn.parameters(0), xlateCtx, level+1) match {
                case JNumber(x) => JNumber(-x)
                case JString(x) => JString("-" + x)
                case x =>
                  throw new Exception("should never get here - negate on " + x.getClass.getName)
              }
            case SoQLFunctions.NotBetween =>
              val deNegFn = FunctionCall(MonomorphicFunction(SoQLFunctions.Between, fn.function.bindings), fn.parameters)
              JObject1("not", toESFilter(deNegFn, xlateCtx, level+1))
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
                      toESScriptFilter(filter, xlateCtx, level)
                  }
                case _ =>
                  toESScriptFilter(filter, xlateCtx, level)
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
                      toESScriptFilter(filter, xlateCtx, level)
                  }
                case _ =>
                  toESScriptFilter(filter, xlateCtx, level)
              }
            case notImplemented =>
              throw new RequireScriptFilter("Require script filter", fn.functionNamePosition)
          }
          esFn
        case notImplemented =>
          throw new RequireScriptFilter("Require script filter", filter.position)
      }

      result
    } catch {
      case ex: RequireScriptFilter =>
        if (canScript) toESScriptFilter(filter, xlateCtx, level)
        else if (level > 0) throw ex
        else throw new NotImplementedException("Expression not implemented", ex.position)
    }
  }

  private def toESScriptFilter(filter: TypedFF[SoQLType], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0): JValue = {
    log.info("switch to script filter: " + filter.toString())
    val (js, ctx) = toESScript(filter, xlateCtx, level)
    JObject1("script", JObject(Map(
      // if ESLang is not set, we use default mvel.  Some expressions like casting require js
      "lang" -> JString(ctx.getOrElse(XlateCtx.ESLang, "mvel").asInstanceOf[String]),
      "script" -> JString(js)
    )))
  }

  private def toESScript(filter: TypedFF[SoQLType], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0):
    Tuple2[String, Map[XlateCtx.Value, AnyRef]] = {

    val requireJS = (XlateCtx.ESLang -> "js")

    val result = filter match {
      case col: ColumnRef[_] =>
        ("doc['%s'].value".format(col.column.name), xlateCtx)
      case lit: StringLiteral[_] =>
        (JString(lit.value).toString, xlateCtx)
      case lit: NumberLiteral[_] =>
        (JNumber(lit.value).toString, xlateCtx)
      case FunctionCall(MonomorphicFunction(SoQLFunctions.IsNull, _), (col: ColumnRef[_]) :: Nil) =>
        ("doc['%s'].empty".format(col.column.name), xlateCtx)
      case FunctionCall(MonomorphicFunction(SoQLFunctions.IsNotNull, _), (col: ColumnRef[_]) :: Nil) =>
        ("doc['%s'].empty".format(col.column.name), xlateCtx)
      case FunctionCall(MonomorphicFunction(SoQLFunctions.Not, _), arg :: Nil) =>
        val children = toESScript(arg, xlateCtx, level+1)
        ("(!%s)".format(toESScript(arg, xlateCtx, level+1)), children._2)
      case fn: FunctionCall[_] =>
        val children = fn.parameters.map(toESScript(_, xlateCtx, level+1))
        val scriptedParams = children.map( x => x._1)
        val childrenCtx = children.foldLeft(xlateCtx) { (x, y) => x ++ y._2}
        val esFn = fn.function.function match {
          case SoQLFunctions.Eq  | SoQLFunctions.EqEq |
               SoQLFunctions.And | SoQLFunctions.Or |
               SoQLFunctions.BinaryMinus | SoQLFunctions.BinaryPlus |
               SoQLFunctions.TimesNumNum | SoQLFunctions.DivNumNum |
               SoQLFunctions.Gt | SoQLFunctions.Gte |
               SoQLFunctions.Lt | SoQLFunctions.Lte |
               SoQLFunctions.Neq| SoQLFunctions.BangEq |
               SoQLFunctions.Concat =>
            ("(%s %s %s)".format(scriptedParams(0), scriptFnMap(fn.function.name), scriptedParams(1)), childrenCtx)
          case SoQLFunctions.UnaryMinus =>
            ("-" + scriptedParams(0), childrenCtx)
          case SoQLFunctions.TextToNumber =>
            val castExpr = "parseFloat(%s)".format(scriptedParams(0))
            (castExpr, childrenCtx + requireJS)
          case SoQLFunctions.NumberToText =>
            val castExpr = "(%s).toString()".format(scriptedParams(0))
            (castExpr, childrenCtx + requireJS)
          case SoQLFunctions.Between =>
            ("(%1$s >= %2$s && %1$s <= %3$s)".format(scriptedParams(0), scriptedParams(1), scriptedParams(2)), childrenCtx)
          case SoQLFunctions.NotBetween =>
            val deNegFn = FunctionCall(MonomorphicFunction(SoQLFunctions.Between, fn.function.bindings), fn.parameters)
            val children = toESScript(deNegFn, xlateCtx, level+1)
            ("(!%s)".format(children._1), children._2)
          case SoQLFunctions.In =>
            val (lhs, lhsCtx) = toESScript(fn.parameters.head, xlateCtx, level+1)
            val rhss = fn.parameters.tail.map(toESScript(_, xlateCtx, level+1))
            val childrenCtx = rhss.foldLeft(lhsCtx) { (accCtx, rhs) => accCtx ++ rhs._2 }
            val script = rhss.map( rhs => "(%s == %s)".format(lhs, rhs._1)).mkString("(", " || ", ")")
            (script, childrenCtx)
          case SoQLFunctions.NotIn =>
            val deNegFn = FunctionCall(MonomorphicFunction(SoQLFunctions.In, fn.function.bindings), fn.parameters)
            val children = toESScript(deNegFn, xlateCtx, level+1)
            ("(!%s)".format(children._1), children._2)
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

  private val log = org.slf4j.LoggerFactory.getLogger(classOf[ESQuery])

  private val JObject0 = JObject(Map.empty)

  private def JObject1(k: String, v: JValue): JObject = JObject(Map(k -> v))

  private val analyzer = new SoQLAnalyzer(SoQLTypeInfo)

  private val scriptFnMap = Map(
    SoQLFunctions.Eq.name -> "==",
    SoQLFunctions.EqEq.name -> "==",
    SoQLFunctions.And.name -> "&&",
    SoQLFunctions.Or.name -> "||",
    SoQLFunctions.Concat.name -> "+",

    SoQLFunctions.Gt.name -> ">",
    SoQLFunctions.Gte.name -> ">=",
    SoQLFunctions.Lt.name -> "<",
    SoQLFunctions.Lte.name -> "<=",
    SoQLFunctions.Neq.name -> "!=",
    SoQLFunctions.BangEq.name -> "!=",

    SoQLFunctions.TimesNumNum.name -> "*",
    SoQLFunctions.DivNumNum.name -> "/",
    SoQLFunctions.BinaryMinus.name -> "-",
    SoQLFunctions.BinaryPlus.name -> "+"
  )
}
