package com.socrata.es.soql.query

import com.rojoma.json.ast._
import com.socrata.soql.functions.SoQLFunctions
import com.socrata.soql.types.{SoQLText, SoQLFloatingTimestamp, SoQLBoolean, SoQLType}
import com.socrata.soql.typed._
import com.socrata.soql.typed.StringLiteral
import com.rojoma.json.ast.JObject
import com.rojoma.json.ast.JBoolean
import com.rojoma.json.ast.JArray
import com.socrata.soql.typed.ColumnRef
import com.socrata.soql.typed.FunctionCall
import com.rojoma.json.ast.JString
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.environment.FunctionName
import com.socrata.soql.ast.SpecialFunctions
import com.socrata.es.meta.ESColumnMap
import com.socrata.es.soql.{SoQLAdapterException, NotImplementedException}

object ESFunction {

  import ESQuery.JObject1
  import SoQLFunctions._

  def apply(functionName: FunctionName) = funMap.get(functionName)

  // These functions are not supported in SoQL Analysis yet.
  val Search = new MonomorphicFunction("search", SpecialFunctions.Operator("search"), Seq(SoQLText), None, SoQLBoolean)

  private val funMap = Map(
    IsNull.name -> isNull _,
    IsNotNull.name -> isNotNull _,
    Not.name -> not _,
    In.name -> in _,
    NotIn.name -> notIn _,
    Eq.name -> equ _,
    EqEq.name -> equ _,
    TextToFloatingTimestamp.name -> textToFloatingTimestamp _,
    Neq.name -> neq _,
    BangEq.name -> neq _,
    And.name -> andOr _,
    Or.name -> andOr _,
    UnaryMinus.name -> unaryMinus _,
    NotBetween.name -> notBetween _,
    WithinCircle.name -> withinCircle _,
    WithinBox.name -> withinBox _,
    Between.name -> between _,
    Lt.name -> lgte _,
    Lte.name -> lgte _,
    Gt.name -> lgte _,
    Gte.name -> lgte _,
    Like.name -> like _,
    NotLike.name -> notLike _,
    Search.function.name -> search _)

  private def textToFloatingTimestamp(fn: FunctionCall[_], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    fn.parameters match {
      case Seq(StringLiteral(lit, _)) =>
        ESColumnMap(SoQLFloatingTimestamp).toES(lit) match {
          case JString(date) =>
            val dateLit = StringLiteral(date, SoQLType)(fn.position)
            ESCoreExpr(dateLit).toFilter(xlateCtx, level + 1)
        }
      case _ => throw new RequireScriptFilter("Require script filter", fn.position)
    }
  }

  private def isNull(fn: FunctionCall[_], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    fn.parameters match {
      case Seq(col: ColumnRef[_]) =>
        JObject1("missing", JObject1("field", ESCoreExpr(col).toFilter(xlateCtx, level+1)))
      case _  => throw new RequireScriptFilter("Require script filter", fn.position)
    }
  }

  private def isNotNull(fn: FunctionCall[_], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    fn.parameters match {
      case Seq(col: ColumnRef[_]) =>
        JObject1("exists", JObject1("field", ESCoreExpr(col).toFilter(xlateCtx, level+1)))
      case _  => throw new RequireScriptFilter("Require script filter", fn.position)
    }
  }

  private def not(fn: FunctionCall[_], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    fn.parameters match {
      case Seq(arg) =>
        JObject1("not", ESCoreExpr(arg).toFilter(xlateCtx, zeroStandaloneBooleanLevel(arg, level+1), canScript))
      case _  => throw new RequireScriptFilter("Require script filter", fn.position)
    }
  }

  private def in(fn: FunctionCall[_], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    fn.parameters match {
      case Seq(col: ColumnRef[_], params@_*) =>
        ESCoreExpr(col).toFilter(xlateCtx, level + 1) match {
          case JString(col) =>
            JObject1("terms", JObject1(col, JArray(params.map( a => ESCoreExpr(a).toFilter(xlateCtx, level+1)))))
          case _ => throw new Exception("should never happen")
        }
      case _ => throw new RequireScriptFilter("Require script filter", fn.position)
    }
  }

  private def notIn(fn: FunctionCall[_], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    fn.parameters match {
      case Seq(col: ColumnRef[_], _*) =>
        val fnc = FunctionCall(MonomorphicFunction(SoQLFunctions.In, fn.function.bindings), fn.parameters)(fn.position, fn.functionNamePosition)
        val deNegFn = ESFunctionCall(fnc)
        JObject1("not", deNegFn.toFilter(xlateCtx, level, canScript))
      case _ => throw new RequireScriptFilter("Require script filter", fn.position)
    }
  }

  private def equ(fn: FunctionCall[_], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    fn.parameters match {
      case SimpleColumnLiteralExpression(colLit) =>
        val lhs = ESCoreExpr(colLit.col).toFilter(xlateCtx, level+1)
        val rhs = ESCoreExpr(colLit.lit).toFilter(xlateCtx, level+1)
        JObject(Map("term" -> JObject(Map(lhs.asInstanceOf[JString].string -> rhs))))
      case _ =>
        throw new RequireScriptFilter("Require script filter", fn.position)
    }
  }

  private def neq(fn: FunctionCall[_], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    val deNegFn = FunctionCall(MonomorphicFunction(SoQLFunctions.Eq, fn.function.bindings), fn.parameters)(fn.position, fn.functionNamePosition)
    JObject1("not", ESFunctionCall(deNegFn).toFilter(xlateCtx, level+1, canScript))
  }

  private def andOr(fn: FunctionCall[_], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    val l = fn.parameters(0)
    val r = fn.parameters(1)
    val lhs = ESCoreExpr(l).toFilter(xlateCtx, zeroStandaloneBooleanLevel(l, level+1), canScript)
    val rhs = ESCoreExpr(r).toFilter(xlateCtx, zeroStandaloneBooleanLevel(r, level+1), canScript)
    JObject(Map(fn.function.name.name.substring(3) -> JArray(Seq(lhs, rhs))))
  }

  private def unaryMinus(fn: FunctionCall[_], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    ESCoreExpr(fn.parameters(0)).toFilter(xlateCtx, level+1, canScript) match {
      case JNumber(x) => JNumber(-x)
      case JString(x) => JString("-" + x)
      case x => throw new Exception("should never get here - negate on " + x.getClass.getName)
    }
  }

  private def notBetween(fn: FunctionCall[_], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    val deNegFn = FunctionCall(MonomorphicFunction(SoQLFunctions.Between, fn.function.bindings), fn.parameters)(fn.position, fn.functionNamePosition)
    JObject1("not", ESFunctionCall(deNegFn).toFilter(xlateCtx, level+1, canScript))
  }

  private def withinCircle(fn: FunctionCall[_], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    val latLon = JObject(Map(
      "lat" -> ESCoreExpr(fn.parameters(1)).toFilter(xlateCtx, level+1),
      "lon" -> ESCoreExpr(fn.parameters(2)).toFilter(xlateCtx, level+1)) )
    JObject1("geo_distance", JObject(Map(
      "distance" -> ESCoreExpr(fn.parameters(3)).toFilter(xlateCtx, level+1),
      "location" -> latLon)))
  }

  private def withinBox(fn: FunctionCall[_], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    val nwLatLon = JObject(Map(
      "lat" -> ESCoreExpr(fn.parameters(1)).toFilter(xlateCtx, level+1),
      "lon" -> ESCoreExpr(fn.parameters(2)).toFilter(xlateCtx, level+1)) )
    val seLatLon = JObject(Map(
      "lat" -> ESCoreExpr(fn.parameters(3)).toFilter(xlateCtx, level+1),
      "lon" -> ESCoreExpr(fn.parameters(4)).toFilter(xlateCtx, level+1)) )
    ESCoreExpr(fn.parameters(0)).toFilter(xlateCtx, level+1) match {
      case JString(locCol) =>
        JObject1("geo_bounding_box",
          JObject1(locCol,
            JObject(Map(
              "top_left" -> nwLatLon,
              "bottom_right" -> seLatLon))))
      case _ =>
        throw new NotImplementedException("First argument to within_box must be a location column.", fn.functionNamePosition)
    }
  }

  private def between(fn: FunctionCall[_], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    fn.parameters match {
      case SimpleColumnLiteralExpression(colLit) if (colLit.lhsIsColumn) =>
        ESCoreExpr(colLit.col).toFilter(xlateCtx, level+1) match {
          case JString(col) =>
            JObject1("range", JObject1(col,
              JObject(Map(
                "from" -> ESCoreExpr(colLit.lit).toFilter(xlateCtx, level+1),
                "to" -> ESCoreExpr(colLit.lit2.get).toFilter(xlateCtx, level+1),
                "include_upper" -> JBoolean(true),
                "include_lower" -> JBoolean(true)))))
          case _ => throw new RequireScriptFilter("Require script filter", fn.position)
        }
      case _ => throw new RequireScriptFilter("Require script filter", fn.position)
    }
  }

  /**
   * Handle >, >=, <, <=
   */
  private def lgte(fn: FunctionCall[_], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    val inclusive = (fn.function.function == SoQLFunctions.Lte || fn.function.function == SoQLFunctions.Gte)
    fn.parameters match {
      case SimpleColumnLiteralExpression(colLit) =>
        val lhsIsColumn =
          if (fn.function.function == SoQLFunctions.Lte || fn.function.function == SoQLFunctions.Lt) colLit.lhsIsColumn
          else !colLit.lhsIsColumn
        ESCoreExpr(colLit.col).toFilter(xlateCtx, level+1) match {
          case JString(col) =>
            val toOrFrom = if (lhsIsColumn) "to" else "from"
            val upperOrLower = if (lhsIsColumn) "include_upper" else "include_lower"
            JObject1("range", JObject1(col,
              JObject(Map(
                toOrFrom -> ESCoreExpr(colLit.lit).toFilter(xlateCtx, level+1),
                upperOrLower -> JBoolean(inclusive)))))
          case _ => throw new RequireScriptFilter("Require script filter", fn.position)
        }
      case _ => throw new RequireScriptFilter("Require script filter", fn.position)
    }
  }

  private def like(fn: FunctionCall[_], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    fn.parameters match {
      case Seq(col: ColumnRef[_], lit: StringLiteral[_], _*) =>
        ESCoreExpr(col).toFilter(xlateCtx, level+1) match {
          case JString(col) =>
            val startsWithRx = """([^\%]+)[\%]""".r
            val JString(litVal) = ESCoreExpr(lit).toFilter(xlateCtx, level)
            litVal match {
              case startsWithRx(s) =>
                JObject1("prefix", JObject1(col, JString(s)))
              case wildCard =>
                JObject1("query", JObject1("wildcard", JObject1(col,
                  JString(litVal.replace("*", "").replace("%", "*")))))
            }
          case _ => throw new Exception("should never get here, %s".format(fn.function.name))
        }
      case _ => throw new RequireScriptFilter("Require script filter", fn.position)
    }
  }

  private def notLike(fn: FunctionCall[_], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    val deNegFn = FunctionCall(MonomorphicFunction(SoQLFunctions.Like, fn.function.bindings), fn.parameters)(fn.position, fn.functionNamePosition)
    JObject1("not", ESFunctionCall(deNegFn).toFilter(xlateCtx, level+1, canScript))
  }

  /**
   * Support "select * where booleanColumn (equivalent to select * where booleanColumn = true)
   */
  private def zeroStandaloneBooleanLevel(expr: CoreExpr[_], level: Int): Int = {
    expr match {
      case col: ColumnRef[_] if (col.typ == SoQLBoolean) => 0
      case _ => level
    }
  }

  private def search(fn: FunctionCall[_], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    fn.parameters match {
      case Seq(StringLiteral(s, _)) =>
        JObject(Map("query" ->
          JObject(Map("query_string" ->
            JObject(Map( "default_field" -> JString("_all") , "query" -> JString(s)))
          ))))
      case _ =>
        throw new SoQLAdapterException("unexpected search argument type", fn.position)
    }
  }
}
