package com.socrata.soql.adapter.elasticsearch

import com.socrata.soql.adapter.{NotImplementedException, XlateCtx}
import com.rojoma.json.ast._
import com.blist.rows.format.DataTypeConverter
import com.blist.models.views.DataType
import com.socrata.soql.functions.SoQLFunctions
import com.socrata.soql.types.{SoQLBoolean, SoQLType}
import com.socrata.soql.typed._
import com.socrata.soql.typed.StringLiteral
import com.rojoma.json.ast.JObject
import com.rojoma.json.ast.JBoolean
import com.rojoma.json.ast.JArray
import com.socrata.soql.typed.ColumnRef
import com.socrata.soql.typed.FunctionCall
import com.rojoma.json.ast.JString
import com.socrata.soql.functions.MonomorphicFunction

object ESFunction {

  import ESQuery.JObject1

  def textToFloatingTimestamp[T](fn: FunctionCall[T], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    fn.parameters match {
      case (param: StringLiteral[_]) :: Nil =>
        val dateLit = StringLiteral(DataTypeConverter.cast(param.value, DataType.Type.CALENDAR_DATE).asInstanceOf[String], SoQLType)
        ESCoreExpr(dateLit).toFilter(xlateCtx, level + 1)
      case _ => throw new RequireScriptFilter("Require script filter", fn.position)
    }
  }

  def isNull[T](fn: FunctionCall[T], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    fn.parameters match {
      case (col: ColumnRef[_]) :: Nil =>
        JObject1("missing", JObject1("field", ESCoreExpr(col).toFilter(xlateCtx, level+1)))
      case _  => throw new RequireScriptFilter("Require script filter", fn.position)
    }
  }

  def isNotNull[T](fn: FunctionCall[T], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    fn.parameters match {
      case (col: ColumnRef[_]) :: Nil =>
        JObject1("exists", JObject1("field", ESCoreExpr(col).toFilter(xlateCtx, level+1)))
      case _  => throw new RequireScriptFilter("Require script filter", fn.position)
    }
  }

  def not[T](fn: FunctionCall[T], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    fn.parameters match {
      case arg :: Nil =>
        JObject1("not", ESCoreExpr(arg).toFilter(xlateCtx, zeroStandaloneBooleanLevel(arg, level+1), canScript))
      case _  => throw new RequireScriptFilter("Require script filter", fn.position)
    }
  }

  def in[T](fn: FunctionCall[T], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    fn.parameters match {
      case (col: ColumnRef[_]) :: params =>
        ESCoreExpr(col).toFilter(xlateCtx, level + 1) match {
          case JString(col) =>
            JObject1("terms", JObject1(col, JArray(params.map( a => ESCoreExpr(a).toFilter(xlateCtx, level+1)))))
          case _ => throw new Exception("should never happen")
        }
      case _ => throw new RequireScriptFilter("Require script filter", fn.position)
    }
  }

  def notIn[T](fn: FunctionCall[T], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    fn.parameters match {
      case (col: ColumnRef[_]) :: _ =>
        val fnc = FunctionCall(MonomorphicFunction(SoQLFunctions.In, fn.function.bindings), fn.parameters)
        val deNegFn = ESFunctionCall(fnc)
        JObject1("not", deNegFn.toFilter(xlateCtx, level, canScript))
      case _ => throw new RequireScriptFilter("Require script filter", fn.position)
    }
  }

  def equ[T](fn: FunctionCall[T], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    fn.parameters match {
      case SimpleColumnLiteralExpression(colLit) =>
        val lhs = ESCoreExpr(colLit.col).toFilter(xlateCtx, level+1)
        val rhs = ESCoreExpr(colLit.lit).toFilter(xlateCtx, level+1)
        JObject(Map("term" -> JObject(Map(lhs.asInstanceOf[JString].string -> rhs))))
      case _ =>
        throw new RequireScriptFilter("Require script filter", fn.position)
    }
  }

  def neq[T](fn: FunctionCall[T], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    val deNegFn = FunctionCall(MonomorphicFunction(SoQLFunctions.Eq, fn.function.bindings), fn.parameters)
    JObject1("not", ESFunctionCall(deNegFn).toFilter(xlateCtx, level+1, canScript))
  }

  def andOr[T](fn: FunctionCall[T], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    val l = fn.parameters(0)
    val r = fn.parameters(1)
    val lhs = ESCoreExpr(l).toFilter(xlateCtx, zeroStandaloneBooleanLevel(l, level+1), canScript)
    val rhs = ESCoreExpr(r).toFilter(xlateCtx, zeroStandaloneBooleanLevel(r, level+1), canScript)
    JObject(Map(fn.function.name.name.substring(3) -> JArray(Seq(lhs, rhs))))
  }

  def unaryMinus[T](fn: FunctionCall[T], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    ESCoreExpr(fn.parameters(0)).toFilter(xlateCtx, level+1, canScript) match {
      case JNumber(x) => JNumber(-x)
      case JString(x) => JString("-" + x)
      case x => throw new Exception("should never get here - negate on " + x.getClass.getName)
    }
  }

  def notBetween[T](fn: FunctionCall[T], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    val deNegFn = FunctionCall(MonomorphicFunction(SoQLFunctions.Between, fn.function.bindings), fn.parameters)
    JObject1("not", ESFunctionCall(deNegFn).toFilter(xlateCtx, level+1, canScript))
  }

  def withinCircle[T](fn: FunctionCall[T], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    val latLon = JObject(Map(
      "lat" -> ESCoreExpr(fn.parameters(1)).toFilter(xlateCtx, level+1),
      "lon" -> ESCoreExpr(fn.parameters(2)).toFilter(xlateCtx, level+1)) )
    JObject1("geo_distance", JObject(Map(
      "distance" -> ESCoreExpr(fn.parameters(3)).toFilter(xlateCtx, level+1),
      "location" -> latLon)))
  }

  def withinBox[T](fn: FunctionCall[T], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
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

  def between[T](fn: FunctionCall[T], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
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
  def lgte[T](fn: FunctionCall[T], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
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

  def like[T](fn: FunctionCall[T], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    fn.parameters match {
      case (col: ColumnRef[_]) :: (lit: StringLiteral[_]) :: _ =>
        ESCoreExpr(col).toFilter(xlateCtx, level+1) match {
          case JString(col) =>
            val startsWithRx = """([^\%]+)[\%]""".r
            lit.value match {
              case startsWithRx(s) => JObject1("prefix", JObject1(col, JString(s)))
              case wildCard => JObject1("query", JObject1("wildcard", JObject1(col,
                JString(lit.value.replace("*", "").replace("%", "*")))))
            }
          case _ => throw new Exception("should never get here, %s".format(fn.function.name))
        }
      case _ => throw new RequireScriptFilter("Require script filter", fn.position)
    }
  }

  def notLike[T](fn: FunctionCall[T], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    val deNegFn = FunctionCall(MonomorphicFunction(SoQLFunctions.Like, fn.function.bindings), fn.parameters)
    JObject1("not", ESFunctionCall(deNegFn).toFilter(xlateCtx, level+1, canScript))
  }

  /**
   * Support "select * where booleanColumn (equivalent to select * where booleanColumn = true)
   */
  private def zeroStandaloneBooleanLevel[T](expr: CoreExpr[T], level: Int): Int = {
    expr match {
      case col: ColumnRef[_] if (col.typ == SoQLBoolean) => 0
      case _ => level
    }
  }
}
