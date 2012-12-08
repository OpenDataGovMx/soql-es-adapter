package com.socrata.soql.adapter.elasticsearch

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers
import com.socrata.rows.ESTestGateway
import com.rojoma.json.io.JsonReader


class ESQueryTest extends FunSuite with MustMatchers {
  import ESQueryTest._

  test("column function literal") {
    toEsQuery("select * where case_number in ( 'HP109135', 'HP110029', 'HP110438')") must equal(json(
      """
        {
          "filter" :
            { "terms" : { "case_number" : [ "HP109135", "HP110029", "HP110438" ] } }
        }
      """))

    toEsQuery("select * where primary_type = 'BURGLARY' limit 5") must equal(json(
      """{ "filter" : { "term" : { "primary_type" : "BURGLARY" } }, "size" : 5 }""".stripMargin))

    toEsQuery("select * where primary_type < 'BURGLARY' limit 5") must equal(json(
      """
        {
          "filter" :
            {
              "range" :
                { "primary_type" : { "to" : "BURGLARY", "include_upper" : false } }
            },
          "size" : 5
        }
      """))
    toEsQuery("select * where primary_type between 'B' and 'CZ' limit 5") must equal(json(
      """
        {
          "filter" :
            {
              "range" :
                {
                  "primary_type" :
                    {
                      "from" : "B",
                      "to" : "CZ",
                      "include_upper" : true,
                      "include_lower" : true
                    }
                }
            },
          "size" : 5
        }
      """))
    toEsQuery("select * where id <= 1000000 limit 1") must equal(json(
      """
        {
          "filter" : {
                       "range" : { "id" : { "to" : 1000000, "include_upper" : true } }
                     },
          "size" : 1
        }
      """))
    toEsQuery("select * where 100000000 < id limit 1") must equal(json(
      """
        {
          "filter" :
            { "range" : { "id" : { "from" : 100000000, "include_lower" : false } } },
          "size" : 1
        }
      """))
    toEsQuery("select * where id between 6006000 and 6006999 limit 3") must equal(json(
      """
        {
          "filter" :
            {
              "range" :
                {
                  "id" :
                    {
                      "from" : 6006000,
                      "to" : 6006999,
                      "include_upper" : true,
                      "include_lower" : true
                    }
                }
            },
          "size" : 3
        }
      """))
    toEsQuery("select * where (2005 = year and primary_type = 'THEFT') or (year = 2006 and primary_type = 'BATTERY')") must equal(json(
      """
        {
          "filter" :
            {
              "or" :
                [
                  {
                    "and" :
                      [
                        { "term" : { "year" : 2005 } },
                        { "term" : { "primary_type" : "THEFT" } }
                      ]
                  },
                  {
                    "and" :
                      [
                        { "term" : { "year" : 2006 } },
                        { "term" : { "primary_type" : "BATTERY" } }
                      ]
                  }
                ]
            }
        }
      """))
    toEsQuery("select * where year = 2008 and primary_type = 'BULGARY' order by case_number") must equal(json(
      """
        {
          "filter" :
            {
              "and" :
                [
                  { "term" : { "year" : 2008 } },
                  { "term" : { "primary_type" : "BULGARY" } }
                ]
            },
          "sort" : [ { "case_number" : "asc" } ]
        }
      """))
    toEsQuery("select primary_type, count(id), max(id) where year = 2009  group by primary_type") must equal(json(
      """
        {
          "query" :
            {
              "filtered" :
                {
                  "query" : { "match_all" : {} },
                  "filter" : { "term" : { "year" : 2009 } }
                }
            },
          "facets" :
            {
              "fc_primary_type_id" :
                {
                  "terms_stats" :
                    {
                      "key_field" : "primary_type",
                      "value_field" : "id",
                      "order" : "term",
                      "size" : 0
                    }
                }
            },
          "size" : 0
        }
      """))
    toEsQuery("select primary_type, count(case_number), count(id), max(id), min(id), avg(id), sum(id) where year = 2010 group by primary_type offset 1 limit 3") must equal(json(
      """
        {
          "query" :
            {
              "filtered" :
                {
                  "query" : { "match_all" : {} },
                  "filter" : { "term" : { "year" : 2010 } }
                }
            },
          "facets" :
            {
              "fc_primary_type_case_number" :
                {
                  "terms_stats" :
                    {
                      "key_field" : "primary_type",
                      "value_script" : "doc[\"case_number\"].empty ? 0 : 1",
                      "order" : "term",
                      "size" : 4
                    }
                },
              "fc_primary_type_id" :
                {
                  "terms_stats" :
                    {
                      "key_field" : "primary_type",
                      "value_field" : "id",
                      "order" : "term",
                      "size" : 4
                    }
                }
            },
          "from" : 1,
          "size" : 0
        }
      """))

    toEsQuery("select * where not (case_number = 'HP109135' or case_number = 'HP110029')") must equal(json(
      """
       {
          "filter" :
            {
              "not" :
                {
                  "or" :
                    [
                      { "term" : { "case_number" : "HP109135" } },
                      { "term" : { "case_number" : "HP110029" } }
                    ]
                }
            }
       }
      """))
  }

  test("require mvel (default) scripted filter") {
    toEsQuery("select * where id + 2008  = year limit 3") must equal(json(
      """
        {
          "filter" :
            {
              "script" :
                {
                  "lang" : "mvel",
                  "script" : "((doc['id'].value + 2008) == doc['year'].value)"
                }
            },
          "size" : 3
        }
      """))
    toEsQuery("select * where case_number || 'x' = description") must equal(json(
      """
        {
          "filter" :
            {
              "script" :
                {
                  "lang" : "mvel",
                  "script" : "((doc['case_number'].value + \"x\") == doc['description'].value)"
                }
            }
        }
      """))
    toEsQuery("select * where year between 2005 and 2004 + 2") must equal(json(
      """
        {
          "filter" :
            {
              "script" :
                {
                  "lang" : "mvel",
                  "script" : "(doc['year'].value >= 2005 && doc['year'].value <= (2004 + 2))"
                }
            }
        }
      """))
    toEsQuery("select * where id between year and year + 3000") must equal(json(
      """
        {
          "filter" :
            {
              "script" :
                {
                  "lang" : "mvel",
                  "script" : "(doc['id'].value >= doc['year'].value && doc['id'].value <= (doc['year'].value + 3000))"
                }
            }
        }
      """))
    toEsQuery("select * where 7000 between year and 8000") must equal(json(
      """
        {
          "filter" :
            {
              "script" :
                {
                  "lang" : "mvel",
                  "script" : "(7000 >= doc['year'].value && 7000 <= 8000)"
                }
            }
        }
      """))
    toEsQuery("select * where 7000 > year + 1000") must equal(json(
      """
        {
          "filter" :
            {
              "script" :
                { "lang" : "mvel", "script" : "(7000 > (doc['year'].value + 1000))" }
            }
        }
      """))
  }



  test("require javascript scripted filter") {
    toEsQuery("select * where fbi_code::number = id") must equal(json(
      """
        {
          "filter" :
            {
              "script" :
                {
                  "lang" : "js",
                  "script" : "(parseFloat(doc['fbi_code'].value) == doc['id'].value)"
                }
            }
        }
      """))
    toEsQuery("select * where year::text = '20' || '00'") must equal(json(
      """
        {
          "filter" :
            {
              "script" :
                {
                  "lang" : "js",
                  "script" : "(doc['year'].value.toString() == (\"20\" + \"00\"))"
                }
            }
        }
      """))
  }

  test("mixed script/non-script or mixed script langs") {
    toEsQuery("select * where updated_on is null") must equal(json(
      """
        { "filter" : { "missing" : { "field" : "updated_on" } } }
      """))

    toEsQuery("select * where updated_on is not null") must equal(json(
      """
        { "filter" : { "exists" : { "field" : "updated_on" } } }
      """))

    toEsQuery("select * where case_number = 'HP109135' or id = year") must equal(json(
      """
        {
          "filter" :
            {
              "or" :
                [
                  { "term" : { "case_number" : "HP109135" } },
                  {
                    "script" :
                      {
                        "lang" : "mvel",
                        "script" : "(doc['id'].value == doc['year'].value)"
                      }
                  }
                ]
            }
        }
      """))

    toEsQuery("select * where id = year or year::text = '2010'") must equal(json(
      """
        {
          "filter" :
            {
              "or" :
                [
                  {
                    "script" :
                      {
                        "lang" : "mvel",
                        "script" : "(doc['id'].value == doc['year'].value)"
                      }
                  },
                  {
                    "script" :
                      {
                        "lang" : "js",
                        "script" : "(doc['year'].value.toString() == \"2010\")"
                      }
                  }
                ]
            }
        }
      """))
  }
}

object ESQueryTest {
  val esGateway = new ESTestGateway()
  val esQuery = new ESQuery("visitors", esGateway)

  def toEsQuery(soql: String): Any = esQuery.full(soql)

  def json(json: String) = (new JsonReader(json)).read().toString
}