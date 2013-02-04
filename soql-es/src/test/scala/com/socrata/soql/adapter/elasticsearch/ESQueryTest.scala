package com.socrata.soql.adapter.elasticsearch

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers
import com.socrata.rows.ESTestGateway
import com.rojoma.json.io.JsonReader


class ESQueryTest extends FunSuite with MustMatchers {
  import ESQueryTest._

  test("column function literal") {
    toEsQuery("select * where case_number in ( 'HP109135', 'HP110029', 'HP10438')") must equal(json(
      """
        {
          "filter" :
            { "terms" : { "case_number" : [ "hp109135", "hp110029", "hp10438" ] } }
        }
      """))

    toEsQuery("select * where primary_type = 'BURGLARY' limit 5") must equal(json(
      """{ "filter" : { "term" : { "primary_type" : "burglary" } }, "size" : 5 }""".stripMargin))

    toEsQuery("select * where primary_type < 'BURGLARY' limit 5") must equal(json(
      """
        {
          "filter" :
            {
              "range" :
                { "primary_type" : { "to" : "burglary", "include_upper" : false } }
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
                      "from" : "b",
                      "to" : "cz",
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
    toEsQuery("select * where id not between 6006000 and 6006999 limit 3") must equal(json(
      """
        {
          "filter" :
            {
              "not" :
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
                        { "term" : { "primary_type" : "theft" } }
                      ]
                  },
                  {
                    "and" :
                      [
                        { "term" : { "year" : 2006 } },
                        { "term" : { "primary_type" : "battery" } }
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
                  { "term" : { "primary_type" : "bulgary" } }
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
              "fc:primary_type:id" :
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
              "fc:primary_type:case_number" :
                {
                  "terms_stats" :
                    {
                      "key_field" : "primary_type",
                      "value_script" : "doc[\"case_number\"].empty ? 0 : 1",
                      "order" : "term",
                      "size" : 4
                    }
                },
              "fc:primary_type:id" :
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
                      { "term" : { "case_number" : "hp109135" } },
                      { "term" : { "case_number" : "hp110029" } }
                    ]
                }
            }
       }
      """))

    toEsQuery("select * where case_number like 'HP%'") must equal (json(
      """
        { "filter" : { "prefix" : { "case_number" : "hp" } } }
      """))

    toEsQuery("select * where case_number like 'HP%123%'") must equal (json(
      """
        { "filter" : { "query" : { "wildcard" : { "case_number" : "hp*123*" } } } }
      """))

    toEsQuery("select * where updated_on is null") must equal(json(
      """
        { "filter" : { "missing" : { "field" : "updated_on" } } }
      """))

    toEsQuery("select * where updated_on is not null") must equal(json(
      """
        { "filter" : { "exists" : { "field" : "updated_on" } } }
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
    toEsQuery("select * where case_number in ( 'HP109135', 'HP' || '110029')") must equal(json(
      """
        {
          "filter" :
            {
              "script" :
                {
                  "lang" : "mvel",
                  "script" : "((doc['case_number'].value == \"hp109135\") || (doc['case_number'].value == (\"hp\" + \"110029\")))"
                }
            }
        }
      """))
    toEsQuery("select * where 'HP109135' in ('HP109135') limit 2") must equal(json(
      """
        {
          "filter" :
            {
              "script" :
                { "lang" : "mvel", "script" : "((\"hp109135\" == \"hp109135\"))" }
            },
          "size" : 2
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
                  "script" : "((doc['year'].value).toString() == (\"20\" + \"00\"))"
                }
            }
        }
      """))
  }

  test("mixed script/non-script or mixed script langs") {

    toEsQuery("select * where case_number = 'HP109135' or id = year") must equal(json(
      """
        {
          "filter" :
            {
              "or" :
                [
                  { "term" : { "case_number" : "hp109135" } },
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
                        "script" : "((doc['year'].value).toString() == \"2010\")"
                      }
                  }
                ]
            }
        }
      """))
  }

  test("support of standalone boolean column") {
    toEsQuery("select * where arrest") must equal(json(
      """{ "filter" : { "term" : { "arrest" : true } } }"""))

    toEsQuery("select * where arrest = false") must equal(json(
      """{ "filter" : { "term" : { "arrest" : false } } }"""))

    toEsQuery("select * where not arrest") must equal(json(
      """{ "filter" : { "not" : { "term" : { "arrest" : true } } } }"""))

    toEsQuery("select * where  case_number = 'HP109135' or arrest") must equal(json(
      """
        {
          "filter" :
            {
              "or" :
                [
                  { "term" : { "case_number" : "hp109135" } },
                  { "term" : { "arrest" : true } }
                ]
            }
        }
      """.stripMargin))

    toEsQuery("select * where not arrest and case_number = 'HP109135'") must equal(json(
      """
        {
          "filter" :
            {
              "and" :
                [
                  { "not" : { "term" : { "arrest" : true } } },
                  { "term" : { "case_number" : "hp109135" } }
                ]
            }
        }
      """.stripMargin))
  }

  // TODO: honor size in columns facet
  test("multi columns grouping") {

    toEsQuery("select primary_type, arrest, count(id), min(id) group by primary_type, arrest order by primary_type, arrest") must equal(json(
      """
        {
          "facets" :
            {
              "fc:_multi:id" :
                {
                  "columns" :
                    {
                      "key_fields" : [ "primary_type", "arrest" ],
                      "value_field" : "id",
                      "size" : 10,
                      "from" : 0,
                      "orders" : [ "primary_type", "arrest" ]
                    }
                }
            },
          "size" : 0
        }
      """))

    toEsQuery("select primary_type, arrest, count(id) group by primary_type, arrest order by count(id) desc, primary_type") must equal(json(
      """
        {
          "facets" :
            {
              "fc:_multi:id" :
                {
                  "columns" :
                    {
                      "key_fields" : [ "primary_type", "arrest" ],
                      "value_field" : "id",
                      "size" : 10,
                      "from" : 0,
                      "orders" : [ ":count desc", "primary_type" ]
                    }
                }
            },
          "size" : 0
        }
      """))
  }

  test("property") {

    toEsQuery("select * where object.prop1.prop2.prop3::text = 'one'") must equal(json(
      """
        {
          "filter" :
            {
              "script" :
                {
                  "lang" : "js",
                  "script" : "try { ((_source.object.prop1.prop2.prop3).toString() == \"one\"); } catch(e) { null; }"
                }
            }
        }
      """))
  }

  test("index") {

    toEsQuery("select * where array[1][2][3]::number = 1") must equal(json(
      """
        {
          "filter" :
            {
              "script" :
                {
                  "lang" : "js",
                  "script" : "try { (parseFloat(_source.array[1][2][3]) == 1); } catch(e) { null; }"
                }
            }
        }
      """))
  }

  test("property and index") {

    toEsQuery("select * where object.one[1]::number = 2") must equal(json(
      """
        {
          "filter" :
            {
              "script" :
                {
                  "lang" : "js",
                  "script" : "try { (parseFloat(_source.object.one[1]) == 2); } catch(e) { null; }"
                }
            }
        }
      """))

    toEsQuery("select * where array[0].two[1]::text = 'one'") must equal(json(
      """
        {
          "filter" :
            {
              "script" :
                {
                  "lang" : "js",
                  "script" : "try { ((_source.array[0].two[1]).toString() == \"one\"); } catch(e) { null; }"
                }
            }
        }
      """))
  }
}

object ESQueryTest {
  val esGateway = new ESTestGateway()
  val esQuery = new ESQuery("visitors", esGateway, None)

  def toEsQuery(soql: String): Any = esQuery.full(soql)._1

  def json(json: String) = (new JsonReader(json)).read().toString
}