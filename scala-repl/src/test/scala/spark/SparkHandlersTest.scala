/**
 * Copyright 2020 Jetbrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package spark

import org.apache.spark.sql.SparkSession
import org.codehaus.jettison.json.JSONObject
import org.jetbrains.ztools.scala.{ReplAware, VariablesView, VariablesViewImpl}
import org.junit.Assert.assertEquals
import org.junit.Test
import spark.handlers.{DatasetHandler, RDDHandler, SparkSessionHandler}

import scala.tools.nsc.interpreter.IMain

class SparkHandlersTest extends ReplAware {

  private trait Repl {
    def eval(code: String): Unit

    def getVariablesView(depth: Int = 3, enableProfiling: Boolean = false): VariablesView
  }

  @Test
  def simpleTest(): Unit = {
    withRepl {
      repl =>
        val view = repl.getVariablesView()
        repl.eval(
          """
            |import spark.implicits._
            |val df = sc.parallelize(List((1, "hello"), (2, "world"))).toDF("id", "name")
            |""".stripMargin)
        println(view.toJsonObject.toString(2))
    }
  }

  @Test
  def testRDDHandler(): Unit =
    withRepl {
      repl =>
        val view = repl.getVariablesView()
        repl.eval(
          """
        val rdd = sc.parallelize(List((1, "hello"), (2, "world")))
        """)
        val json = view.toJsonObject
        println(json.toString(2))
        assertEquals("org.apache.spark.rdd.RDD[(Int, String)]", getInPath[String](json, "rdd.type"))
        assertEquals("Int", getInPath[Int](json, "rdd.value.id.type"))
        assertEquals("Int", getInPath[Int](json, "rdd.value.getNumPartitions().type"))
        assertEquals("org.apache.spark.storage.StorageLevel", getInPath[String](json, "rdd.value.getStorageLevel().type"))
    }

  @Test
  def testSparkSessionHandler(): Unit =
    withRepl {
      repl =>
        val view = repl.getVariablesView()
        repl.eval(
          """
            |import org.apache.spark.sql.SparkSession
            |class A(val spark: SparkSession)
            |val a = new A(spark)
            |""".stripMargin)
        val json = view.toJsonObject
        println(json.toString(2))
    }

  @Test
  def testSparkContextHandler(): Unit = {

  }

  @Test
  def testMissingDataType(): Unit = {
    withRepl { repl =>
      val view = repl.getVariablesView()

      repl.eval(
        """
          |val sqlContext= new org.apache.spark.sql.SQLContext(sc)
          |
          |import sqlContext.implicits._
          |val bankText = sc.parallelize(List("42, \"foo\", \"bar\", \"baz\", 69"))
          |
          |case class Bank(age: Integer, job: String, marital: String, education: String, balance: Integer)
          |
          |val bank = bankText.map(s => s.split(",")).filter(s => s(0) != "\"age\"").map(
          |    s => Bank(s(0).toInt,
          |            s(1).replaceAll("\"", ""),
          |            s(2).replaceAll("\"", ""),
          |            s(3).replaceAll("\"", ""),
          |            s(4).toInt
          |        )
          |).toDF()
          |""".stripMargin)
      val json = view.toJsonObject
      assertEquals("", view.errors.mkString(","))
      val schemaArray = getInPath[JSONObject](json, "bank.value.schema()").getJSONArray("value")
      checkStructField(schemaArray.getJSONObject(0), { metadata =>
        metadata.getString("type") == "org.apache.spark.sql.types.Metadata" &&
          metadata.getString("value") == "{}"
      }, { nullable =>
        nullable.getBoolean("value") == true
      }, { dataType =>
        dataType.getString("jvm-type") == "org.apache.spark.sql.types.IntegerType$"
      }, { name =>
        name.getString("value") == "age"
      })
      checkStructField(schemaArray.getJSONObject(4), { metadata =>
        metadata.getString("ref") == "bank.schema()[0].metadata"
      }, { nullable =>
        nullable.getBoolean("value") == true
      }, { dataType =>
        dataType.getString("ref") == "bank.schema()[0].dataType"
      }, { name =>
        name.getString("value") == "balance"
      })
    }
  }

  @Test
  def testRddHandlerWithError(): Unit = withRepl {
    repl =>
      val view = repl.getVariablesView()
      repl.eval(
        """
        val rdd = sc.textFile("file:///home/nikita.pavlenko/big-data/online_retail.csv")
        """)
      val json = view.toJsonObject
      assert(json.toString() == "{}")
  }

  private def checkStructField(json: JSONObject, metadata: JSONObject => Boolean, nullable: JSONObject => Boolean,
                               dataType: JSONObject => Boolean, name: JSONObject => Boolean): Unit = {
    val innerJson = json.getJSONObject("value")
    assert(metadata(innerJson.getJSONObject("metadata")))
    assert(nullable(innerJson.getJSONObject("nullable")))
    assert(dataType(innerJson.getJSONObject("dataType")))
    assert(name(innerJson.getJSONObject("name")))
  }

  override protected def configure(variablesView: VariablesViewImpl) =
     super.configure(variablesView)
      .registerTypeHandler(new DatasetHandler())
      .registerTypeHandler(new RDDHandler())
      .registerTypeHandler(new SparkSessionHandler())

  var spark: SparkSession = _

  override def beforeRepl(): Unit = {
    super.beforeRepl()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("Simple Application").getOrCreate()
  }

  override def afterRepl(): Unit = {
    spark.close()
    super.afterRepl()
  }

  override def bindings(intp: IMain): Unit = {
    super.bindings(intp)
    intp.bind("spark", spark)
    intp.bind("sc", spark.sparkContext)
  }
}
