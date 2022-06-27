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
import org.jetbrains.ztools.scala.{ReplAware, VariablesView, VariablesViewImpl}
import org.junit.Assert.assertEquals
import org.junit.Test
import spark.handlers.{DatasetHandler, RDDHandler, SparkSessionHandler}

import scala.collection.mutable
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
        println(view.toJson)
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
        println(view.toJson)
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
        println(view.toJson)
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
      val schemaArray = getInPath[mutable.Map[String,Any]](json, "bank.value.schema()")("value").asInstanceOf[mutable.MutableList[Any]]
      checkStructField(schemaArray(0).asInstanceOf[mutable.Map[String,Any]], { metadata =>
        metadata("type") == "org.apache.spark.sql.types.Metadata" &&
          metadata("value") == "{}"
      }, { nullable =>
        nullable("value").asInstanceOf[Boolean] == true
      }, { dataType =>
        dataType("jvm-type") == "org.apache.spark.sql.types.IntegerType$"
      }, { name =>
        name("value") == "age"
      })
      checkStructField(schemaArray(4).asInstanceOf[mutable.Map[String,Any]], { metadata =>
        metadata("ref") == "bank.schema()[0].metadata"
      }, { nullable =>
        nullable("value") == true
      }, { dataType =>
        dataType("ref") == "bank.schema()[0].dataType"
      }, { name =>
        name("value") == "balance"
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
      val json = view.toJson
      assertEquals( "{}", json)
  }

  private def checkStructField(json: mutable.Map[String,Any], metadata: mutable.Map[String,Any] => Boolean, nullable: mutable.Map[String,Any] => Boolean,
                               dataType: mutable.Map[String,Any] => Boolean, name: mutable.Map[String,Any] => Boolean): Unit = {
    val innerJson = json("value").asInstanceOf[mutable.Map[String,Any]]
    assert(metadata(innerJson("metadata").asInstanceOf[mutable.Map[String,Any]]))
    assert(nullable(innerJson("nullable").asInstanceOf[mutable.Map[String,Any]]))
    assert(dataType(innerJson("dataType").asInstanceOf[mutable.Map[String,Any]]))
    assert(name(innerJson("name").asInstanceOf[mutable.Map[String,Any]]))
  }

  override protected def configure(variablesView: VariablesViewImpl) =
    super.configure(variablesView)


  var spark: SparkSession = _

  override def beforeRepl(): Unit = {
    super.beforeRepl()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("Simple Application2").getOrCreate()
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
