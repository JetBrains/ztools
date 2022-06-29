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
import org.jetbrains.ztools.scala.{ReplAware, VariablesView}
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.mutable
import scala.tools.nsc.interpreter.IMain

class SparkHandlersTest extends ReplAware {
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
        val json = view.resolveVariables
        println(view.toJson)
        assertEquals("org.apache.spark.rdd.RDD", getInPath[String](json, "rdd.type"))
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
        val json = view.resolveVariables
        println(view.toJson)
    }

  @Test
  def testSparkContextHandler(): Unit = {

  }

  @Test
  def testDataFrame(): Unit = {
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
      val json = view.resolveVariables
      println(view.toJson)
      assertEquals("", view.errors.mkString(","))
      val schema = getInPath[mutable.Map[String, Any]](json, "bank.value.schema()")
      val schemaArray = schema("value").asInstanceOf[Array[Any]]
      checkStructField(schemaArray(0), true, "age", "integer")
      checkStructField(schemaArray(1), true, "job", "string")
      checkStructField(schemaArray(2), true, "marital", "string")
      checkStructField(schemaArray(3), true, "education", "string")
      checkStructField(schemaArray(4), true, "balance", "integer")
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
      assertEquals("{\"rdd\":{\"type\":\"org.apache.spark.rdd.RDD\",\"value\":\"NoSuchMethodException: org.apache.spark.io.LZ4CompressionCodec.<init>(org.apache.spark.SparkConf)\"}}", json)
  }

  private def checkStructField(field: Any,
                               nullable: Boolean,
                               name: String,
                               dataType: String): Unit = {
    val json = field.asInstanceOf[mutable.Map[String, Any]]
    assertEquals(nullable, json("nullable").asInstanceOf[mutable.Map[String, Any]]("value"))
    assertEquals(name, json("name").asInstanceOf[mutable.Map[String, Any]]("value"))
    assertEquals(dataType, json("dataType").asInstanceOf[mutable.Map[String, Any]]("value"))
  }

  override protected def configure(variablesView: VariablesView) =
    super.configure(variablesView)


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
