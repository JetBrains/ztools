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
package org.jetbrains.ztools.spark

import org.apache.spark.sql.SparkSession
import org.jetbrains.bigdataide.shaded.org.json.{JSONArray, JSONObject}
import org.junit.{AfterClass, BeforeClass, Rule, Test}

class SparkCatalogMetadataProviderTest {

  @Test
  def simpleTest(): Unit = {
    withSpark { spark =>
      import spark.implicits._
      val sc = spark.sparkContext
      val df = sc.parallelize(List((1, "hello"), (2, "world"))).toDF("id", "text")
      try {
        df.createOrReplaceTempView("test_table")
        val provider = new SparkCatalogMetadataProvider(spark)
        val databasesJson = provider.fromCatalog.getJSONArray("databases")
        assert(databasesJson.length() == 1)
        val resJson = databasesJson.getJSONObject(0)
        assert(resJson.getString("name") == "default")
        assert(resJson.getString("description") == "Default Hive database")
        val tables = resJson.getJSONArray("tables")
        assert(tables.length() == 1)
        val table = tables.getJSONObject(0)
        assert(table.getString("tableType") == "TEMPORARY")
        val columns = table.getJSONArray("columns")
        assert(columns.length() == 2)
        assertColumn(columns.getJSONObject(0), "int", "id", nullable = false)
        assertColumn(columns.getJSONObject(1), "string", "text", nullable = true)
        assert(table.getString("name") == "test_table")
        assert(table.getBoolean("isTemporary"))
      } finally {
        spark.catalog.dropTempView("test_table")
      }
    }
  }

  @Test
  def fromSparkContextTest(): Unit = {
    withSpark { spark =>
      import spark.implicits._
      val sc = spark.sparkContext
      val df = sc.parallelize(List((1, "hello"), (2, "world"))).toDF("id", "text")
      try {
        df.write.saveAsTable("test_table_2")
        val provider = new SparkCatalogMetadataProvider(spark)
        val databasesJson = provider.fromSpark.getJSONArray("databases")
        assert(databasesJson.length() == 1)
        val resJson = databasesJson.getJSONObject(0)
        assert(resJson.getString("name") == "default")
        val tables = resJson.getJSONArray("tables")
        assert(tables.length() == 1)
        val table = tables.getJSONObject(0)
        val columns = table.getJSONArray("columns")
        assert(columns.length() == 2)
        assertColumn(columns.getJSONObject(0), "int", "id", full = false)
        assertColumn(columns.getJSONObject(1), "string", "text", full = false)
        assert(table.getString("name") == "test_table_2")
        assert(!table.getBoolean("isTemporary"))
      } finally {
        spark.sql("drop table test_table_2")
      }
    }
  }

  def withSpark[T](body: SparkSession => T): T = {
    val result = body.apply(SparkCatalogMetadataProviderTest.spark)
    result
  }

  protected def assertColumn(column: JSONObject, dataType: String, name: String, full: Boolean = true, nullable: Boolean = true): Unit = {
    if (full) assert(column.getBoolean("nullable") == nullable)
    assert(column.getString("dataType") == dataType)
    assert(column.getString("name") == name)
  }
}

object SparkCatalogMetadataProviderTest {
  @BeforeClass
  def init(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[2]")
      .enableHiveSupport()
      .appName("Simple Application").getOrCreate()
  }

  @AfterClass
  def cleanUp(): Unit = {
    spark.close()
  }

  var spark: SparkSession = _
}
