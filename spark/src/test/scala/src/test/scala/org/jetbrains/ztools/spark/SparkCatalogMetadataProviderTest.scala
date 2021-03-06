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
import org.junit.Test

class SparkCatalogMetadataProviderTest {
  @Test
  def simpleTest(): Unit = {
    withSpark { spark =>
      import spark.implicits._
      val sc = spark.sparkContext
      val df = sc.parallelize(List((1, "hello"), (2, "world"))).toDF("id", "text")
      df.createOrReplaceTempView("test_table")
      val provider = new SparkCatalogMetadataProvider(spark.catalog)
      println(provider.toJson.toString(2))
    }
  }

  def withSpark[T](body: SparkSession => T): T = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("Simple Application").getOrCreate()
    val result = body.apply(spark)
    spark.close()
    result
  }
}
