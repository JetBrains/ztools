/**
 * Copyright 2020 Jetbrains
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jetbrains.ztools.spark

import org.apache.spark.sql.SparkSession
import org.jetbrains.ztools.scala.{IMainWrapper, VariablesView, VariablesViewImpl}

import scala.tools.nsc.interpreter.IMain

object Tools {
  private var env: VariablesView = _
  private var iMain: IMain = _
  private var spark: SparkSession = _

  def init(iMain: IMain, depth: Int = 0,
           filterUnitResults: Boolean = true,
           enableProfiling: Boolean = false): Unit = {
    if (env != null) return

    this.iMain = iMain

    val iMainWrapper = new IMainWrapper(iMain)

    env = createVariablesView(iMainWrapper.valueOfTerm,
      depth = depth,
      filterUnitResults = filterUnitResults,
      enableProfiling = enableProfiling)

    env.registerTypeHandler(new DatasetHandler())
      .registerTypeHandler(new RDDHandler())
      .registerTypeHandler(new SparkSessionHandler())
      .registerTypeHandler(new SparkContextHandler())

    spark = getSparkSession(iMainWrapper)
  }

  def getEnv: VariablesView = env

  private def createVariablesView(valueOfTermFunc: String => Option[Any],
                                  depth: Int = 0,
                                  filterUnitResults: Boolean = true,
                                  enableProfiling: Boolean = false): VariablesView =
    new VariablesViewImpl(
      collectionSizeLimit = 100,
      stringSizeLimit = 400,
      blackList = "$intp,sc,spark,sqlContext,z,engine".split(',').toList,
      filterUnitResults = filterUnitResults,
      enableProfiling = enableProfiling,
      depth = depth) {

      override def variables(): List[String] =
        iMain.definedSymbolList.filter { x => x.isGetter }.map(_.name.toString).distinct

      override def valueOfTerm(id: String): Option[Any] = valueOfTermFunc(id)

      override def typeOfExpression(id: String): String = iMain.typeOfExpression(id, silent = true).toString()
    }

  def getCatalogProvider(catalogType: String = "spark"): CatalogMetadataProvider = catalogType match {
    case "spark" => new SparkCatalogMetadataProvider(spark.catalog)
    case "external" => new ExternalCatalogMetadataProvider(spark.sharedState.externalCatalog)
    case _ => throw new IllegalArgumentException("catalogType must be \"spark\" or \"external\"")
  }

  private def getSparkSession(wrapper: IMainWrapper): SparkSession = {
    wrapper.valueOfTerm("spark") match {
      case Some(spark) => spark.asInstanceOf[SparkSession]
      case _ => throw new RuntimeException("Can't get spark session")
    }
  }
}
