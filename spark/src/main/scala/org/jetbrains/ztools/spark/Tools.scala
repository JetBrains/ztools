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
import org.jetbrains.ztools.scala.{VariablesView, VariablesViewImpl, ZtoolsInterpreterWrapper}
import org.jetbrains.ztools.spark.handlers.{DatasetHandler, RDDHandler, SparkContextHandler, SparkSessionHandler}

import scala.tools.nsc.interpreter.IMain

//noinspection ScalaUnusedSymbol
object Tools {
  private var variablesView: VariablesView = _
  private var iMain: IMain = _
  private var spark: SparkSession = _

  //noinspection ScalaUnusedSymbol
  def init(iMain: IMain,
           depth: Int = 0,
           filterUnitResults: Boolean = true,
           enableProfiling: Boolean = false): Unit = {
    if (variablesView != null)
      return

    this.iMain = iMain

    val iMainWrapper = new ZtoolsInterpreterWrapper(iMain)

    variablesView = createVariablesView(iMainWrapper.valueOfTerm,
      depth = depth,
      filterUnitResults = filterUnitResults,
      enableProfiling = enableProfiling)

    variablesView.registerTypeHandler(new DatasetHandler())
      .registerTypeHandler(new RDDHandler())
      .registerTypeHandler(new SparkSessionHandler())
      .registerTypeHandler(new SparkContextHandler())

    spark = getSparkSession(iMainWrapper)
  }

  def getEnv: VariablesView = variablesView

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

  private def getSparkSession(wrapper: ZtoolsInterpreterWrapper): SparkSession = {
    wrapper.valueOfTerm("spark") match {
      case Some(spark) => spark.asInstanceOf[SparkSession]
      case _ => throw new RuntimeException("Can't get spark session")
    }
  }
}
