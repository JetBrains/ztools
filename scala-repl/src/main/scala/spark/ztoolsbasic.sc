try {
  import org.jetbrains.ztools.scala.VariablesView

  import scala.tools.nsc.interpreter.IMain


  /**
   * Main section
   */
  val iMain: IMain = null
  val depth: Int = 0
  val filterUnitResults: Boolean = true
  val enableProfiling: Boolean = false
  val collectionSizeLimit = 100
  val stringSizeLimit = 400
  val timeout = 5000
  val variableTimeout = 2000
  val interpreterResCountLimit = 5
  val blackList = "$intp,sc,spark,sqlContext,z,engine".split(',').toList
  val whiteList: List[String] = null


  val variableView = new VariablesView(
    intp = iMain,
    timeout = timeout,
    variableTimeout = variableTimeout,
    collectionSizeLimit = collectionSizeLimit,
    stringSizeLimit = stringSizeLimit,
    blackList = blackList,
    whiteList = whiteList,
    filterUnitResults = filterUnitResults,
    enableProfiling = enableProfiling,
    depth = depth,
    interpreterResCountLimit = interpreterResCountLimit
  )

  val variablesJson = variableView.getZtoolsJsonResult
  println("--ztools-scala--")
  println(variablesJson)
  println("--ztools-scala--")
}
catch {
  case t: Throwable =>
    import org.apache.commons.lang.exception.ExceptionUtils
    import org.json4s.jackson.Serialization
    import org.json4s.{Formats, NoTypeHints}

    implicit val ztoolsFormats: AnyRef with Formats = Serialization.formats(NoTypeHints)
    val result = Serialization.write(Map(
      "errors" -> Array(f"${ExceptionUtils.getMessage(t)}\n${ExceptionUtils.getStackTrace(t)}")
    ))
    println("--ztools-scala--")
    println(Serialization.write(result))
    println("--ztools-scala--")
}



