import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

try {
  import org.jetbrains.ztools.scala.VariablesView

  import scala.tools.nsc.interpreter.IMain


  /**
   * Main section
   */
  val iMain: IMain = $intp
  val depth: Int = 0
  val filterUnitResults: Boolean = true
  val enableProfiling: Boolean = true
  val collectionSizeLimit = 100
  val stringSizeLimit = 400
  val timeout = 5000
  val variableTimeout = 2000
  val interpreterResCountLimit = 5
  val blackList = "$intp,z,engine".split(',').toList
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
  implicit val ztoolsFormats: AnyRef with Formats = Serialization.formats(NoTypeHints)
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
    println(result)
    println("--ztools-scala--")
}



