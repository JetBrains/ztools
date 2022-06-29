package org.jetbrains.ztools.scala.handlers

import org.jetbrains.ztools.scala.core.Loopback
import org.jetbrains.ztools.scala.handlers.impls._
import org.jetbrains.ztools.scala.interpreter.ScalaVariableInfo
import org.jetbrains.ztools.scala.reference.ReferenceManager
import spark.handlers.{DatasetHandler, RDDHandler, SparkContextHandler, SparkSessionHandler}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class HandlerManager(enableProfiling: Boolean,
                     timeout: Int,
                     stringSizeLimit: Int,
                     collectionSizeLimit: Int,
                     referenceManager: ReferenceManager) {
  private val handlerChain = ListBuffer[AbstractTypeHandler](
    new NullHandler(),
    new StringHandler(stringSizeLimit),
    new ArrayHandler(collectionSizeLimit, timeout),
    new JavaCollectionHandler(collectionSizeLimit, timeout),
    new SeqHandler(collectionSizeLimit, timeout),
    new SetHandler(collectionSizeLimit, timeout),
    new MapHandler(collectionSizeLimit, timeout),
    new ThrowableHandler(),
    new SpecialsHandler(stringSizeLimit),
    new PrimitiveHandler(),
    new DatasetHandler(),
    new RDDHandler(),
    new SparkContextHandler(),
    new SparkSessionHandler(),
    new ObjectHandler(stringSizeLimit, this, referenceManager, timeout)
  ).map(new HandlerWrapper(_, enableProfiling))

  def getErrors: mutable.Seq[String] = handlerChain.flatMap(x => x.handler.getErrors)

  def handleVariable(info: ScalaVariableInfo, loopback: Loopback, depth: Int, startTime: Long = System.currentTimeMillis()): Any = {
    handlerChain.find(_.accept(info)).map(_.handle(info, loopback, depth, startTime)).getOrElse(mutable.Map[String, Any]())
  }
}
