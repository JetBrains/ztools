package org.jetbrains.ztools.scala.reference

class ReferenceWrapper(val ref: AnyRef) {
  override def hashCode(): Int = ref.hashCode()

  override def equals(obj: Any): Boolean = obj match {
    case value: ReferenceWrapper =>
      ref.eq(value.ref)
    case _ => false
  }
}