package org.mellowtech.mellowdb.search

/**
  * @author msvens
  * @since 2017-02-04
  */
object SearchType extends Enumeration {
  type SearchType = Value
  val FIELD, TEXT, NONE = Value
}
