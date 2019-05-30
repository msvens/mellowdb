package org.mellowtech.mellowdb.util

import java.util.concurrent.atomic.AtomicLong

import org.json4s._
import org.json4s.ext.EnumNameSerializer
import org.json4s.{CustomSerializer, JField, JObject, JString}
import org.mellowtech.mellowdb.DbType
import org.mellowtech.mellowdb.search.SearchType
import org.mellowtech.mellowdb.table.TableType

/**
  * @author msvens
  * @since 2017-02-04
  */
object DbSerializers {


  val defaults = DefaultFormats :: DbTypeSerializer :: ColumnTypeSerializer :: SearchTypeSerializer :: AtomicLongSerializer :: Nil


  object DbTypeSerializer extends EnumNameSerializer(DbType)
  object ColumnTypeSerializer extends EnumNameSerializer(TableType)
  object SearchTypeSerializer extends EnumNameSerializer(SearchType)


  object AtomicLongSerializer extends CustomSerializer[AtomicLong](format => (
    {case JObject(JField("highId", JString(v)) :: Nil) => new AtomicLong(v.toLong)},
    {case al: AtomicLong => JString(al.toString)}
  ))

  /*object DbTypeSerializer extends CustomSerializer[DbType](format => (
    { case JObject(JField("$dbtype", JString(s)) :: Nil) => DbType.withName(s) },
    { case t: DbType => JObject(JField("$dbtype", JString(t.toString)) :: Nil) }))*/

}
