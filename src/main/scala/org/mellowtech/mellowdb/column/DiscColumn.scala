package org.mellowtech.mellowdb.column
import org.mellowtech.core.collections.DiscMapBuilder
import org.mellowtech.mellowdb.util.MapBuilder

import scala.util.Try

/**
  * @author msvens
  * @since 2017-02-05
  */
class DiscColumn[A,B](override val header: ColumnHeader, path: String) extends Column[A,B]{

  import scala.collection.JavaConverters._

  val valueBlockSize = DiscMapBuilder.DEFAULT_VALUE_BLOCK_SIZE
  lazy val isBlob: Boolean = Column.calcSize(header).maxValueSize.get > valueBlockSize / 10
  val builder = new MapBuilder()
  builder.blobValues(isBlob)
  val dbmap = builder.build[A,B](header.keyType, header.valueType, path, header.sorted)

  override def +=(kv: (A, B)): DiscColumn.this.type = {
    dbmap.put(kv._1, kv._2)
    this
  }

  override def -=(key: A): DiscColumn.this.type = {
    dbmap.remove(key)
    this
  }

  override def close: Try[Unit] = Try(dbmap.close())

  override def contains(key: A): Boolean = dbmap.containsKey(key)

  override def get(key: A): Option[B] = Option(dbmap.get(key))

  override def flush: Try[Unit] = Try(dbmap.save())

  override def iterator: Iterator[(A, B)] = for {
    e <- dbmap.iterator().asScala
  } yield((e.getKey, e.getValue))

  override def size: Long = dbmap.size()
}
