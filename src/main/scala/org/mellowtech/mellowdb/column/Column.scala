package org.mellowtech.mellowdb.column

import java.io.{File, FileWriter}

import org.json4s.DefaultFormats
import org.mellowtech.core.codec.BCodec
import org.mellowtech.mellowdb.DbType
import org.mellowtech.mellowdb.DbType._
import org.mellowtech.mellowdb.search.SearchType._
import org.mellowtech.mellowdb.util.DbSerializers.{AtomicLongSerializer, ColumnTypeSerializer, DbTypeSerializer, SearchTypeSerializer}
import org.mellowtech.mellowdb.util.{BCodecs, DbSerializers}

import scala.collection.immutable.TreeMap
import scala.io.Source
import scala.util.{Success, Try}

case class ColumnHeader(
                         name: String,
                         table: String,
                         keyType: DbType = STRING,
                         valueType: DbType = STRING,
                         sorted: Boolean = true,
                         maxValueSize: Option[Int] = Some(256),
                         maxKeySize: Option[Int] = Some(64),
                         index: Integer = -1,
                         cacheSize: Integer = -1,
                         search: SearchType = NONE,
                         nullable: Boolean = true)

/**
  * @author msvens
  * @since 2017-02-04
  */
trait Column[A,B] {

  def +=(kv:(A,B)): Column.this.type
  def -=(key: A): Column.this.type
  def apply(key: A): B = get(key).getOrElse(throw new NoSuchElementException)
  def close: Try[Unit]
  def contains(key: A): Boolean
  def get(key: A): Option[B]
  def flush: Try[Unit]
  def header: ColumnHeader
  def iterator: Iterator[(A,B)]
  def size: Long
}

class MemoryColumn[A, B](private var m: Map[A,B], val header: ColumnHeader)(implicit val ord: Ordering[A]) extends Column[A,B] {

  override def +=(kv: (A, B)): MemoryColumn.this.type = {
    m += kv
    this
  }

  override def -=(key: A): MemoryColumn.this.type = {
    m -= key
    this
  }

  override def close: Try[Unit] = Success()

  override def contains(key: A): Boolean = m contains key

  override def get(key: A): Option[B] = m get key

  override def flush: Try[Unit] = Success()

  override def iterator: Iterator[(A, B)] = m.iterator

  override def size: Long = m.size

}


object Column {

  import org.json4s._
  import org.json4s.native.Serialization.{read, write}

  implicit val formats = DefaultFormats + DbTypeSerializer + ColumnTypeSerializer + SearchTypeSerializer + AtomicLongSerializer

  val columnHeaderFile: String = "column.head"

  def calcSize(header: ColumnHeader): ColumnHeader = {
    def size(b: BCodec[_], ms: Option[Int]): Int = b.isFixed match {
      case true => b.fixedSize()
      case false => ms match {
        case Some(s) => s
        case None => Int.MaxValue
      }
    }
    val keyCodec: BCodec[_] = BCodecs.getCodec(DbType.asScalaClass(header.keyType))
    val valueCodec: BCodec[_] = BCodecs.getCodec(DbType.asScalaClass(header.keyType))
    val kSize = size(keyCodec, header.maxKeySize)
    val vSize = size(valueCodec, header.maxValueSize)
    header.copy(maxKeySize = Some(kSize), maxValueSize = Some(vSize))
  }

  def readColumnHeader(dir: File): ColumnHeader = {
    val jsonString = Source.fromFile(new File(dir, columnHeaderFile)).mkString

    read[ColumnHeader](jsonString)
  }

  def writeColumnHeader(ch: ColumnHeader, dir: String): String = {
    val d = new File(dir+"/"+ch.name)
    if(!d.exists) d.mkdir
    val w = new FileWriter(new File(d, columnHeaderFile))
    w.write(write(ch))
    w.close()
    d.getAbsolutePath
  }

  def apply[A,B](ch: ColumnHeader, path: String)(implicit ord: Ordering[A]): Column[A,B] = {
    return new DiscColumn[A,B](ch,path)
  }

  def apply[A,B](ch: ColumnHeader)(implicit ord: Ordering[A]): Column[A,B] = ch.sorted match {
    case true => apply[A,B](ch, TreeMap.empty)
  }

  def apply[A,B](ch: ColumnHeader, kv: Map[A,B])(implicit ord: Ordering[A]): Column[A,B] = new MemoryColumn[A,B](kv, ch)

}