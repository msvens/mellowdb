package org.mellowtech.mellowdb
package table

import java.io.File
import java.util.concurrent.atomic.AtomicLong

import org.mellowtech.mellowdb.DbType._
import org.mellowtech.mellowdb.column.{Column, ColumnHeader}
import org.mellowtech.mellowdb.row.Row
import scala.util.Try


object TableType extends Enumeration {
  type TableType = Value
  val MEMORY_ROW, MEMORY_COLUMN, COLUMN, ROW = Value
}


case class TableHeader(name: String,
                       keyType: DbType = STRING,
                       primColumn: Option[String] = None,
                       maxKeySize: Option[Int] = None,
                       maxRowSize: Option[Int] = None,
                       val highId: AtomicLong = new AtomicLong(0),
                       sorted: Boolean = true,
                       logging: Boolean = false,
                       tableType: TableType.TableType = TableType.ROW,
                       indexed: Boolean = false,
                       genKey: Boolean = false){

  def incrHighId: Long = highId.incrementAndGet()

  def incrementAndGet[A]: A = {
    val toRet = incrHighId
    keyType match {
      case LONG => toRet.asInstanceOf[A]
      case INT => toRet.toInt.asInstanceOf[A]
      case _ => throw new Error("could not convert to keyType")
    }
  }
}


/**
  * @author Martin Svensson
  * @since 0.1
  */
trait Table[A] {

  def +=[B](key: A, value: B, column: String): Table.this.type
  def +=(key: A, row: Row): Table.this.type
  def -=(key: A, column: String): Table.this.type
  def -=(key: A): Table.this.type

  def columnHeader: Seq[ColumnHeader]
  def addColumn(ch: ColumnHeader): Table.this.type
  def close: Try[Unit]
  def contains(key: A): Boolean
  def flush: Try[Unit]
  def get(key: A): Option[Row]
  def column[B](column: String): Column[A,B]
  def header: TableHeader
  def iterator: Iterator[(A,Row)]
  def size: Long



}

object Table {



  def openColumnHeaders(dir: String): Seq[(String,ColumnHeader)] = for {
    f <- new File(dir).listFiles()
    if f.isDirectory && !f.getName.equals("tblidx")
    c = Column.readColumnHeader(f)
  } yield(c.name, c)

  def openColumns[A](headers: Seq[ColumnHeader], dir: String)(implicit ord: Ordering[A]): Seq[(String,Column[A,Any])] = for {
    ch <- headers
    d = new File(dir + "/" + ch.name + "/data")
  } yield (ch.name, Column[A,Any](ch, d.getAbsolutePath))

}
