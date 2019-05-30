package org.mellowtech.mellowdb.table
import org.mellowtech.mellowdb.DbType
import org.mellowtech.mellowdb.column.{Column, ColumnHeader}
import org.mellowtech.mellowdb.row.Row

import scala.util.Try

/**
  * @author msvens
  * @since 2017-02-03
  */
class RowTable[A:Ordering](override val header: TableHeader,
                           val path: String,
                           colHeaders: Seq[ColumnHeader]) extends Table[A]{

  var columnHeaders = Table.openColumnHeaders(path).toMap
  colHeaders foreach {addColumn(_)}

  //The maxValueSize should be changed to reflect the actual columnHeaders or just set to a blobvalue
  val dataColHeader = ColumnHeader(name = "datacol", maxValueSize = Some(4096), keyType = header.keyType,
    valueType = DbType.BYTES, table = header.name, sorted = true)

  val dataCol: Column[A, Array[Byte]] = Column(dataColHeader, path+"/datacol")

  override def close: Try[Unit] = dataCol.close

  override def flush: Try[Unit] = dataCol.flush

  override def +=[B](key: A, value: B, column: String): RowTable.this.type = ???

  override def +=(key: A, row: Row): RowTable.this.type = {
    val data = Row.toBytes(row)
    dataCol += (key,data)
    this
  }

  override def -=(key: A, column: String): RowTable.this.type =  get(key)  match {
    case Some(v) => {
      val v1 = v - column
      +=(key,v1)
    }
    case None => this
  }

  override def -=(key: A): RowTable.this.type = {
    dataCol -= key
    this
  }

  override def contains(key: A): Boolean = dataCol.contains(key)

  override def get(key: A): Option[Row] = dataCol.get(key) match {
    case Some(v) => Some(Row(v, header.sorted))
  }

  override def iterator: Iterator[(A, Row)] =
    for(kv <- dataCol.iterator) yield (kv._1,Row(kv._2, header.sorted))

  override def size: Long = dataCol.size

  override def addColumn(ch: ColumnHeader): RowTable.this.type = columnHeaders.get(ch.name) match {
    case Some(header) => this
    case None => {
      columnHeaders += ((ch.name, ch))
      Column.writeColumnHeader(ch, path)
      this
    }
  }

  override def columnHeader: Seq[ColumnHeader] = columnHeaders.values.toSeq

  override def column[B](column: String): Column[A, B] = columnHeaders contains column match {
    case true => {
      val i = for {
        r <- iterator
        v = r._2.get(column) if v.isDefined
      } yield(r._1,v.get)
      Column(columnHeaders(column), i.toMap)
    }
    case false => {
      throw new NoSuchElementException("column not defined")
    }
  }
}
