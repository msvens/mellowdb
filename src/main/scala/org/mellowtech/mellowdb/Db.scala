package org.mellowtech.mellowdb

import java.io.File
import java.util.Date
import java.util.concurrent.atomic.AtomicLong

import org.mellowtech.core.util.DelDir
import org.mellowtech.mellowdb.table.{Table, TableHeader}

import scala.util.{Success, Try}

/**
  * @author Martin Svensson
  * @since 0.1
  */
object DbType extends Enumeration {
  type DbType = Value

  val BYTE = Value(0x01)
  val BOOLEAN = Value(0x02)
  val SHORT = Value(0x03)
  val CHAR = Value(0x04)
  val INT = Value(0x05)
  val LONG = Value(0x06)
  val FLOAT = Value(0x07)
  val DOUBLE = Value(0x08)
  val STRING = Value(0x09)
  val DATE = Value(0x10)
  val BYTES = Value(0x11)
  val UUID = Value(0x12)

  def byteValue(dbType: DbType): Byte = dbType.id.toByte
  def fromByte(b: Byte): DbType = DbType(b)

  /*def asJavaClass(dbType: DbType): Class[_] = dbType match {
    case BYTE => classOf[java.lang.Byte]
    case BOOLEAN => classOf[java.lang.Boolean]
    case SHORT => classOf[java.lang.Short]
    case INT => classOf[java.lang.Integer]
    case LONG => classOf[java.lang.Long]
    case FLOAT => classOf[java.lang.Float]
    case DOUBLE => classOf[java.lang.Double]
    case STRING => classOf[String]
    case DATE => classOf[java.util.Date]
    case BYTES => classOf[Array[Byte]]
  }*/

  def asScalaClass(dbType: DbType): Class[_] = dbType match {
    case BYTE => classOf[Byte]
    case BOOLEAN => classOf[Boolean]
    case SHORT => classOf[Short]
    case INT => classOf[Int]
    case LONG => classOf[Long]
    case FLOAT => classOf[Float]
    case DOUBLE => classOf[Double]
    case STRING => classOf[String]
    case DATE => classOf[Date]
    case BYTES => classOf[Array[Byte]]
    case UUID => classOf[java.util.UUID]
  }

  def asDbType[A](a: A): DbType = a match {
    case x: Byte => DbType.BYTE
    case x: Boolean => DbType.BOOLEAN
    case x: Short => DbType.SHORT
    case x: Int => DbType.INT
    case x: Long => DbType.LONG
    case x: Float => DbType.FLOAT
    case x: Double => DbType.DOUBLE
    case x: String => DbType.STRING
    case x: Date => DbType.DATE
    case x: Array[Byte] => DbType.BYTES
    case x: java.util.UUID => DbType.UUID
  }
}


/**
  * @author msvens
  * @since 2017-01-16
  */
trait Db {

  def add[A](table: TableHeader): Option[Table[A]]
  def delete(tableName: String)
  def name: String
  def get[A](tableName: String): Option[Table[A]]
  def apply[A](tableName: String): Table[A] = get[A](tableName) match {
    case Some(t) => t
    case None => throw new NoSuchElementException("no table with that name")
  }
  def list: List[String]

  def flush: Try[Unit]
  def close: Try[Unit]

}

class FileDb(val name: String, val path: String) extends Db {

  val TableHeaderFile = "header.mth"

  var tables: Map[String,Table[_]] = Map()

  private def createDir(): Unit = {
    val f = new File(path)
    f.mkdirs
  }

  private def deleteDir(): Unit = DelDir.d(path)

  private def openDb(): Try[Unit] = {
    Success()
  }


  override def add[A](table: TableHeader): Option[Table[A]] = ???

  override def delete(tableName: String): Unit = ???

  override def get[A](tableName: String): Option[Table[A]] = ???

  override def list: List[String] = ???

  override def flush: Try[Unit] = ???

  override def close: Try[Unit] = ???
}

object Db {

  def openTable(th: TableHeader, p: String): Table[_] = ???/*th.keyType match{
    case DbType.INT => Table[Int](th, p)
    case DbType.STRING => STable[String](th, p)
    case DbType.BYTE => STable[Byte](th, p)
    case DbType.CHAR => STable[Char](th, p)
    case DbType.SHORT => STable[Short](th, p)
    case DbType.LONG => STable[Long](th, p)
    case DbType.FLOAT => STable[Float](th, p)
    case DbType.DOUBLE => STable[Double](th, p)
    case DbType.DATE => STable[Date](th, p)
    case DbType.BYTES => STable[Iterable[Byte]](th,p)
  }*/
}