package org.mellowtech.mellowdb.row

import java.io.ByteArrayOutputStream

import org.mellowtech.core.codec.BCodec
import org.mellowtech.mellowdb.DbType

import scala.collection.generic.Sorted


/**
  * @author Martin Svensson
  * @since 0.1
  */
trait Row {

  def apply[A](column: String): A = get(column).get

  def contains[A](column: String): Boolean = get(column).isDefined

  def get[A](column: String): Option[A]

  def isEmpty: Boolean = list == Nil

  def list: List[(String,Any)] = iterator.toList

  def iterator: Iterator[(String,Any)]

  def size: Int = list.size

  override def toString: String = iterator.foldLeft("")((s,cv) => s + cv._1 + "::" +cv._2+" ")

  override def equals(other: Any): Boolean = other match  {
    case that: Row =>
      def m(kv: (String,Any)): Boolean = {
        that.get[Any](kv._1) match {
          case None => false
          case Some(v) => kv._2 == v
        }
      }
      that.size == this.size && iterator.forall(m)
    case _ => false
  }

  def +[A](value: (String,A)): Row

  def ++(values: Map[String,Any]): Row

  def -(column: String): Row

}

class SparseRow(m: Map[String,Any]) extends Row{

  override def isEmpty = m.isEmpty

  override def iterator = m.iterator

  override def get[A](column: String) = m.get(column) match {
    case Some(v) => v match {
      case a: A => Some(a)
      case _ => throw new ClassCastException
    }
    case _ => None
  }

  override def +[A](value: (String,A)) = new SparseRow(m + value)

  override def ++(values: Map[String,Any]) = new SparseRow(m ++ values)

  override def -(column: String) = new SparseRow(m - column)


}

object Row{
  import java.nio.ByteBuffer

  import org.mellowtech.mellowdb.table.TableHeader
  import org.mellowtech.mellowdb.util.BCodecs

  import scala.collection.immutable.TreeMap

  val strCodec: BCodec[String] = BCodecs.getCodec[String]
  val intCodec: BCodec[Int] = BCodecs.getCodec[Int]

  def apply(): Row = new SparseRow(Map[String,Any]())

  def apply(map: Map[String,Any]): Row = new SparseRow(map)

  def apply(header: TableHeader): Row = header.sorted match {
    case true => new SparseRow(TreeMap[String,Any]())
    case false => new SparseRow(Map[String,Any]())
  }

  //def apply[A](th: TableHeader, b: Array[Byte], t: IndexTable[A])(implicit ord: Ordering[A]): Row = ???

  def apply(b: Array[Byte], sorted: Boolean = false): Row = {
    val bb = ByteBuffer.wrap(b)
    val size = intCodec.from(bb)
    val cv = for {
      i <- 0 until size
    } yield (strCodec.from(bb),BCodecs.fromBytes(bb))
    val m = sorted match {
      case true => TreeMap[String,Any]() ++ cv
      case false => Map[String,Any]() ++ cv
    }
    apply(m)
  }

  def toBytes[A](row: Row): Array[Byte] = {
    val bos = new ByteArrayOutputStream
    intCodec.to(row.size, bos)
    row.iterator.foreach { case (c, v) => {
      strCodec.to(c, bos)
      bos.write(DbType.byteValue(DbType.asDbType(v)))
      BCodecs.getCodec(v).to(v,bos)
    }
    }
      bos.toByteArray
    }


  }

