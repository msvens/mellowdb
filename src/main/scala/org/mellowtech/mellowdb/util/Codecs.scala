package org.mellowtech.mellowdb.util

import java.io.OutputStream
import java.util.{Date, UUID => JUUID}

import org.mellowtech.core.codec._
import org.mellowtech.core.collections.{DiscMap, DiscMapBuilder}
import org.mellowtech.mellowdb.DbType


/**
  * @author Martin Svensson
  * @since 0.1
  */
object BCodecs {

  import java.nio.ByteBuffer
  import org.mellowtech.mellowdb.DbType._
  import scala.reflect.{ClassTag, classTag}

  object SByteCodec extends BCodec[Byte] {
    override val isFixed = true
    override val fixedSize = 1
    override def byteCompare(offset1: Int, bb1: ByteBuffer, offset2: Int, bb2: ByteBuffer): Int = bb1.get(offset1) - bb2.get(offset2)
    override def byteSize(a: Byte): Int = 1
    override def byteSize(bb: ByteBuffer): Int = 1
    override def from(bb: ByteBuffer): Byte = bb.get()
    override def to(a: Byte, bb: ByteBuffer): Unit = bb.put(a)
  }

  object SBooleanCodec extends BCodec[Boolean] {
    override val isFixed = true
    override val fixedSize = 1
    override def byteCompare(offset1: Int, bb1: ByteBuffer, offset2: Int, bb2: ByteBuffer): Int = bb1.get(offset1) - bb2.get(offset2)
    override def byteSize(a: Boolean): Int = 1
    override def byteSize(bb: ByteBuffer): Int = 1
    override def from(bb: ByteBuffer): Boolean = bb.get() match {
      case 0 => false
      case _ => true
    }
    override def to(a: Boolean, bb: ByteBuffer): Unit = a match {
      case false => bb.put(0.toByte)
      case true => bb.put(1.toByte)
    }
  }

  object SCharCodec  extends BCodec[Char] {
    override val isFixed = true
    override val fixedSize = 2
    override def byteCompare(offset1: Int, bb1: ByteBuffer, offset2: Int, bb2: ByteBuffer): Int =
      Character.compare(bb1.getChar(offset1),bb2.getChar(offset2))
    override def byteSize(a: Char): Int = 2
    override def byteSize(bb: ByteBuffer): Int = 2
    override def from(bb: ByteBuffer): Char = bb.getChar()
    override def to(a: Char, bb: ByteBuffer): Unit = bb.putChar(a)
  }

  object SShortCodec extends BCodec[Short] {
    override val isFixed = true
    override val fixedSize = 2
    override def byteSize(a: Short): Int = 2
    override def byteSize(bb: ByteBuffer): Int = 2
    override def from(bb: ByteBuffer): Short = bb.getShort()
    override def to(a: Short, bb: ByteBuffer): Unit = bb.putShort(a)
    override def byteCompare(offset1: Int, bb1: ByteBuffer, offset2: Int, bb2: ByteBuffer): Int = bb1.getShort(offset1) - bb2.getShort(offset2)
  }

  object SIntCodec extends BCodec[Int] {
    override val isFixed = true
    override val fixedSize = 4
    override def from(bb: ByteBuffer): Int = bb.getInt()
    override def to(a: Int, bb: ByteBuffer): Unit = bb.putInt(a)
    override def byteSize(a: Int): Int = 4
    override def byteSize(bb: ByteBuffer): Int = 4
    override def byteCompare(offset1: Int, bb1: ByteBuffer, offset2: Int, bb2: ByteBuffer): Int =
      Integer.compare(bb1.getInt(offset1),bb2.getInt(offset2))
  }

  object SLongCodec extends BCodec[Long] {
    override val isFixed = true
    override val fixedSize = 8
    override def from(bb: ByteBuffer): Long = bb.getLong()
    override def to(a: Long, bb: ByteBuffer): Unit = bb.putLong(a)
    override def byteSize(a: Long): Int = 8
    override def byteSize(bb: ByteBuffer): Int = 8
    override def byteCompare(offset1: Int, bb1: ByteBuffer, offset2: Int, bb2: ByteBuffer): Int =
      java.lang.Long.compare(bb1.getLong(offset1),bb2.getLong(offset2))
  }

  object SFloatCodec extends BCodec[Float] {
    override val isFixed = true
    override val fixedSize = 4
    override def from(bb: ByteBuffer): Float = bb.getFloat
    override def to(a: Float, bb: ByteBuffer): Unit = bb.putFloat(a)
    override def byteSize(a: Float): Int = 4
    override def byteSize(bb: ByteBuffer): Int = 4
    override def byteCompare(offset1: Int, bb1: ByteBuffer, offset2: Int, bb2: ByteBuffer): Int =
      java.lang.Float.compare(bb1.getFloat(offset1),bb2.getFloat(offset2))
  }

  object SDoubleCodec extends BCodec[Double] {
    override val isFixed = true
    override val fixedSize = 8
    override def from(bb: ByteBuffer): Double = bb.getDouble()
    override def to(a: Double, bb: ByteBuffer): Unit = bb.putDouble(a)
    override def byteSize(a: Double): Int = 8
    override def byteSize(bb: ByteBuffer): Int = 8
    override def byteCompare(offset1: Int, bb1: ByteBuffer, offset2: Int, bb2: ByteBuffer): Int =
      java.lang.Double.compare(bb1.getDouble(offset1),bb2.getDouble(offset2))
  }

  val stringCodec = new StringCodec
  val byteArrayCodec = new ByteArrayCodec
  val dateCodec = new DateCodec
  val uuidCodec = new UUIDCodec

  val BooleanClass = classOf[Boolean]
  val ByteClass = classOf[Byte]
  val ShortClass = classOf[Short]
  val IntClass = classOf[Int]
  val LongClass = classOf[Long]
  val FloatClass = classOf[Float]
  val DoubleClass = classOf[Double]
  val CharClass = classOf[Char]
  val StringClass = classOf[String]
  val ByteArrayClass = classOf[Array[Byte]]
  val DateClass = classOf[Date]
  val UUIDClass = classOf[JUUID]



  //def getCodec()
  def getCodec[A: ClassTag]: BCodec[A] =
    getCodec(classTag[A].runtimeClass.asInstanceOf[Class[A]])

  def getCodec[A](a: A): BCodec[A] = getCodec(a.getClass.asInstanceOf[Class[A]])

  def getCodec[A](t: Class[A]): BCodec[A] = (t match {
    case BooleanClass => SBooleanCodec
    case ByteClass => SByteCodec
    case ShortClass => SShortCodec
    case IntClass => SIntCodec
    case LongClass => SLongCodec
    case FloatClass => SFloatCodec
    case DoubleClass => SDoubleCodec
    case StringClass => stringCodec
    case ByteArrayClass => byteArrayCodec
    case DateClass => dateCodec
    case UUIDClass => uuidCodec
    case _ => Codecs.fromClass(t)
  }).asInstanceOf[BCodec[A]]

  /*
  def toBytes[A](a: A, out: OutputStream): Unit = {

    out.write()
    getCodec(a).to(a,out)
    out.write(bs._2.id)
    bs._1.to(out)
  }
  */

  /*
  def toBStorable[A](a: A): (BComparable[_,_],DbType) = a match {
    case b: Boolean => (new SBoolean(b), DbType.BOOLEAN)
    case b: Byte => (new SByte(b), DbType.BYTE)
    case s: Short => (new SShort(s), DbType.SHORT)
    case i: Int => (new SInt(i), DbType.INT)
    case l: Long => (new SLong(l), DbType.LONG)
    case f: Float => (new SFloat(f), DbType.FLOAT)
    case d: Double => (new SDouble(d), DbType.DOUBLE)
    case c: Char => (new SChar(c), DbType.CHAR)
    case s: String => (new CBString(s), DbType.STRING)
    case ba: Array[Byte] => (new CBByteArray(ba), DbType.BYTES)
    case d: Date => (new CBDate(d), DbType.DATE)
    case u: JUUID => (new CBUUID(u), DbType.UUID)
  }
  */

  def fromBytes(bb: ByteBuffer): Any = fromBytes(DbType(bb.get()),bb)

  def fromBytes(dbType: DbType, bb: ByteBuffer): Any = dbType match {
    case BOOLEAN => SByteCodec.from(bb)
    case BYTE => SByteCodec.from(bb)
    case SHORT => SShortCodec.from(bb)
    case INT => SIntCodec.from(bb)
    case LONG => SLongCodec.from(bb)
    case FLOAT => SFloatCodec.from(bb)
    case DOUBLE => SDoubleCodec.from(bb)
    case CHAR => SCharCodec.from(bb)
    case STRING => stringCodec.from(bb)
    case BYTES => byteArrayCodec.from(bb)
    case DATE => dateCodec.from(bb)
    case UUID => uuidCodec.from(bb)


  }



}

class MapBuilder extends DiscMapBuilder{

  import org.mellowtech.mellowdb.DbType
  import org.mellowtech.mellowdb.DbType.DbType
  import scala.reflect.{ClassTag, classTag}


  private def toClass[T: ClassTag]: Class[T] = classTag[T].runtimeClass.asInstanceOf[Class[T]]

  private def toClass[T](dbType: DbType): Class[T] = DbType.asScalaClass(dbType).asInstanceOf[Class[T]]

  def build[A: ClassTag, B: ClassTag](fileName: String, sorted: Boolean): DiscMap[A, B] = {
    build[A, B](toClass[A], toClass[B], fileName, sorted)
  }

  def build[A, B](keyType: DbType, valueType: DbType, fileName: String, sorted: Boolean): DiscMap[A, B] = {
    build[A, B](toClass[A](keyType), toClass[B](valueType), fileName, sorted)
  }

  override def build[A, B](keyClass: Class[A], valueClass: Class[B],
                           fileName: String, sorted: Boolean): DiscMap[A, B] = {

    create[A,B](BCodecs.getCodec(keyClass),BCodecs.getCodec(valueClass),fileName,sorted)


  }
  
  
}
