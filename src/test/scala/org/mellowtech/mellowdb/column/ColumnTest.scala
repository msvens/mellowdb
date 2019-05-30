package org.mellowtech.mellowdb.column

import java.io.File
import org.scalatest._

object Files {
  import org.mellowtech.core.util.Platform
  import org.mellowtech.core.util.DelDir

  def temp(dir: String): String = new File(Platform.getTempDir+"/"+dir).getAbsolutePath

  def fileTemp(dir: Option[String], f: String) = {
    val tdir = dir match {
      case Some(d) => temp(d)
      case None => temp("")
    }
    file(Some(tdir), f)
  }

  def file(dir: Option[String], f: String): File = dir match{
    case Some(d) => new File(d + "/" + f)
    case None => new File(f)
  }

  def file(f: String): File = file(null, f)

  def del(dir: String):Boolean = DelDir.d(dir)
  def delTemp(dir: String):Boolean=DelDir.d(temp(dir))


  def create(dir: String): Boolean = {
    new File(dir).mkdir()
  }
  def createTemp(dir: String): Boolean = create(temp(dir))

  def resource(r: String): File = {
    val url = Files.getClass.getResource("/"+r)
    new File(url.getFile)
  }

  def resource(pkg: String, r: String): File = {
    if(pkg == null){
      resource(r)
    }
    else{
      pkg.replace(".", "/") match {
        case x if x.startsWith("/") => resource(x+"/"+r)
        case x => resource("/"+x+"/"+r)
      }
    }
  }
}
/**
  * @author msvens
  * @since 2017-02-07
  */
trait ColumnTest extends TestSuiteMixin { this: TestSuite =>


  import scala.util.Random
  import scala.collection.mutable.ArrayBuffer

  val ch = ColumnHeader("col1", "table1")
  val dir = "columntest"
  val f = "discBasedMap";
  val cols: ArrayBuffer[Column[String,String]] = ArrayBuffer()

  abstract override def withFixture(test: NoArgTest) = {
    cols += Column[String,String](ch)
    val tdir = Files.temp(dir+""+Random.alphanumeric.take(10).mkString(""))
    Files.create(tdir)
    cols += Column[String,String](ch,tdir+"/"+f)
    try super.withFixture(test) // To be stackable, must call super.withFixture
    finally {
      cols.clear
      Files.del(tdir)
    }
  }

}

class ColumnSpec extends FlatSpec with ColumnTest {

  behavior of "an empty column"

  it should "have size zero" in {
    cols.foreach(f => assert(f.size === 0))
  }

  it should "return none when getting a value" in {
    cols.foreach(f => assert(f.get("someKey") === None))
  }

  it should "throw an exception when calling apply" in {
    cols.foreach(f => intercept[NoSuchElementException] (f("someKey")))
  }



}


