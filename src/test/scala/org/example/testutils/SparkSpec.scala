package org.example.testutils

import org.apache.spark._
import org.apache.spark.sql.Row
import org.scalatest._

trait SparkSpec extends BeforeAndAfterEach {
  this: Suite =>

  private val master = "local[2]"
  private val appName = this.getClass.getSimpleName

  private var _sc: SparkContext = _

  def sc = _sc

  val conf: SparkConf = new SparkConf()
    .setMaster(master)
    .setAppName(appName)
    .set("spark.ui.enabled", "false")

  override def beforeEach(): Unit = {
    super.beforeEach()

    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on Shutdown
    _sc = new SparkContext(conf)
  }

  override def afterEach(): Unit = {
    if (_sc != null) {
      _sc.stop()
      _sc = null
    }
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.master.port")

    super.afterEach()
  }

  implicit def toSeqOfTuples[R <: Row](as:Array[R]): Array[Product] = {
    val result = as.map((row: Row) => {
      val tupleClass = Class.forName("scala.Tuple" + row.size)
      val rowElems = row.toSeq.asInstanceOf[Seq[Object]]
      tupleClass.getConstructors.apply(0).newInstance(rowElems: _*).asInstanceOf[Product]
    })
    result
  }
}