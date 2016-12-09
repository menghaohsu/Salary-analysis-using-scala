import data.LogitData
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class dataSpec extends FlatSpec with BeforeAndAfterAll with Matchers{

  var sc: SparkContext = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val conf = new SparkConf().setMaster("local[4]")
      .setAppName("test")
      .set("spark.driver.allowMultipleContexts","true")
    sc = new SparkContext(conf)
  }

  override protected def afterAll(): Unit = {
    try {
      sc.stop()
      sc = null
    } finally {
      super.afterAll()
    }
  }

  "Array(CAPTAIN, FIRE SUPPRESSION,140546.86,97868.77,31909.28,0.0,2011)" should
    "be converted to Array((CAPTAIN, FIRE SUPPRESSION),140546.86,97868.77,31909.28,0.0,2011)" in{
    val a=Array("CAPTAIN", "IRE SUPPRESSION","140546.86","97868.77","31909.28","0.0","2011")
    val b=LogitData.clean(a) shouldBe Array("CAPTAIN,IRE SUPPRESSION","140546.86","97868.77","31909.28","0.0","2011")
  }

  "Array((CAPTAIN, FIRE SUPPRESSION),,97868.77,31909.28,not provided,2011)" should
    "be converted to Array((CAPTAIN, FIRE SUPPRESSION),0,97868.77,31909.28,0,2011)" in{
    val a=Array("CAPTAIN,IRE SUPPRESSION","","97868.77","31909.28","not provided","2011")
    val b=LogitData.checkDouble(a) shouldBe Array("CAPTAIN,IRE SUPPRESSION","0","97868.77","31909.28","0","2011")
  }

  "Array(0,2000,5000,20000.30,500,2011)" should "be filtered out" in{
    val a=sc.parallelize(Array("0,2000,5000,20000.30,500,2011","1,5000,5000,40000.300,5000,2011"))
    val b=a
      .map(line=>{ line.split(",").map(c=>c.toDouble) })
      .filter(LogitData.pt)
      .count() shouldBe 1
  }

}
