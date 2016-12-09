import ml.Logistic
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class logisticSpec extends FlatSpec with BeforeAndAfterAll with Matchers{

  "size" should "be 1" in{
    val a=Array((1.0,1.0),(0.0,0.0),(1.0,0.0))
    val b=a.filter(Logistic.compareWithLabel).length shouldBe 1
  }

  "size" should "be 3" in{
    val a=Array((1.0,0.0),(0.0,1.0),(1.0,0.0))
    val b=a.filter(Logistic.compareWithLabel).length shouldBe 3
  }

}

