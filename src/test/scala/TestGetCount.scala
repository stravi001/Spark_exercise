import org.scalatest.funsuite.AnyFunSuite
import Utils.textUtils

class TestGetCount extends AnyFunSuite {

  val classTextUtils = new textUtils

  test("If string contains substring once textUtils.getCount should return 1") {
    assert(classTextUtils.getCount("abc", "a") == 1)
  }

  test("If string does not contain substring textUtils.getCount should return 1") {
    assert(classTextUtils.getCount("abc", "d") == 0)
  }

  test("If substring is empty textUtils.getCount should throw an ArithmeticException: / by zero") {
    assertThrows[ArithmeticException] {
      classTextUtils.getCount("abc", "")
    }
  }

}
