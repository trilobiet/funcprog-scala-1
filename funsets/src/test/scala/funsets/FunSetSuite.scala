package funsets

import org.junit._

/**
 * This class is a test suite for the methods in object FunSets.
 *
 * To run this test suite, start "sbt" then run the "test" command.
 */
class FunSetSuite {

  import FunSets._

  @Test def `contains is implemented`: Unit = {
    assert(contains(x => true, 100))
  }

  /**
   * When writing tests, one would often like to re-use certain values for multiple
   * tests. For instance, we would like to create an Int-set and have multiple test
   * about it.
   *
   * Instead of copy-pasting the code for creating the set into every test, we can
   * store it in the test class using a val:
   *
   *   val s1 = singletonSet(1)
   *
   * However, what happens if the method "singletonSet" has a bug and crashes? Then
   * the test methods are not even executed, because creating an instance of the
   * test class fails!
   *
   * Therefore, we put the shared values into a separate trait (traits are like
   * abstract classes), and create an instance inside each test method.
   *
   */

  trait TestSets {
    val s1 = singletonSet(1)
    val s2 = singletonSet(2)
    val s3 = singletonSet(3)
  }

  /**
   * This test is currently disabled (by using @Ignore) because the method
   * "singletonSet" is not yet implemented and the test would fail.
   *
   * Once you finish your implementation of "singletonSet", remvoe the
   * @Ignore annotation.
   */
  // @Ignore("not ready yet")
  @Test def `singleton set one contains one`: Unit = {

    /**
     * We create a new instance of the "TestSets" trait, this gives us access
     * to the values "s1" to "s3".
     */
    new TestSets {
      /**
       * The string argument of "assert" is a message that is printed in case
       * the test fails. This helps identifying which assertion failed.
       */
      assert(contains(s1, 1), "Singleton")
    }
  }

  @Test def `union contains all elements of each set`: Unit = {
    new TestSets {
      val s = union(s1, s2)
      assert(contains(s, 1), "Union 1")
      assert(contains(s, 2), "Union 2")
      assert(!contains(s, 3), "Union 3")
    }
  }

  // By acdhirr
  @Test def `intersect contains only elements that are in both sets`: Unit = {
    new TestSets {
      val s = union(s1, s2)
      val t = union(s2, s3)
      val u = intersect(s,t)
      assert(!contains(u, 1), "Intersect 1")
      assert(contains(u, 2), "Intersect 2")
      assert(!contains(u, 3), "Intersect 3")
    }
  }

  // By acdhirr
  @Test def `diff contains only elements that are in s but not in t`: Unit = {
    new TestSets {
      val s = union(s1, s2)
      val t = diff(s, s1) // (1,2) diff (1) = 2
      assert(contains(t, 2), "Diff 2")
      assert(!contains(t, 1), "Diff 1")
    }
  }

  // By acdhirr
  @Test def `test filter`: Unit = {
    new TestSets {
      val s = union(s1, s2)
      val t = filter(s, x => x%2==0)
      assert(contains(t,2), "Filter 2")
      assert(!contains(t,1), "Filter 1")
    }
  }

  // By acdhirr
  @Test def `test forall`: Unit = {
    new TestSets {
      val s = union(s1, s3)
      val f = (x: Int) => x%2==1 // both elems are odd
      assert( forall( s, f ) , "Forall 1,3")
    }
  }

  // By acdhirr
  @Test def `test exists`: Unit = {
    new TestSets {
      val sOdd = union(s1, s3)
      val sAll = union(sOdd, s2)
      val f = (x: Int) => x%2==0 // even elem exists
      assert( !exists( sOdd, f ) , "Exists 2")
      assert( exists( sAll, f ) , "Exists 2")
    }
  }

  @Rule def individualTestTimeout = new org.junit.rules.Timeout(10 * 1000)
}
