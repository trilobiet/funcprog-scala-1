package quickcheck

import org.scalacheck._
import Arbitrary.{arbitrary, _}
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  lazy val genHeap: Gen[H] =
    for {
      i <- arbitrary[Int]
      h <- oneOf(const(empty),genHeap)
    } yield insert(i,h)


  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

  // Example 1:
  // Adding a single element to an empty heap, and then removing this element,
  // should yield the element in question.
  property("min1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }

  // Example 2:
  // For any heap, adding the minimal element, and then finding it,
  // should return the element in question.
  property("gen1") = forAll { (h: H) =>
    val m = if (isEmpty(h)) 0 else findMin(h)
    findMin(insert(m, h)) == m
  }

  // Hint 1:
  // If you insert any two elements into an empty heap,
  // finding the minimum of the resulting heap should get the smallest of the two elements back.
  property("min2") = forAll { (a: Int, b: Int) =>
    val h = insert(b, insert(a, empty))
    findMin(h) == Math.min(a,b)
  }

  // Hint 2:
  // If you insert an element into an empty heap, then delete the minimum,
  // the resulting heap should be empty.
  property("del1") = forAll { a: Int =>
    val h = deleteMin(insert(a, empty))
    isEmpty(h)
  }

  // Hint 3:
  // Given any heap, you should get a sorted sequence of elements
  // when continually finding and deleting minima.
  // (Hint: recursion and helper functions are your friends.)
  property("sort1") = forAll { h: H =>

    def inner(prevMin: Option[Int], heap: H): Boolean = {

      if (isEmpty(heap)) true
      else if (prevMin.nonEmpty && prevMin.get > findMin(heap)) false
      else inner(Some(findMin(heap)), deleteMin(heap))
    }

    inner(None,h)
  }

  // Hint 4:
  // Finding a minimum of the melding of any two heaps
  // should return a minimum of one or the other.
  property("meld1") = forAll { (h1: H, h2: H) =>

    if (isEmpty(h1) || isEmpty(h2)) true
    else {
      val h3 = meld(h1, h2)
      findMin(h3) == Math.min(findMin(h1), findMin(h2))
    }
  }

  // Acdhirr's addition 1:
  // Deleting the minimum for a heap should remove the
  // value returned by findMin.
  property("delMin") = forAll { h1:H =>

    val min = findMin(h1)
    val h2 = deleteMin(h1)
    val h3 = insert(min, h2)

    // Now h3 must be equal to h1
    //println("Equals? " + heapsAreEqual(h1,h3))
    heapsAreEqual(h1,h3)
  }

  // Acdhirr's addition 2:
  // Any int pair (ij) added to the meld of Heaps h1 and h2
  // must result in a heap that is equal to the meld of
  // i added to h1 and j added to h2
  property("meld&insert") = forAll { (h1: H, h2: H, ij: (Int,Int) ) =>

    val (i,j) = ij
    val h3 = insert(i,insert(j,meld(h1, h2)))
    val h4 = meld(insert(i,h1),insert(j,h2))

    heapsAreEqual(h3,h4)
  }

  // Check equality for two heaps
  def heapsAreEqual(h1: H, h2: H): Boolean = {

    if (isEmpty(h1) && isEmpty(h2)) true
    else if ((isEmpty(h1) && !isEmpty(h2)) ||
      (isEmpty(h2) && !isEmpty(h1))) false
    else
      (findMin(h1) == findMin(h2)) &&
        heapsAreEqual(deleteMin(h1),deleteMin(h2))
  }

}


