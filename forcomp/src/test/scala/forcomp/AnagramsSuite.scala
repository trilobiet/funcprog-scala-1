package forcomp

import org.junit._
import org.junit.Assert.assertEquals


class AnagramsSuite {
  import Anagrams._

  @Test def `wordOccurrences: abcd (3pts)`: Unit =
    assertEquals(List(('a', 1), ('b', 1), ('c', 1), ('d', 1)), wordOccurrences("abcd"))

  @Test def `wordOccurrences: Robert (3pts)`: Unit =
    assertEquals(List(('b', 1), ('e', 1), ('o', 1), ('r', 2), ('t', 1)), wordOccurrences("Robert"))


  @Test def `sentenceOccurrences: abcd e (5pts)`: Unit =
    assertEquals(List(('a', 1), ('b', 1), ('c', 1), ('d', 1), ('e', 1)), sentenceOccurrences(List("abcd", "e")))

  @Test def `sentenceOccurrences: jan piet klaas`: Unit =
    assertEquals(List(('a', 3), ('e', 1), ('i', 1), ('j', 1), ('k', 1), ('l', 1), ('n', 1), ('p', 1), ('s', 1), ('t', 1)),
      sentenceOccurrences(List("jan", "piet", "klaas")))

  @Test def `dictionaryByOccurrences.get: eat (10pts)`: Unit =
    assertEquals(Some(Set("ate", "eat", "tea")), dictionaryByOccurrences.get(List(('a', 1), ('e', 1), ('t', 1))).map(_.toSet))


  @Test def `wordAnagrams married (2pts)`: Unit =
    assertEquals(Set("married", "admirer"), wordAnagrams("married").toSet)

  @Test def `wordAnagrams player (2pts)`: Unit =
    assertEquals(Set("parley", "pearly", "player", "replay"), wordAnagrams("player").toSet)



  @Test def `subtract: lard - r (10pts)`: Unit = {
    val lard = List(('a', 1), ('d', 1), ('l', 1), ('r', 1))
    val r = List(('r', 1))
    val lad = List(('a', 1), ('d', 1), ('l', 1))
    assertEquals(lad, subtract(lard, r))
  }

  @Test def `subtract: aldaar - ar (10pts)`: Unit = {
    val aldaar = List(('a', 3), ('d', 1), ('l', 1), ('r', 1))
    val ar = List(('a', 1),('r', 1))
    val alda = List(('a', 2), ('d', 1), ('l', 1))
    assertEquals(alda, subtract(aldaar, ar))
  }




  @Test def `combinations: [] (8pts)`: Unit =
    assertEquals(List(Nil), combinations(Nil))

  @Test def `combinations: abba (8pts)`: Unit = {
    val abba = List(('a', 2), ('b', 2))
    val abbacomb = List(
      List(),
      List(('a', 1)),
      List(('a', 2)),
      List(('b', 1)),
      List(('a', 1), ('b', 1)),
      List(('a', 2), ('b', 1)),
      List(('b', 2)),
      List(('a', 1), ('b', 2)),
      List(('a', 2), ('b', 2))
    )
    assertEquals(abbacomb.toSet, combinations(abba).toSet)
  }


  @Test def `sentence anagrams: [] (10pts)`: Unit = {
    val sentence = List()
    assertEquals(List(Nil), sentenceAnagrams(sentence))
  }

  @Test def `sentence anagrams: Linux rulez (10pts)`: Unit = {
    val sentence = List("Linux", "rulez")
    val anas = List(
      List("Rex", "Lin", "Zulu"),
      List("nil", "Zulu", "Rex"),
      List("Rex", "nil", "Zulu"),
      List("Zulu", "Rex", "Lin"),
      List("null", "Uzi", "Rex"),
      List("Rex", "Zulu", "Lin"),
      List("Uzi", "null", "Rex"),
      List("Rex", "null", "Uzi"),
      List("null", "Rex", "Uzi"),
      List("Lin", "Rex", "Zulu"),
      List("nil", "Rex", "Zulu"),
      List("Rex", "Uzi", "null"),
      List("Rex", "Zulu", "nil"),
      List("Zulu", "Rex", "nil"),
      List("Zulu", "Lin", "Rex"),
      List("Lin", "Zulu", "Rex"),
      List("Uzi", "Rex", "null"),
      List("Zulu", "nil", "Rex"),
      List("rulez", "Linux"),
      List("Linux", "rulez")
    )
    assertEquals(anas.toSet, sentenceAnagrams(sentence).toSet)
  }

  @Test def `sentence anagrams: A mad`: Unit = {
    val sentence = List("A", "mad")
    val anas = List(
      List("Adam"),
      List("ad", "am"),
      List("am", "ad")
    )
    assertEquals(anas.toSet, sentenceAnagrams(sentence).toSet)
  }

  @Test def `sentence anagrams: Am mad`: Unit = {
    val sentence = List("am","mad")
    val anas = List(
      List("mad", "am"),
      List("am", "dam"),
      List("am", "mad"),
      List("madam"),
      List("dam", "am")
    )
    assertEquals(anas.toSet, sentenceAnagrams(sentence).toSet)
  }

  @Test def `sentence anagrams: Yes man`: Unit = {
    val sentence = List("Yes","man")
    val anas =  List(
      List("en", "as", "my"),
      List("en", "my", "as"),
      List("man", "yes"),
      List("men", "say"),
      List("as", "en", "my"),
      List("as", "my", "en"),
      List("sane", "my"),
      List("Sean", "my"),
      List("my", "en", "as"),
      List("my", "as", "en"),
      List("my", "sane"),
      List("my", "Sean"),
      List("say", "men"),
      List("yes", "man")
    )
    assertEquals(anas.toSet, sentenceAnagrams(sentence).toSet)
  }




  @Rule def individualTestTimeout = new org.junit.rules.Timeout(10 * 1000)
}
