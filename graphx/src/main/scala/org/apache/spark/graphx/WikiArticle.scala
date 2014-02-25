package org.apache.spark.graphx

import java.util.regex.Pattern
// import org.apache.spark.graphx._
import java.util.regex.Matcher
import scala.util.matching.Regex
import scala.collection.mutable
import scala.xml._
import org.apache.spark.serializer.KryoRegistrator
// import org.apache.spark.util.collection.OpenHashSet
import scala.collection.immutable.HashSet
import java.security.MessageDigest
import java.nio.ByteBuffer


class WikiArticle(wtext: String) extends Serializable {
  @transient val links: Array[String] = WikiArticle.parseLinks(wtext)
  @transient val neighbors = links.map(WikiArticle.titleHash).distinct
  @transient lazy val redirect: Boolean = !WikiArticle.redirectPattern.findFirstIn(wtext).isEmpty
  @transient lazy val stub: Boolean = !WikiArticle.stubPattern.findFirstIn(wtext).isEmpty
  @transient lazy val disambig: Boolean = !WikiArticle.disambigPattern.findFirstIn(wtext).isEmpty
  @transient lazy val tiXML = WikiArticle.titlePattern.findFirstIn(wtext).getOrElse("")
  val title: String = {
    try {
      XML.loadString(tiXML).text
    } catch {
      case e => "NOTFOUND" // don't use null because we get null pointer exceptions
    }
  }
  val relevant: Boolean = !(redirect || stub || disambig || title == "NOTFOUND" || title == null)
  val vertexID: VertexId = WikiArticle.titleHash(title)
  val edges: HashSet[Edge[Double]] = {
    val temp = neighbors.map { n => Edge(vertexID, n, 1.0) }
    val set = new HashSet[Edge[Double]]() ++ temp
    set
  }
  // val edges: HashSet[(VertexId, VertexId)] = {
  //   val temp = neighbors.map { n => (vertexID, n) }
  //   val set = new HashSet[(VertexId, VertexId)]() ++ temp
  //   set
  // }
}

object WikiArticle {
  @transient val titlePattern = "<title>(.*)<\\/title>".r
  @transient val redirectPattern = "#REDIRECT\\s+\\[\\[(.*?)\\]\\]".r
  @transient val disambigPattern = "\\{\\{disambig\\}\\}".r
  @transient val stubPattern = "\\-stub\\}\\}".r
  @transient val linkPattern = Pattern.compile("\\[\\[(.*?)\\]\\]", Pattern.MULTILINE) 

  private def parseLinks(wt: String): Array[String] = {
    val linkBuilder = new mutable.ArrayBuffer[String]()
    val matcher: Matcher = linkPattern.matcher(wt)
    while (matcher.find()) {
      val temp: Array[String] = matcher.group(1).split("\\|")
      if (temp != null && temp.length > 0) {
        val link: String = temp(0)
        if (!link.contains(":")) {
          linkBuilder += link
        }
      }
    }
    return linkBuilder.toArray
  }

  // substitute underscores for spaces and make lowercase
  private def canonicalize(title: String): String = {
    title.trim.toLowerCase.replace(" ", "_")
  }

  // Hash of the canonical article name. Used for vertex ID.
  // TODO this should be a 64bit hash
  private def titleHash(title: String): VertexId = { math.abs(WikiArticle.myHashcode(canonicalize(title))) }

  private def myHashcode(s: String): Long = {
    var h: Long = 1125899906842597L  // prime
    // var h = 29
    val len: Int = s.length
    for (i <- 0 until len) {
      h = 31*h + s.charAt(i)
    }
    h
//     val md: MessageDigest = MessageDigest.getInstance("MD5")
//     md.update(s.getBytes)
//     val result: Array[Byte] = md.digest()
//     val longResult = ByteBuffer.wrap(result).getLong
//     // shift result by 2
//     val retval = longResult >> 10
//     retval
  }

}


