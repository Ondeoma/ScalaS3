package scalas3.s3

import java.util.concurrent.ConcurrentHashMap

/** 設定の文字列（バケットポリシーや CORS）を解析した結果を覚えておく。リクエストのたびに JSON や XML を解析しないため。
  * 文字列そのものをキーにするので、設定が変われば別のエントリになる。数が上限を超えたら全部捨てて作り直す。
  */
private[s3] final class ParseCache[A](parse: String => Option[A], maxEntries: Int = 1024):
  private val entries = ConcurrentHashMap[String, Option[A]]()

  def apply(text: String): Option[A] =
    val hit = entries.get(text)
    if hit != null then hit
    else
      val parsed = parse(text)
      if entries.size >= maxEntries then entries.clear()
      entries.put(text, parsed): Unit
      parsed
