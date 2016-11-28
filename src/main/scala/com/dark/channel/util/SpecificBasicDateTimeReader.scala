package com.dark.channel.util
import org.elasticsearch.hadoop.serialization.Parser
import org.elasticsearch.spark.sql.ScalaRowValueReader
import org.joda.time.format.ISODateTimeFormat
/**
  * Created by darkxue on 28/11/16.
  */
class SpecificBasicDateTimeReader extends ScalaRowValueReader {
  override def date(value: String, parser: Parser): AnyRef = {
    parser.currentName() match {
      case "date" =>
        new java.sql.Timestamp(ISODateTimeFormat.basicDateTime().parseDateTime(value).getMillis).asInstanceOf[AnyRef]
      case x =>
        super.date(value, parser)
    }
  }
}
