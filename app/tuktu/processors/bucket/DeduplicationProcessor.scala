package tuktu.processors.bucket

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import play.api.libs.iteratee.Enumeratee
import play.api.libs.json.JsObject
import tuktu.api._
import scala.collection.GenTraversableOnce

/**
 * Deduplicates all elements in a bucketed datapacket
 */
class DeduplicationProcessor(resultName: String) extends BaseBucketProcessor(resultName) {
    var fields: List[String] = _

    override def initialize(config: JsObject) {
        // Get the field to sort on
        fields = (config \ "fields").as[List[String]]
    }

    override def doProcess(data: List[Map[String, Any]]): List[Map[String, Any]] = {
        var deduplicatedResult = collection.mutable.Map[List[Any], Map[String, Any]]()

        for (datum <- data) {
            // Get the field names we need
            val key = fields.map(field => datum(field)).toList
            // Only include if not present yet
            if (!deduplicatedResult.contains(key))
                deduplicatedResult += key -> datum
        }

        // Turn into data packet
        deduplicatedResult.map { case (key, datum) => datum }.toList
    }
}