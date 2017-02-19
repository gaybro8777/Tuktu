package tuktu.dlib.processors

import play.api.libs.json.JsObject
import play.api.libs.iteratee.Enumeratee
import tuktu.api.{ BaseProcessor, DataPacket, utils }
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Derives the URL of an LRE thumbnail from its resource Id.
 */
class LREThumbnailProcessor(resultName: String) extends BaseProcessor(resultName) {
    var field: String = _

    override def initialize(config: JsObject) {
        field = (config \ "field").as[String]
    }

    override def processor(): Enumeratee[DataPacket, DataPacket] = Enumeratee.mapM(data => Future {
        DataPacket(data.data.map( datum => {
            val id = datum(field).toString.toInt
            val tmp = (id % 1000).toString
            val folder = tmp.length match {
              case 1 => "00" + tmp 
              case 2 => "0" + tmp
              case 3 => tmp
            }
            
            datum + ( resultName -> ("http://lrethumbnails.eun.org/" + folder + "/" + id + ".png") ) 
        }))
    })
}