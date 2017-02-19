package tuktu.zetta.processors.utils

import play.api.libs.json.JsObject
import play.api.libs.iteratee.Enumeratee
import tuktu.api.{ BaseProcessor, DataPacket, utils }
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Copy a file into another.
 */
class MkStringProcessor(resultName: String) extends BaseProcessor(resultName) {
    var fields: Option[Seq[String]] = _
    var separator: String = _

    override def initialize(config: JsObject) {
        fields = (config \ "fields").asOpt[Seq[String]]
        separator = (config \ "separator").asOpt[String].getOrElse("")
    }

    override def processor(): Enumeratee[DataPacket, DataPacket] = Enumeratee.mapM(data => Future {
        DataPacket(data.data.map( datum => {
            fields match {
                case None => {
                    (for ( key <- datum.keySet ) yield {
                        (key -> (datum( key ) match {
                            case seq: Seq[Any] => seq.map( _.toString ).mkString( separator )
                            case x => x 
                        }))
                    }).toMap       
                }
                case Some( seqFields ) => {
                    (for ( key <- datum.keySet ) yield {
                        (key -> (if ( seqFields.contains( key ) )
                        {
                            datum( key ).asInstanceOf[Seq[Any]].map( _.toString ).mkString( separator )
                        }
                        else
                        {
                            datum( key )    
                        }))
                    }).toMap
                }
            }
        }))
    })
}