package tuktu.dlib.processors.oaipmh

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import play.api.libs.iteratee.Enumeratee
import play.api.libs.json.JsValue
import play.api.libs.concurrent.Akka
import play.api.Play.current
import tuktu.api._
import play.api.libs.json._
import play.api.cache.Cache
import scala.collection.mutable.ListBuffer
import tuktu.dlib.utils._


/**
 * Harvests metadata records or metadata records identifiers from an OAI-PMH target repository.
 */
class HarvesterProcessor(genActor: ActorRef, resultName: String) extends BufferProcessor(genActor, resultName) {
    
  implicit val timeout = Timeout(Cache.getAs[Int]("timeout").getOrElse(5) seconds)
    
    // Set up the packet sender actor
    val packetSenderActor = Akka.system.actorOf(Props(classOf[PacketSenderActor], genActor))
    
    var identifiersOnly: Boolean = _
    var target: String = _
    var metadataPrefix: String = _
    var from: Option[String] = _
    var until: Option[String] = _
    var set: Option[String] = _
    var toj: Boolean = _
    
    override def initialize(config: JsObject) 
    {
        identifiersOnly = (config \ "identifiersOnly").asOpt[Boolean].getOrElse(false)
        target = (config \ "target").as[String]
        metadataPrefix = (config \ "metadataPrefix").as[String]
        from = (config \ "from").asOpt[String]
        until = (config \ "until").asOpt[String]
        set = (config \ "set").asOpt[String]
        toj = (config \ "toJSON").asOpt[Boolean].getOrElse(false)
    }
    
    override def processor(): Enumeratee[DataPacket, DataPacket] = Enumeratee.mapM((data: DataPacket) => {
        val futures = Future.sequence( 
            (for (datum <- data.data) yield
            {
               // compose verb
               val verb = identifiersOnly match{
                 case false => utils.evaluateTuktuString(target, datum) + "?verb=ListRecords"
                 case true => utils.evaluateTuktuString(target, datum) + "?verb=ListIdentifiers"
               }
               // compose params
               val params = "&metadataPrefix=" + utils.evaluateTuktuString(metadataPrefix, datum) + (from match{
                  case None => ""
                  case Some(f) => "&from=" + utils.evaluateTuktuString(f, datum)
                }) + (until match{
                  case None => ""
                  case Some(u) => "&until=" + utils.evaluateTuktuString(u, datum)
                }) + (set match{
                  case None => ""
                  case Some(s) => "&set=" + utils.evaluateTuktuString(s, datum)
                })
              // harvest
              listRecords( verb, params, datum )
            }).flatten
        )
        
        futures.map {
            case _ => data
        }
    }) compose Enumeratee.onEOF(() => packetSenderActor ! new StopPacket)
    
  /**
   * Utility method to (selectively) harvest metadata records (or their identifiers) from a repository
   * @param verb: The OAI-PMH verb url (listrecords or listidentifiers)
   * @param params: The harvesting parameters for the verb considered
   * @param datum: The datum containing the query 
   * @return the corresponding metadata records or identifiers packaged in separate data packets.
   */

    def listRecords( verb: String, params: String, datum: Map[String, Any] ): Seq[Future[Any]] =
    {
        val response = oaipmh.harvest( verb + params )
        // check for error
        (response \\ "error").headOption match
        {
            case None => {
                // extract records and send them to channel
                val records = identifiersOnly match {
                    case false => (response \ "ListRecords" \ "record" \ "metadata" ).flatMap( _.child ).toSeq
                    case true => (response \ "ListIdentifiers" \ "header" ).toSeq
                }
                val recs = (records map { rec => rec.toString.trim })
                val result = for (record <- recs; if (!record.isEmpty)) yield
                {
                    toj match{
                        case false => packetSenderActor ? (datum + ( resultName -> record ))
                        case true => packetSenderActor ? (datum + ( resultName -> oaipmh.xml2jsObject( record ) ))
                    }
                }
                // check for resumption token
                val rToken = identifiersOnly match{
                    case false => ( response \ "ListRecords" \ "resumptionToken" ).headOption
                    case true => ( response \ "ListIdentifiers" \ "resumptionToken" ).headOption
                }
                rToken match
                {
                    case Some( resumptionToken ) => result ++ listRecords( verb, ("&resumptionToken=" + resumptionToken.text), datum ) // keep harvesting
                    case None => result // harvesting completed
                }
            }  
            case Some( err ) => {
                toj match{
                    case false => Seq(packetSenderActor ? (datum + ( resultName -> response.toString )))
                    case true => Seq(packetSenderActor ? (datum + ( resultName -> oaipmh.xml2jsObject( response.toString ) )))
                }
            }
        }
    }
}