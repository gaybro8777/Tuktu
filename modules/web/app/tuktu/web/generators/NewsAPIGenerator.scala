package tuktu.web.generators

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Props
import akka.routing.RoundRobinPool
import play.api.Play.current
import play.api.libs.concurrent.Akka
import play.api.libs.iteratee.Enumeratee
import play.api.libs.json.JsValue
import tuktu.api._
import scala.concurrent.duration.DurationInt
import scala.concurrent.ExecutionContext.Implicits.global
import akka.routing.Broadcast
import akka.actor.PoisonPill
import play.api.libs.ws.WS
import play.api.libs.json.JsObject
import org.joda.time.format.DateTimeFormat
import play.api.libs.json.JsString
import play.api.libs.json.Json

case class PollRound()
case class PollReply(
        articles: List[JsObject]
)

class PollerActor(parent: ActorRef, token: String, source: String, tags: List[String]) extends Actor with ActorLogging {
    var latestTimeSeen: Long = 0L
    val timeFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ")
    
    def receive() = {
        case pr: PollRound => {
            // Make the request for the given source
            val request = WS.url("https://newsapi.org/v1/articles?source=" + source + "&sortBy=latest&apiKey=" + token)
            request.get.map {result =>
                if (result.status == 200) {
                    val articles = (result.json.as[JsObject] \ "articles").as[List[JsObject]]
                        .filter{article =>
                            // Find the ones that are new
                            val time = timeFormat.parseDateTime((article \ "publishedAt").as[String]).getMillis
                            time > latestTimeSeen
                        }
                    
                    // Forward the articles to parent actor
                    parent ! new PollReply(articles.map {article =>
                        article + ("source" -> JsString(source)) + ("tags" -> Json.toJson(tags.filter {tag =>
                                val title = (article \ "title").as[String].toLowerCase
                                val description = (article \ "description").as[String].toLowerCase
                                title.toLowerCase.contains(tag) || description.toLowerCase.contains(tag)
                            }))
                    })
                    
                    // Update latestTimeSeen
                    if (articles.size > 0)
                        latestTimeSeen = timeFormat.parseDateTime((articles.maxBy{article =>
                            timeFormat.parseDateTime((article \ "publishedAt").as[String]).getMillis
                        } \ "publishedAt").as[String]).getMillis
                }
            }
        }
        case sp: StopPacket => self ! PoisonPill
    }
}

class NewsAPIGenerator(resultName: String, processors: List[Enumeratee[DataPacket, DataPacket]], senderActor: Option[ActorRef]) extends BaseGenerator(resultName, processors, senderActor) {
    var pollerActors: List[ActorRef] = _
    
    override def _receive = {
        case config: JsValue => {
            // Get token
            val token = (config \ "token").as[String]
            // Get tags
            val tags = (config \ "tags").as[List[String]]
            // Get sources
            val sources = (config \ "sources").asOpt[List[String]].getOrElse(
                    List("al-jazeera-english","ars-technica","bild","breitbart-news","business-insider","business-insider-uk","buzzfeed","daily-mail","der-tagesspiegel","die-zeit","engadget","espn-cric-info","financial-times","football-italia","fortune","four-four-two","fox-sports","gruenderszene","hacker-news","handelsblatt","ign","mashable","metro","mirror","mtv-news","newsweek","new-york-magazine","nfl-news","reddit-r-all","reuters","talksport","techcrunch","techradar","the-economist","the-guardian-uk","the-hindu","the-lad-bible","the-next-web","the-sport-bible","the-telegraph","the-times-of-india","the-verge","time","usa-today","wired-de","wirtschafts-woche")
            )
            // Update time, default to once every hour
            val updateTime = (config \ "update_time").asOpt[Int].getOrElse(3600)
            
            if (sources.isEmpty || token.isEmpty) self ! new StopPacket
            else {
                // Set up actors
                pollerActors = sources.map {source =>
                    Akka.system.actorOf(Props(classOf[PollerActor], self, token, source, tags))
                }
                
                // Set up scheduling
                Akka.system.scheduler.schedule(
                    0 seconds,
                    updateTime seconds,
                    self,
                new PollRound)
            }
        }
        case pr: PollRound => {
            // Send through the sources to the actor pool
            pollerActors.foreach(_ ! pr)
        }
        case sp: StopPacket => {
            if (pollerActors != null)
                pollerActors.foreach(_ ! sp)
            cleanup
        }
        case pr: PollReply => pr.articles.foreach {article =>
            channel.push(new DataPacket(List(Map(resultName -> article))))
        }
    }
}