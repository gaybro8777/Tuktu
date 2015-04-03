package controllers

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import akka.actor.ActorIdentity
import akka.actor.Identify
import akka.actor.actorRef2Scala
import akka.pattern.ask
import akka.util.Timeout
import play.api.Play.current
import play.api.libs.concurrent.Akka
import play.api.mvc.Action
import play.api.mvc.Controller
import play.api.cache.Cache

object Asynchronous extends Controller {
    implicit val timeout = Timeout(Cache.getAs[Int]("timeout").getOrElse(5) seconds)

    /**
     * Loads a config and executes the data processing based on an ID
     * @param id String The ID (name) of the config to fetch
     */
    def load(id: String) = Action {
    	// Send this to our analytics async handler
    	Akka.system.actorSelection("user/TuktuDispatcher") ! new DispatchRequest(id, None, false, false, false, None)
        
        Ok("")
    }
    
    /**
     * Does the same as load but now the config is given as post parameter
     */
    def loadPost() = Action { implicit request =>
        // Dispatch for this user, with config given
        val jsonBody = request.body.asJson.getOrElse(null)
        if (jsonBody != null) {
            // Get the ID from the request
            val id = (jsonBody \ "id").as[String]
	    	// Send this to our analytics async handler
	    	Akka.system.actorSelection("user/TuktuDispatcher") ! new DispatchRequest(id, Some(jsonBody), false, false, false, None)
        }
	    
        Ok("")
    }

}