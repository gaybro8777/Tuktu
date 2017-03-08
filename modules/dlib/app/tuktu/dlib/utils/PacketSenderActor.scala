package tuktu.dlib.utils

import akka.actor._
import tuktu.api._

/**
 * Actor for forwarding data packets
 */
class PacketSenderActor(remoteGenerator: ActorRef) extends Actor with ActorLogging {
    remoteGenerator ! new InitPacket
    
    def receive() = {
        case sp: StopPacket => {
            remoteGenerator ! sp
            self ! PoisonPill
        }
        case datum: Map[String, Any] => {
            // Directly forward
            remoteGenerator ! DataPacket(List(datum))
            sender ! "ok"
        }
    }
}