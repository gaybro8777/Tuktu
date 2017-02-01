package monitor

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.PoisonPill
import akka.routing.Broadcast
import akka.util.Timeout
import play.api.Play.current
import play.api.cache.Cache
import play.api.Logger
import tuktu.api._
import tuktu.api.utils.MapToJsObject
import play.api.Play
import play.api.libs.json.Json

class DataMonitor extends Actor with ActorLogging {
    implicit val timeout = Timeout(Cache.getAs[Int]("timeout").getOrElse(5) seconds)

    // Monitoring maps, uuid -> AppMonitorObject
    val appMonitor = collection.mutable.Map.empty[String, AppMonitorObject]

    // Keep track of ActorRef addresses -> uuid
    val uuidMap = collection.mutable.Map.empty[String, String]

    // Mapping of all subflows
    val subflowMap = collection.mutable.Map.empty[String, String]

    // Keep track of a list of actors we need to notify on push base about events happening
    val eventListeners = collection.mutable.Set.empty[ActorRef]

    // Remove expired monitor objects from appMonitor
    def removeFromMonitor(removed: TraversableOnce[AppMonitorObject]) {
        // Remove from appMonitor
        appMonitor --= removed.map { _.uuid }

        // Remove expired ActorRefs from uuidMap and subflowMap
        val expiredUUIDs = removed.map { _.uuid }.toSet
        val expiredActorRefs = uuidMap.filter { case (_, uuid) => expiredUUIDs.contains(uuid) }.keys
        uuidMap --= expiredActorRefs
        subflowMap --= expiredActorRefs
    }

    def receive = {
        case any => {
            handle(any)
            for (listener <- eventListeners) listener.forward(any)
        }
    }

    def handle: PartialFunction[Any, Unit] = {
        case "init" => {
            // Initialize monitor
        }
        case "clearFinished" =>
            removeFromMonitor(appMonitor.filter { app => app._2.errors.isEmpty && app._2.endTime != None }.values)
        case "clearErrors" =>
            removeFromMonitor(appMonitor.filter { app => app._2.errors.nonEmpty && app._2.endTime != None }.values)
        case cfp: ClearFlowPacket =>
            removeFromMonitor(List(appMonitor.get(cfp.uuid)).flatten)
        case fmp: SubflowMapPacket => {
            // Add to our map
            for (subflow <- fmp.subflows)
                subflowMap += subflow.path.toStringWithoutAddress -> fmp.mailbox.path.toStringWithoutAddress
        }
        case bpnp: BackPressureNotificationPacket => {
            // Notify the generator(s)
            // TODO: If a generator is executed multiple times, we should know which instance is actually blocking here
            if (appMonitor.contains(bpnp.idString)) {
                appMonitor(bpnp.idString).actors.foreach(actor => actor ! new BackPressurePacket())
            }
        }
        case aip: AppInitPacket => {
            if (!appMonitor.contains(aip.uuid)) {
                removeFromMonitor(appMonitor.filter(_._2.is_expired).values)
                appMonitor += (aip.uuid -> new AppMonitorObject(
                    aip.uuid,
                    aip.configName,
                    aip.instanceCount,
                    aip.timestamp,
                    aip.stopOnError))
            }
            aip.mailbox.collect {
                case mailbox => {
                    appMonitor(aip.uuid).actors += mailbox
                    uuidMap += mailbox.path.toStringWithoutAddress -> aip.uuid
                }
            }
        }
        case enp: ErrorNotificationPacket => {
            // Get the actors and stop them
            appMonitor.get(enp.uuid) collect {
                case app => {
                    app.errors += enp.processorName -> ("Error happened at flow: " + enp.configName + ", processor: " + enp.processorName + ", id: " + enp.uuid + ", on Input:\n" + enp.input.take(1000) + { if (enp.input.size > 1000) " [...]" else "" } + "\n" + enp.error.toString)
                    app.actors.foreach(_ ! new ErrorPacket)
                    if (app.stopOnError) app.actors.foreach(_ ! new StopPacket)
                }
            }
        }
        case amel: AddMonitorEventListener    => eventListeners += sender
        case rmel: RemoveMonitorEventListener => eventListeners -= sender
        case amp: AppMonitorPacket => {
            uuidMap.get(amp.getParentName) match {
                case Some(id) => self ! new AppMonitorUUIDPacket(id, amp.status, amp.timestamp)
                case None     => Logger.warn("DataMonitor received AppMonitorPacket for unknown app: " + amp.getName)
            }
        }
        case amp: AppMonitorUUIDPacket => {
            // Get app from appMonitor 
            appMonitor.get(amp.uuid) match {
                case None => Logger.warn("DataMonitor received AppMonitorUUIDPacket for unknown app: " + amp.uuid)
                case Some(app) => amp.status match {
                    case "done" => {
                        app.finished_instances += 1

                        if (app.instances <= app.finished_instances) {
                            // Update end time and start expiration
                            app.expire(amp.timestamp)
                            if (app.instances < app.finished_instances)
                                // This shouldn't happen
                                Logger.warn("App " + amp.uuid + " received \"done\" more often (" + app.finished_instances + ") than it had running instances (" + app.instances + ").")
                        }
                    }
                    case "stop" => {
                        // Broadcast to all actors to stop
                        app.actors.foreach(_ ! Broadcast(new StopPacket))
                    }
                    case "kill" => {
                        // Update end time and start expiration
                        app.expire(amp.timestamp)

                        // Broadcast to all actors to kill themselves
                        app.actors.foreach(_ ! Broadcast(PoisonPill))
                    }
                    case _ => Logger.warn("DataMonitor received AppMonitorUUIDPacket for unknown status: " + amp.status)
                }
            }
        }
        case pmp: ProcessorMonitorPacket => {
            appMonitor.get(pmp.uuid) match {
                case Some(app) => {
                    // Renew expiration and update maps
                    app.expire(pmp.timestamp, false)

                    val latest = app.processorDataPackets.getOrElseUpdate(pmp.processor_id, collection.mutable.Map.empty)
                    latest(pmp.typeOf) = pmp.data

                    val count = app.processorDatumCount.getOrElseUpdate(pmp.processor_id, collection.mutable.Map.empty.withDefaultValue(0))
                    count(pmp.typeOf) += pmp.data.size

                    val DPcount = app.processorDataPacketCount.getOrElseUpdate(pmp.processor_id, collection.mutable.Map.empty.withDefaultValue(0))
                    DPcount(pmp.typeOf) += 1

                    // If BeginType, queue up new BeginTime
                    if (pmp.typeOf == BeginType) {
                        val beginTimes = app.processorBeginTimes.getOrElseUpdate(pmp.processor_id, collection.mutable.Queue.empty)
                        beginTimes.enqueue(pmp.timestamp)
                    } else if (pmp.typeOf == EndType) {
                        val beginTimes = app.processorBeginTimes.getOrElseUpdate(pmp.processor_id, collection.mutable.Queue.empty)
                        if (beginTimes.nonEmpty) {
                            val durations = app.processorDurations.getOrElseUpdate(pmp.processor_id, collection.mutable.ListBuffer.empty)
                            durations += pmp.timestamp - beginTimes.dequeue
                        } else {
                            Logger.warn("DataMonitor received more ProcessorMonitorPackets of type EndType than BeginType for app " + pmp.uuid + " and processor " + pmp.processor_id)
                        }
                    }
                }
                case None => {
                    Logger.warn("DataMonitor received ProcessorMonitorPacket for unkown app with uuid: " + pmp.uuid)
                }
            }
        }
        case mp: MonitorPacket => {
            appMonitor.get(mp.uuid) match {
                case Some(app) => {
                    // Renew expiration and update maps
                    app.expire(mp.timestamp, false)

                    val count = app.flowDatumCount.getOrElseUpdate(mp.branch, collection.mutable.Map.empty.withDefaultValue(0))
                    count(mp.typeOf) += mp.amount

                    val DPcount = app.flowDataPacketCount.getOrElseUpdate(mp.branch, collection.mutable.Map.empty.withDefaultValue(0))
                    DPcount(mp.typeOf) += 1
                }
                case None => {
                    // TODO: Doesn't work well with concurrent processor
                    //Logger.warn("DataMonitor received MonitorPacket for unkown app with uuid: " + mp.uuid)
                }
            }
        }
        case mop: MonitorOverviewRequest => {
            // Remove expired objects from monitor
            removeFromMonitor(appMonitor.filter(_._2.is_expired).values)
            // Partition remaining objects whether they are finished or not
            val (running, finished) = appMonitor.partition(_._2.endTime == None)
            sender ! new MonitorOverviewResult(running toMap, finished toMap, subflowMap toMap)
        }
        case mldp: MonitorLastDataPacketRequest => {
            val result = appMonitor.get(mldp.flow_name) match {
                case None => ("Nothing recorded for this flow yet.", "Nothing recorded for this flow yet.")
                case Some(appData) => {
                    val in = appData.processorDataPackets.get(mldp.processor_id) match {
                        case None => "No DataPackets recorded for this procsesor yet."
                        case Some(map) => map.get(BeginType) match {
                            case None     => "No incoming DataPackets recorded for this processor yet."
                            case Some(dp) => Json.prettyPrint(Json.toJson(dp.data.map(datum => MapToJsObject(datum))))
                        }
                    }
                    val out = appData.processorDataPackets.get(mldp.processor_id) match {
                        case None => "No DataPackets recorded for this procsesor yet."
                        case Some(map) => map.get(EndType) match {
                            case None     => "No processed DataPackets recorded for this processor yet."
                            case Some(dp) => Json.prettyPrint(Json.toJson(dp.data.map(datum => MapToJsObject(datum))))
                        }
                    }
                    (in, out)
                }
            }
            sender ! result
        }
        case m => Logger.warn("DataMonitor received unknown message: " + m)
    }
}