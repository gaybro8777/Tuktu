package controllers

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import akka.actor._
import akka.pattern.ask
import akka.routing.Broadcast
import akka.routing.SmallestMailboxPool
import akka.util.Timeout
import controllers.nodehandler.nodeHandler
import play.api.Play
import play.api.Play.current
import play.api.cache.Cache
import play.api.libs.concurrent.Akka
import play.api.libs.iteratee.Concurrent
import play.api.libs.iteratee.Enumeratee
import play.api.libs.iteratee.Iteratee
import play.api.libs.json.JsObject
import play.api.libs.json.Json
import tuktu.api._
import tuktu.generators.AsyncStreamGenerator
import tuktu.generators.EOFSyncStreamGenerator
import tuktu.generators.SyncStreamGenerator
import tuktu.processors.EOFBufferProcessor
import tuktu.processors.bucket.concurrent.BaseConcurrentProcessor
import tuktu.processors.meta.ConcurrentProcessor
import mailbox.DeadLetterWatcher
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import java.nio.file.Paths

case class treeNode(
        name: String,
        parents: List[Class[_]],
        children: List[Class[_]]
)

object Dispatcher {
    implicit val timeout = Timeout(Cache.getAs[Int]("timeout").getOrElse(5) seconds)
    /**
     * Flow monitoring enumeratee
     */
    def monitorEnumeratee(uuid: String, branch: String, mpType: MPType): Enumeratee[DataPacket, DataPacket] = {
        Enumeratee.mapM((data: DataPacket) => Future {
            Akka.system.actorSelection("user/TuktuMonitor") ! new MonitorPacket(mpType, uuid, branch, data.size)
            data
        })
    }

    /**
     * Processor monitoring enumeratee
     */
    def processorMonitor(uuid: String, processor_id: String, mpType: MPType): Enumeratee[DataPacket, DataPacket] = {
        Enumeratee.mapM((data: DataPacket) => Future {
            Akka.system.actorSelection("user/TuktuMonitor") ! new ProcessorMonitorPacket(mpType, uuid, processor_id, data)
            data
        })
    }

    /**
     * Takes a list of Enumeratees and iteratively composes them to form a single Enumeratee
     * @param nextId The names of the processors next to compose
     * @param processorMap A map cntaining the names of the processors and the processors themselves
     * @return A 3-tuple containing:
     *   - The idString used to map this flow against (for error recovery)
     *   - A list of all processors, one for each branch
     *   - A list of all subflow actors
     */
    def buildEnums (
            nextIds: List[String],
            processorMap: Map[String, ProcessorDefinition],
            genActor: Option[ActorRef],
            configName: String,
            stopOnError: Boolean
    ): (String, List[Enumeratee[DataPacket, DataPacket]], List[ActorRef]) = {
        // Get log level
        val logLevel = Cache.getAs[String]("logLevel").getOrElse("none")
        // Generate the logEnumeratee ID
        val idString = java.util.UUID.randomUUID.toString
        // Keep track of all subflows
        val subflows = collection.mutable.ListBuffer.empty[ActorRef]
        // Keep track of instantiated processors
        val instantiatedEnumeratees = collection.mutable.Map[String, Enumeratee[DataPacket, DataPacket]]()

        // Count how often a processor is a successor of the generator or a processor
        // and ignore all but the last EOF for each processor that has more than one predecessor
        val referenceCounts = (nextIds ++ processorMap.flatMap { case (id, definition) => definition.next })
            .groupBy { a => a }.mapValues { predecessors => predecessors.size }.filter { case (id, predecessors) => predecessors > 1 }
            .map { case (id, predecessors) =>
                id -> BranchMergeProcessor.ignoreEOFs[DataPacket](predecessors)
            }

        /**
         * Builds a chain of processors recursively
         */
        def buildSequential(
                procName: String,
                accum: Enumeratee[DataPacket, DataPacket],
                iterationCount: Int,
                branch: String
        ): Enumeratee[DataPacket, DataPacket] = {
            // Get processor definition
            val pd = processorMap(procName)

            // Get class of the processor
            val procClazz = Class.forName(pd.name)

            // Check if this processor is a bufferer
            if (classOf[BufferProcessor].isAssignableFrom(procClazz) || classOf[BaseConcurrentProcessor].isAssignableFrom(procClazz)) {
                /*
                 * Bufferer processor, we pass on an actor that can take up the datapackets with the regular
                 * flow and cut off regular flow for now
                 */

                // Get sync or not
                val sync = (pd.config \ "sync").asOpt[Boolean].getOrElse(false)
                // Create an idString for the subflow
                val subIdString = java.util.UUID.randomUUID.toString
                // Create generator
                val generator = sync match {
                    case true => {
                        /*if (classOf[EOFBufferProcessor].isAssignableFrom(procClazz)) {
                            Akka.system.actorOf(SmallestMailboxPool(1).props(
                                Props(classOf[tuktu.generators.EOFSyncStreamGenerator], "",
                                    pd.next.map(processorName => {
                                        // Create the new lines of processors
                                        monitorEnumeratee(subIdString, branch, BeginType) compose
                                        buildSequential(processorName, utils.logEnumeratee(idString, configName), iterationCount + 1, branch) compose
                                        monitorEnumeratee(subIdString, branch, EndType)
                                    }),
                                    {
                                        // If sync is set to true, this is the last one in the line and we need to send bakc
                                        if (sync) genActor
                                        else None
                                    }
                                )), name = "subflows_" + subIdString
                            )
                        } else {*/
                            Akka.system.actorOf(SmallestMailboxPool(1).props(
                                    Props(classOf[tuktu.generators.SyncStreamGenerator], "",
                                        pd.next.map(processorName => {
                                            // Create the new lines of processors
                                            monitorEnumeratee(subIdString, branch, BeginType) compose
                                            buildSequential(processorName, utils.logEnumeratee(idString, configName), iterationCount + 1, branch) compose
                                            monitorEnumeratee(subIdString, branch, EndType)
                                        }),
                                        {
                                            if (classOf[BufferProcessor].isAssignableFrom(procClazz)) {
                                                // If sync is set to true, this is the last one in the line and we need to send bakc
                                                if (sync) genActor
                                                else None
                                            }
                                            else None
                                        }
                                    )), name = "subflows_" + subIdString
                            )
                        //}
                    }
                    case false => {
                        Akka.system.actorOf(SmallestMailboxPool(1).props(
                                Props(classOf[tuktu.generators.AsyncStreamGenerator], "",
                                    pd.next.map(processorName => {
                                        // Create the new lines of processors
                                        monitorEnumeratee(subIdString, branch, BeginType) compose
                                        buildSequential(processorName, utils.logEnumeratee(idString, configName, processorName), iterationCount + 1, branch) compose
                                        monitorEnumeratee(subIdString, branch, EndType)
                                    }),
                                    None
                                )), name = "subflow_" + subIdString
                        )
                    }
                }

                // Dead letter watcher
                val watcher = Akka.system.actorOf(Props(classOf[DeadLetterWatcher], generator), "watcher_" + "subflows_" + subIdString)
                Akka.system.eventStream.subscribe(watcher, classOf[DeadLetter])

                // Notify the monitor so we can recover from errors
                Akka.system.actorSelection("user/TuktuMonitor") ! new AppInitPacket(
                        subIdString,
                        configName,
                        1,
                        stopOnError,
                        Some(generator)
                )
                // Add to subflow list
                subflows += generator

                // Create processor Enumeratee if it doesn't exist yet
                val procEnum = instantiatedEnumeratees.getOrElseUpdate(procName, {
                    // Instantiate new processor
                    val iClazz = {
                        val ic = procClazz.getConstructor(
                                classOf[ActorRef],
                                classOf[String]
                        ).newInstance(
                                generator,
                                pd.resultName
                        )

                        // Initialize the processor first
                        val initMethod = procClazz.getMethods.find(m => m.getName == "initialize").get
                        initMethod.invoke(ic, pd.config)

                        // Return instance
                        ic
                    }

                    // Get Enumeratee from new processor
                    val method = procClazz.getMethods.find(m => m.getName == "processor").get
                    val methodEnum = method.invoke(iClazz).asInstanceOf[Enumeratee[DataPacket, DataPacket]]

                    // Prepend with EOF counter if processor has more than one predecessor
                    if (referenceCounts.contains(pd.id))
                        referenceCounts(pd.id) compose methodEnum
                    else
                        methodEnum
                })

                if (logLevel == "all") {
                    accum compose
                        processorMonitor(idString, pd.id, BeginType) compose
                        procEnum compose
                        processorMonitor(idString, pd.id, EndType) compose
                        utils.logEnumeratee(idString, configName)
                } else {
                    accum compose
                        procEnum compose
                        utils.logEnumeratee(idString, configName)
                }
            } else {
                // 'Regular' processor
                // Create processor Enumeratee if it doesn't exist yet
                val procEnum = instantiatedEnumeratees.getOrElseUpdate(procName, {
                    // Instantiate new processor
                    val iClazz = {
                        val ic = procClazz.getConstructor(classOf[String]).newInstance(pd.resultName)

                        // Initialize the processor first
                        val initMethod = procClazz.getMethods.find(m => m.getName == "initialize").get
                        initMethod.invoke(ic, pd.config)

                        // Return instance
                        ic
                    }

                    // Get Enumeratee from new processor
                    val method = procClazz.getMethods.find(m => m.getName == "processor").get
                    val methodEnum = method.invoke(iClazz).asInstanceOf[Enumeratee[DataPacket, DataPacket]]

                    // Prepend with EOF counter if processor has more than one predecessor
                    if (referenceCounts.contains(pd.id))
                        referenceCounts(pd.id) compose methodEnum
                    else
                        methodEnum
                })

                val composition = if (logLevel == "all") {
                    accum compose
                        processorMonitor(idString, pd.id, BeginType) compose
                        procEnum compose
                        processorMonitor(idString, pd.id, EndType)
                } else {
                    accum compose procEnum
                }

                // Recurse, determine whether we need to branch or not
                pd.next match {
                    case List() => {
                        // No processors left, return accum
                        composition compose utils.logEnumeratee(idString, configName)
                    }
                    case List(id) => {
                        // No branching, just recurse
                        buildSequential(id, composition compose utils.logEnumeratee(idString, configName, id), iterationCount + 1, branch)
                    }
                    case _ => {
                        // We need to branch, use the broadcasting enumeratee
                        composition compose buildBranch(
                            for ((nextId, index) <- pd.next.zipWithIndex) yield
                                buildSequential(nextId, utils.logEnumeratee(idString, configName), iterationCount + 1, branch + "_" + index)
                        )
                    }
                }
            }
        }

        /**
         * Whenever we branch in a flow, we need a special enumeratee that helps us with the broadcasting. The dispatcher
         * should make sure the flow ends there and the broadcaster picks up.
         */
        def buildBranch(
                nextProcessors: List[Enumeratee[DataPacket, DataPacket]]
        ): Enumeratee[DataPacket, DataPacket] = {
            //val queueSize = Cache.getAs[Int]("mon.bp.blocking_queue_size").getOrElse(Play.configuration.getInt("tuktu.monitor.backpressure.blocking_queue_size").getOrElse(1000))
            // Set up the broadcast
            val (enumerator, channel) = Concurrent.broadcast[DataPacket]

            // Set up broadcast
            for (processor <- nextProcessors)
                enumerator |>> processor &>> Iteratee.ignore

            // Make a broadcasting Enumeratee and sink Iteratee
            Enumeratee.mapM[DataPacket]((data: DataPacket) => Future {
                // Broadcast data
                channel.push(data)
                data
            }) compose Enumeratee.onEOF(() => channel.eofAndEnd) compose utils.logEnumeratee(idString)
        }

        // First build all Enumeratees
        (
                idString,
                for ((nextId, index) <- nextIds.zipWithIndex) yield {
                    // Prepend a start packet for the monitor and append a stop packet
                    monitorEnumeratee(idString, index.toString, BeginType) compose
                    buildSequential(nextId, utils.logEnumeratee[DataPacket](idString, configName, nextId), 0, index.toString) compose
                    monitorEnumeratee(idString, index.toString, EndType)
                },
                subflows toList
        )
    }

    /**
     * Builds the processor map
     */
    def buildProcessorMap(processors: List[JsObject]) = {
        // Get all processors
        (for (processor <- processors) yield {
            // Get all fields
            val processorId = (processor \ "id").as[String]
            val processorName = (processor \ "name").as[String]
            val processorConfig = (processor \ "config").as[JsObject]
            val resultName = (processor \ "result").as[String]
            val next = (processor \ "next").as[List[String]]

            // Create processor definition
            val procDef = new ProcessorDefinition(
                    processorId,
                    processorName,
                    processorConfig,
                    resultName,
                    next
            )

            // Return map
            processorId -> procDef
        }).toMap
    }
}

/**
 * This actor is the heart of Tuktu and bootstraps all flows executed by Tuktu.
 * For detailed explanation on this actor, read the manual at http://www.tuktu.io
 */
class Dispatcher(monitorActor: ActorRef) extends Actor with ActorLogging {
    implicit val timeout = Timeout(Cache.getAs[Int]("timeout").getOrElse(5) seconds)

    /**
     * Receive function that does all the magic
     */
    def receive() = {
        case hc: HealthCheck => {
            // We are requested a health check, send back the reply
            sender ! new HealthReply()
        }
        case dr: DispatchRequest => {
            // Get config
            val config = dr.config match {
                case Some(cfg) => cfg
                case None      => utils.loadConfig(dr.configName).get
            }

            // Get source actor if not set yet
            val sourceActor = dr.sync match {
                case false => None
                case true => dr.sourceActor match {
                    case None => Some(sender)
                    case Some(sa) => Some(sa)
                }
            }

            // Start local actor, get all data processors
            val processorMap = Dispatcher.buildProcessorMap((config \ "processors").as[List[JsObject]])

            // Get the data generators
            val generators = (config \ "generators").as[List[JsObject]]
            for ((generator, index) <- generators.zipWithIndex) {
                // Get all fields
                val generatorName = {
                    // TDFS selection, must do that here!
                    if ((generator \ "name").as[String] == "tuktu.generators.LineGenerator" ||
                            (generator \ "name").as[String] == "tuktu.generators.BinaryFileGenerator") {
                        if ((generator \ "config" \ "filename").as[String].startsWith("tdfs://")) {
                            // Replace the generator with the TDFS reader generator
                            (generator \ "name").as[String] match {
                                case "tuktu.generators.LineGenerator" => "tuktu.dfs.generators.TDFSLineReaderGenerator"
                                case "tuktu.generators.BinaryFileGenerator" => "tuktu.dfs.generators.TDFSBinaryReaderGenerator"
                            }
                        } else (generator \ "name").as[String]
                    } else (generator \ "name").as[String]
                }
                val generatorConfig = (generator \ "config").as[JsObject]
                val resultName = (generator \ "result").as[String]
                val next = (generator \ "next").as[List[String]]
                val nodeAddress = (generator \ "nodes").asOpt[List[JsObject]]
                val stopOnError = (generator \ "stop_on_error").asOpt[Boolean].getOrElse(true)

                // Parse the nodes field to see where this generator should be constructed
                val nodeList = nodeHandler.handleNodes(nodeAddress.getOrElse(Nil))

                // Go over all nodes and submit the generator there
                for (nodeInstance <- nodeList) {
                    val hostname = nodeInstance._1
                    val instanceCount = nodeInstance._2

                    // See if this one needs to be started remotely or not
                    val startRemotely = {
                        // We may or may not need to start remotely
                        if (hostname == Cache.getAs[String]("homeAddress").getOrElse("127.0.0.1")) false
                        else true
                    }

                    val clusterNodes = Cache.getOrElse[scala.collection.mutable.Map[String, ClusterNode]]("clusterNodes")(scala.collection.mutable.Map())
                    if (startRemotely && !dr.isRemote && hostname != "" && clusterNodes.contains(hostname)) {
                        // We need to start an actor on a remote location
                        val location = "akka.tcp://application@" + hostname  + ":" + clusterNodes(hostname).akkaPort + "/user/TuktuDispatcher"

                        // Get the identity
                        val fut = Akka.system.actorSelection(location) ? Identify(None)
                        val remoteDispatcher = Await.result(fut.mapTo[ActorIdentity], 5 seconds).getRef

                        // Send a remoted dispatch request, which is just the obtained config
                        dr.returnRef match {
                            case true => {
                                // We need to get the actor reference and return it
                                val refFut = remoteDispatcher ? new DispatchRequest(dr.configName, Some(config), true, dr.returnRef, dr.sync, sourceActor)
                                sender ? Await.result(refFut.mapTo[ActorRef], 5 seconds)
                            }
                            case false => remoteDispatcher ! new DispatchRequest(dr.configName, Some(config), true, dr.returnRef, dr.sync, sourceActor)
                        }
                    } else if (!startRemotely) {
                        // Build the processor pipeline for this generator
                        val (idString, processorEnumeratees, subflows) = Dispatcher.buildEnums(next, processorMap, sourceActor, dr.configName, stopOnError)

                        // Set up the generator
                        val clazz = Class.forName(generatorName)

                        // Make the amount of actors we require
                        val actorRef = {
                            // See if this is the JS generator or not
                            if (classOf[TuktuBaseJSGenerator].isAssignableFrom(clazz)) {
                                // Define ids based on config location that will handle requests
                                // Usually that is just the path directory/filename relative to the analytics folder, stripped from file ending
                                // For backwards compatibility, directory.filename(instead of /) will also work (both POST)
                                // Main GET path are the directories which contain Tuktu.json
                                val option: Option[String] = {
                                    val analytics = Paths.get(current.configuration.getString("tuktu.configrepo").getOrElse("configs"))
                                        .relativize(Paths.get(current.configuration.getString("tuktu.webrepo").getOrElse("configs/analytics")))
                                    val path = analytics.relativize(Paths.get(dr.configName))

                                    // Only depth 2 configs are allowed for now in analytics
                                    if (path.getNameCount != 2)
                                        None
                                    else if (path.endsWith("Tuktu") || path.endsWith("Tuktu.js") || path.endsWith("Tuktu.json"))
                                        // Directories will be represented by their containing Tuktu.json file if present to shorten links of main GET endpoints
                                        Some(path.getParent.toString.replace('\\', '/'))
                                    else
                                        // This is the POST path; regular path, stripped from file ending
                                        // Routes are currently set up in such a way that these cannot be called directly, but only through a JSON body parameter
                                        Some(path.toString.replace('\\', '/').replaceFirst("[.]json^", ""))
                                }

                                val actorRef = Akka.system.actorOf(
                                    SmallestMailboxPool(instanceCount).props(
                                        Props(clazz, resultName, processorEnumeratees, sourceActor)),
                                    name = dr.configName.replaceAll("[^a-zA-Z0-9-_.*$+:@&=,!~';]", "_") + "_" + clazz.getName + "_" + idString)

                                // Add mailbox to hostmap, watch it (to be notified when it's terminated), and return
                                option.map { id =>
                                    Cache.getAs[collection.mutable.Map[String, ActorRef]]("web.hostmap") match {
                                        case None          => play.api.Logger.warn("web.hostmap is not set")
                                        case Some(hostmap) => hostmap += id -> actorRef
                                    }
                                    context.watch(actorRef)
                                }
                                actorRef
                            } else
                                Akka.system.actorOf(
                                    SmallestMailboxPool(instanceCount).props(
                                        Props(clazz, resultName, processorEnumeratees, {
                                            // If there are subflows, then we should not send back from the actor, but
                                            // from the subflows instead
                                            if (subflows.isEmpty) sourceActor else None
                                        })),
                                    name = dr.configName.replaceAll("[^a-zA-Z0-9-_.*$+:@&=,!~';]", "_") + "_" + clazz.getName + "_" + idString)
                        }

                        // Dead letter watcher
                        val watcher = Akka.system.actorOf(Props(classOf[DeadLetterWatcher], actorRef), "watcher_" + dr.configName.replaceAll("[^a-zA-Z0-9-_.*$+:@&=,!~';]", "_") + "_" + clazz.getName + "_" + idString)
                        Akka.system.eventStream.subscribe(watcher, classOf[DeadLetter])

                        // Notify the monitor so we can recover from errors
                        Akka.system.actorSelection("user/TuktuMonitor") ! new AppInitPacket(idString, dr.configName, instanceCount, stopOnError, Some(actorRef))
                        // Send all subflows
                        Akka.system.actorSelection("user/TuktuMonitor") ! new SubflowMapPacket(actorRef, subflows)

                        // Send init packet
                        actorRef ! Broadcast(new InitPacket)
                        // Send it the config
                        actorRef ! Broadcast(generatorConfig)

                        // Return reference
                        if (dr.returnRef) sender ! actorRef
                    }
                }
            }
        }
        case Terminated(actor) =>
            // Right now, the only actors we watch, are web mailboxes; remove them from the hostmap
            Cache.getAs[collection.mutable.Map[String, ActorRef]]("web.hostmap") match {
                case None          => play.api.Logger.warn("web.hostmap is not set")
                case Some(hostmap) => for ((id, actorRef) <- hostmap if actor == actorRef) { hostmap -= id }
            }
        case _ => {}
    }
}