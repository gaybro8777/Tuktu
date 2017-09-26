package tuktu.nosql.generators

import akka.actor.ActorRef
import play.api.libs.iteratee.{ Enumeratee, Enumerator }
import play.api.libs.json.{ Json, JsObject, JsValue }
import play.api.Play.current
import play.modules.reactivemongo.json.{ JsObjectDocumentWriter, JSONSerializationPack }
import play.modules.reactivemongo.json.collection.JSONCollection
import reactivemongo.api.commands.Command
import reactivemongo.api.{ FailoverStrategy, MongoConnection, MongoConnectionOptions, MongoDriver, QueryOpts, ReadPreference, ScramSha1Authentication }
import reactivemongo.core.nodeset.Authenticate
import scala.concurrent.duration.DurationInt
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import tuktu.api.{ BaseGenerator, DataPacket, InitPacket, StopPacket }
import tuktu.nosql.util._
import play.api.Logger
import tuktu.api.utils

/**
 * Generator for MongoDB aggregations
 */
class MongoDBAggregateGenerator(resultName: String, processors: List[Enumeratee[DataPacket, DataPacket]], senderActor: Option[ActorRef]) extends BaseGenerator(resultName, processors, senderActor) {
    var conn: MongoConnection = _
    var nodes: List[String] = _

    override def _receive = {
        case config: JsValue => {
            // Get hosts
            nodes = (config \ "hosts").as[List[String]]
            // Get connection properties
            val opts = (config \ "mongo_options").asOpt[JsObject]
            val mongoOptions = MongoPool.parseMongoOptions(opts)
            // Get credentials
            val auth = (config \ "auth").asOpt[JsObject].map { a =>
                Authenticate(
                    (a \ "db").as[String],
                    (a \ "user").as[String],
                    (a \ "password").as[String])
            }

            // Get the connection
            val fConnection = MongoPool.getConnection(nodes, mongoOptions, auth)

            // Get tasks
            val tasks = (config \ "tasks").as[List[JsObject]]

            // Batch all the results before pushing it on the channel
            val batch = (config \ "batch").asOpt[Boolean].getOrElse(false)

            // prepare aggregation pipeline
            val resultFuture = fConnection.flatMap { connection =>
                conn = connection
                val fCollection = MongoPool.getCollection(connection, (config \ "db").as[String], (config \ "collection").as[String])
                fCollection.onFailure {
                    case _ => self ! new StopPacket
                }
                fCollection.flatMap { collection: JSONCollection =>
                    // Prepare aggregation pipeline
                    val pipeline = tasks.map { task => MongoPipelineTransformer.json2task(task)(collection) }
                    // Get data based on the aggregation pipeline
                    val resultData: Future[List[JsObject]] = collection.aggregate(pipeline.head, pipeline.tail).map(_.head[JsObject])
                    resultData.onFailure {
                        case _ => self ! new StopPacket
                    }
                    // Get futures into JSON
                    resultData.map { resultList =>
                        for (resultRow <- resultList) yield tuktu.api.utils.JsObjectToMap(resultRow)
                    }
                }
            }

            // Handle results
            resultFuture.onSuccess {
                case res: List[Map[String, Any]] => {
                    // Determine what to do based on batch or non batch
                    if (batch)
                        channel.push(DataPacket(res))
                    else
                        res.foreach(row => channel.push(DataPacket(List(row))))
                    self ! new StopPacket()
                }
                case _ => self ! new StopPacket()
            }
            resultFuture.onFailure {
                case e: Throwable => {
                    e.printStackTrace
                    self ! new StopPacket
                }
            }
        }
        case sp: StopPacket => {
            if (conn != null)
                MongoPool.releaseConnection(nodes, conn)
            cleanup
        }
    }
}

/**
 * Generator for MongoDB finds
 */
class MongoDBFindGenerator(resultName: String, processors: List[Enumeratee[DataPacket, DataPacket]], senderActor: Option[ActorRef]) extends BaseGenerator(resultName, processors, senderActor) {
    var conn: MongoConnection = _
    var nodes: List[String] = _

    override def _receive = {
        case config: JsValue => {
            // Get hosts
            nodes = (config \ "hosts").as[List[String]]
            // Get connection properties
            val opts = (config \ "mongo_options").asOpt[JsObject]
            val mongoOptions = MongoPool.parseMongoOptions(opts)
            // Get credentials
            val auth = (config \ "auth").asOpt[JsObject].map { a =>
                Authenticate(
                    (a \ "db").as[String],
                    (a \ "user").as[String],
                    (a \ "password").as[String])
            }

            // Get the connection
            val fConnection = MongoPool.getConnection(nodes, mongoOptions, auth)

            // Get query and filter
            val query = (config \ "query").as[JsObject]
            val filter = (config \ "filter").asOpt[JsObject].getOrElse(Json.obj())
            val sort = (config \ "sort").asOpt[JsObject].getOrElse(Json.obj())

            // Continue when we have a connection set up
            fConnection.map { connection =>
                conn = connection
                val fCollection = MongoPool.getCollection(connection, (config \ "db").as[String], (config \ "collection").as[String])
                fCollection.onSuccess {
                    case collection: JSONCollection => {
                        val enumerator = collection.find(query, filter)
                            .sort(sort).cursor[JsObject](ReadPreference.nearest)
                            .enumerate().andThen(Enumerator.eof)
                        // Transformator to turn the JsObjects into DataPackets
                        val transformator: Enumeratee[JsObject, DataPacket] = Enumeratee.mapM(record => Future { DataPacket(List(tuktu.api.utils.JsObjectToMap(record))) })
                        // onEOF close the reader and send StopPacket
                        val onEOF = Enumeratee.onEOF[DataPacket](() => self ! new StopPacket)

                        // Chain this together
                        processors.foreach(processor => {
                            enumerator |>> (transformator compose onEOF compose processor) &>> sinkIteratee
                        })
                    }
                    case _ => self ! new StopPacket
                }
                fCollection.onFailure {
                    case _ => self ! new StopPacket
                }
            }
        }
        case sp: StopPacket => {
            if (conn != null)
                MongoPool.releaseConnection(nodes, conn)
            cleanup(false)
        }
    }
}

/**
 * A Generator to list the collections in a database (requires MongoDB 3.0 or higher)
 */
class MongoDBCollectionsGenerator(resultName: String, processors: List[Enumeratee[DataPacket, DataPacket]], senderActor: Option[ActorRef]) extends BaseGenerator(resultName, processors, senderActor) {
    var conn: MongoConnection = _
    var nodes: List[String] = _

    override def _receive = {
        case config: JsValue => {
            // Get hosts
            nodes = (config \ "hosts").as[List[String]]
            // Get connection properties
            val opts = (config \ "mongo_options").asOpt[JsObject]
            val mongoOptions = MongoPool.parseMongoOptions(opts)
            // Get credentials
            val auth = (config \ "auth").asOpt[JsObject].map { a =>
                Authenticate(
                    (a \ "db").as[String],
                    (a \ "user").as[String],
                    (a \ "password").as[String])
            }

            // Get the connection
            val fConnection = MongoPool.getConnection(nodes, mongoOptions, auth)
            fConnection.map { connection =>
                conn = connection
                // Get DB
                val fDb = connection.database((config \ "db").as[String])
                fDb.onFailure {
                    case _ => self ! new StopPacket
                }
                fDb.map { db =>
                    // Get command
                    val command = Json.obj("listCollections" -> 1)
                    val runner = Command.run(JSONSerializationPack)
                    // Run it
                    val futureResult = runner(db, runner.rawCommand(command)).one[JsObject]

                    val futureCollections = futureResult.map { result => (result \\ "name").map { coll => coll.as[String] } }
                    futureCollections.onSuccess {
                        case collections: List[String] => {
                            collections.foreach { collection => channel.push(DataPacket(List(Map(resultName -> collection)))) }
                            self ! new StopPacket
                        }
                        case _ => self ! new StopPacket
                    }
                    futureCollections.onFailure {
                        case e: Throwable => {
                            e.printStackTrace
                            self ! new StopPacket
                        }
                    }
                }
            }
        }
        case sp: StopPacket => {
            if (conn != null)
                MongoPool.releaseConnection(nodes, conn)
            cleanup
        }
    }
}

/**
 * A Generator to run a raw database command
 */
class MongoDBCommandGenerator(resultName: String, processors: List[Enumeratee[DataPacket, DataPacket]], senderActor: Option[ActorRef]) extends BaseGenerator(resultName, processors, senderActor) {
    var conn: MongoConnection = _
    var nodes: List[String] = _

    override def _receive = {
        case config: JsValue => {
            // Get hosts
            nodes = (config \ "hosts").as[List[String]]
            // Get connection properties
            val opts = (config \ "mongo_options").asOpt[JsObject]
            val mongoOptions = MongoPool.parseMongoOptions(opts)
            // Get credentials
            val auth = (config \ "auth").asOpt[JsObject].map { a =>
                Authenticate(
                    (a \ "db").as[String],
                    (a \ "user").as[String],
                    (a \ "password").as[String])
            }
            val flatten = (config \ "flatten").asOpt[Boolean].getOrElse(false)

            // Get the connection
            val fConnection = MongoPool.getConnection(nodes, mongoOptions, auth)
            fConnection.map { connection =>
                conn = connection
                // Get DB
                val fDb = connection.database((config \ "db").as[String])
                fDb.onFailure {
                    case _ => self ! new StopPacket
                }
                fDb.map { db =>
                    // Get command
                    val command = (config \ "command").as[JsObject]
                    val runner = Command.run(JSONSerializationPack)
                    // Run it
                    val futureResult = runner(db, runner.rawCommand(command)).one[JsObject]
                    futureResult.onSuccess {
                        case o: JsObject => {
                            // Get the result code
                            if (o.keys.contains("ok") && (o \ "ok").as[Double] == 1.0) {
                                // Get the resulting data
                                (o \ "result").as[List[JsObject]].foreach {datum =>
                                    channel.push(new DataPacket(List(
                                            if (flatten) utils.JsObjectToMap(datum)
                                            else Map(resultName -> datum)
                                    )))
                                }
                                // We are done
                                self ! new StopPacket
                            } else {
                                Logger.warn("MongoDB Command Generator got unexpected response: " + o)
                                self ! new StopPacket
                            }
                        }
                        case a: Any => {
                            Logger.warn("MongoDB Command Generator got unexpected response: " + a)
                            self ! new StopPacket
                        }
                    }
                    futureResult.onFailure {
                        case e: Throwable => {
                            Logger.error(e.getMessage)
                            self ! new StopPacket
                        }
                    }
                }
            }
        }
        case sp: StopPacket => {
            if (conn != null)
                MongoPool.releaseConnection(nodes, conn)
            cleanup
        }
    }
}