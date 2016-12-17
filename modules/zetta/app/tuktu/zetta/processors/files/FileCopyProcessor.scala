package tuktu.zetta.processors.files

import java.io.File
import org.apache.commons.io.FileUtils
import play.api.libs.json.JsObject
import play.api.libs.iteratee.Enumeratee
import tuktu.api.{ BaseProcessor, DataPacket, utils }
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Copy a file into another.
 */
class FileCopyProcessor(resultName: String) extends BaseProcessor(resultName) {
    var from: String = _
    var to: String = _

    override def initialize(config: JsObject) {
        from = (config \ "from").as[String]
        to = (config \ "to").as[String]
    }

    override def processor(): Enumeratee[DataPacket, DataPacket] = Enumeratee.mapM(data => Future {
        for (datum <- data) {
        	val srcFile: File = new File( utils.evaluateTuktuString( from, datum ) )
        	val destFile: File = new File( utils.evaluateTuktuString( to, datum ) )
        	FileUtils.copyFile( srcFile, destFile )    
        }
        data
    })
}