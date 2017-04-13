package tuktu.web.processors.analytics

import play.api.libs.iteratee.Enumeratee
import tuktu.api.DataPacket
import play.api.libs.json.JsObject
import tuktu.api.BaseProcessor
import scala.concurrent.Future
import tuktu.api.utils
import tuktu.api.WebJsEventObject
import scala.concurrent.ExecutionContext.Implicits.global
import play.api.Play
import tuktu.api.BaseJsProcessor

class EventCollectorProcessor(resultName: String) extends BaseJsProcessor(resultName) {
    var selector: String = _
    var eventName: String = _
    var flowName: String = _
    var eventValue: String = _

    override def initialize(config: JsObject) {
        selector = (config \ "selector").as[String]
        eventName = (config \ "event_name").as[String]
        flowName = (config \ "flow_name").as[String]
        eventValue = (config \ "event_value").asOpt[String].getOrElse("true")
    }

    override def processor(): Enumeratee[DataPacket, DataPacket] = Enumeratee.mapM((data: DataPacket) => Future {
        for (datum <- data) yield addJsElement(datum, new WebJsEventObject(
            utils.evaluateTuktuString(selector, datum),
            utils.evaluateTuktuString(eventName, datum),
            {
                val flow = utils.evaluateTuktuString(flowName, datum)
                val value = utils.evaluateTuktuString(eventValue, datum)
                "function(){tuktuvars." + resultName + "="+value+";tuktuFlow('" + flow + "');}"
            }
        ))
    })
}