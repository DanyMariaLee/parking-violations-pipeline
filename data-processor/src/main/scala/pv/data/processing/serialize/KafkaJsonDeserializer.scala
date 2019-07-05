package pv.data.processing.serialize

import org.apache.kafka.common.serialization.Deserializer
import pv.common.json.JsonProtocols
import pv.common.output.domain.WrappedNYCData
import spray.json._

class KafkaJsonDeserializer extends Deserializer[WrappedNYCData] with JsonProtocols {

  override def deserialize(topic: String, data: Array[Byte]): WrappedNYCData = {
    nycDataFormat.read(new String(data.map(_.toChar)).parseJson)
  }

}
