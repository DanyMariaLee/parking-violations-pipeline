package pv.http.service.serialize

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Serializer
import pv.common.json.JsonProtocols
import pv.common.output.domain.WrappedNYCData

class KafkaJsonSerializer extends Serializer[WrappedNYCData] with JsonProtocols {

  override def serialize(topic: String, data: WrappedNYCData): Array[Byte] = {
    val objectMapper = new ObjectMapper()
    objectMapper.writeValueAsBytes(data)
    nycDataFormat.write(data).toString.map(_.toByte).toArray
  }

}
