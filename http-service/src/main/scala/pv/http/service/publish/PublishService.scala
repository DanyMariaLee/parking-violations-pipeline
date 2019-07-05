package pv.http.service.publish

import akka.Done
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.producer.ProducerRecord
import pv.common.output.domain.WrappedNYCData

import scala.concurrent.Future

trait PublishService {

  def publish(nycData: WrappedNYCData, topic: String
             )(implicit
               settings: ProducerSettings[String, WrappedNYCData],
               mat: ActorMaterializer
             ): Future[Done] = {
    Source.single(nycData)
      .map(value => {
        new ProducerRecord[String, WrappedNYCData](
          topic, nycData.plateID, value
        )
      })
      .runWith(Producer.plainSink[String, WrappedNYCData](settings))
  }
}
