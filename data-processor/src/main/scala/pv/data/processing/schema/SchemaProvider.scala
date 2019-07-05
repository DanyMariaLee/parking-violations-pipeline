package pv.data.processing.schema

import org.apache.spark.sql.Encoders
import pv.common.output.domain.WrappedNYCData

trait SchemaProvider {

  val schema = Encoders.product[WrappedNYCData].schema
}
