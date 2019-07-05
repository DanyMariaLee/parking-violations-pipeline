package pv.data.provider.config

import org.scalatest.{FlatSpec, Matchers}

class ConfigReaderSpec extends FlatSpec with Matchers with ConfigReader {

  behavior of "ConfigReader"

  "readConfig" should "parse configuration correctly" in {

    readConfig.handleErrorWith{ case e =>
      fail(s"Failed to parse config: ${e.getMessage}")
    }
  }

}
