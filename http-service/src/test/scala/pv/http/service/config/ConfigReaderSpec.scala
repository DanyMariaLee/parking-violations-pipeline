package pv.http.service.config

import org.scalatest.{FlatSpec, Matchers}

class ConfigReaderSpec extends FlatSpec with Matchers with ConfigReader {

  behavior of "ConfigReader"

  "readConfig" should "parse configuration correctly" in {

    readConfig.handleErrorWith{ case e =>
      fail(s"Failed to parse pv.view.config: ${e.getMessage}")
    }
  }

}