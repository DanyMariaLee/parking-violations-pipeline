package pv.common.util

import cats.effect.IO
import com.typesafe.scalalogging.Logger

object IOSeqExecution {

  def executeSeq(seqIO: Seq[IO[Unit]])(implicit logger: Logger) =
    seqIO.par.foreach {
      _.handleErrorWith { e =>
        logger.error(e.getMessage)
        IO.raiseError(e)
      }.unsafeRunSync()
    }
}
