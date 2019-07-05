package pv.aggregation.scheduler

import org.quartz._
import pv.aggregation.AggregationProcess
import pv.common.util.IOSeqExecution.executeSeq

class ScheduledAggregation
  extends Job
    with AggregationProcess {

  override def execute(context: JobExecutionContext): Unit = {
    executeSeq(aggregate(config))
  }
}