package pv.aggregation

import pv.aggregation.scheduler.AggregationTrigger

object AggregateApp extends App
  with AggregationProcess
  with AggregationTrigger {

  scheduler.start()
  scheduler.scheduleJob(sparkJob, trigger)
}
