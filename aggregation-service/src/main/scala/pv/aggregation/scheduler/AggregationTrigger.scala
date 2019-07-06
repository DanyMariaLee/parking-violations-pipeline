package pv.aggregation.scheduler

import org.quartz.TriggerBuilder.newTrigger
import org.quartz.impl.StdSchedulerFactory
import org.quartz.{CronScheduleBuilder, JobBuilder}

trait AggregationTrigger {

  val trigger = newTrigger()
    .withIdentity("aggregationTrigger", "sparkJobsGroup")
    .withSchedule(
      CronScheduleBuilder.cronSchedule("0 0/10 * 1/1 * ? *"))
    .build()

  val sparkJob = JobBuilder.newJob(classOf[ScheduledAggregation])
    .withIdentity("ScheduledAggregation", "sparkJobsGroup")
    .build()

  val scheduler = new StdSchedulerFactory().getScheduler()
}
