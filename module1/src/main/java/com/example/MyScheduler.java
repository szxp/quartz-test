package com.example;

import io.quarkus.runtime.Startup;
import io.quarkus.scheduler.Scheduled;
import lombok.extern.slf4j.Slf4j;
import org.quartz.*;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.sql.DataSource;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;
import javax.transaction.Transactional;
import java.sql.SQLException;
import java.util.Set;

@Startup
@ApplicationScoped
@Slf4j
public class MyScheduler {

    @Inject
    private Scheduler quartz;

    @Inject
    private DataSource dataSource;

    @Inject
    private TransactionManager transactionManager;

    @Scheduled(every = "10s", identity = "scheduler1", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void execute() throws SchedulerException, InterruptedException {
        log.info("Execute: scheduler1");

        try {
            log.info("conn: {}", dataSource.getConnection());
            log.info("txman status: {}", transactionManager.getStatus());
            if (transactionManager.getTransaction() != null) {
                log.info("txm status: {}", transactionManager.getTransaction().getStatus());
            }
        } catch (SystemException | SQLException e) {
            throw new RuntimeException(e);
        }

        scheduleJob();
    }

    private void scheduleJob() throws SchedulerException {
        String jobName = "myJob";
        String triggerName = "myTrigger";

        JobKey jobKey = JobKey.jobKey(jobName);
        if (quartz.checkExists(jobKey)) {
            log.info("Job already exits: {}", jobKey);
            return;
        }

        JobDetail job = JobBuilder.newJob(MyJob.class)
                .withIdentity(jobName)
                .build();

        Trigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(triggerName)
                .startNow()
                .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                        .withIntervalInSeconds(3)
                        .repeatForever())
                .build();

        quartz.scheduleJob(job, Set.of(trigger), true);
    }

    @Slf4j
    public static class MyJob implements Job {

        @Inject
        private DataSource dataSource;

        @Inject
        private TransactionManager transactionManager;

        @Transactional
        public void execute(JobExecutionContext context) throws JobExecutionException {
            log.info("Execute: job");
            try {
                log.info("conn: {}", dataSource.getConnection());
                log.info("txman status: {}", transactionManager.getStatus());
                if (transactionManager.getTransaction() != null) {
                    log.info("txm status: {}", transactionManager.getTransaction().getStatus());
                }
            } catch (SystemException | SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }


}
