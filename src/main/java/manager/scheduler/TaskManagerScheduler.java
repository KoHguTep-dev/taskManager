package loadmanager.scheduler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import common.dto.NewTaskDto;
import common.dto.TaskQueueDto;
import common.dto.TaskQueueStatus;
import common.loader.LoaderStatus;
import loadmanager.web.GatewayWebService;
import loadmanager.web.MdbWebService;
import orchschedulerclient.client.OrchSchedulerClientService;
import orchschedulerclient.client.body.LoaderStatusRequestDto;

import javax.annotation.PostConstruct;
import java.time.OffsetDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class TaskManagerScheduler {
    private final GatewayWebService gateway;
    private final MdbWebService mdb;
    private final OrchSchedulerClientService orchScheduler;


    @Value(value = "${schedule.manager.enabled}")
    private boolean enabled;

    /**
     * Время ожидания, По его истечению, если задача не принята (статус LOADING) - задача считается зависшей и отменяется
     */
    @Value(value = "${schedule.manager.waiting}")
    private int waiting = 60_000;

    @PostConstruct
    private void init() {
        log.info("{} {}", this.getClass().getSimpleName(), this.enabled ? "enabled" : "disabled");
        log.debug("Elapsing time for task set {} second", waiting / 1000);
    }

    @Scheduled(cron = "${schedule.manager.cron}")
    public void gutQueue() {
        log.debug("Checking a task from the queue");
        mdb.findByStatus(TaskQueueStatus.PENDING)
                .switchIfEmpty(mdb.findByStatus(TaskQueueStatus.ACTIVE))
                .switchIfEmpty(startNew())
                .flatMap(this::checkStatus)
                .subscribe();
    }

    private Mono<TaskQueueDto> startNew() {
        return mdb.findNext()
                .switchIfEmpty(Mono.error(new RuntimeException()))
                .onErrorResume(ex -> {
                    log.debug("Task queue is empty");
                    return Mono.empty();
                })
                .flatMap(task -> {
                    log.debug("Send the task {} to work", task.getTaskId());
                    NewTaskDto newTaskDto = task.getOptions();
                    newTaskDto.setTaskId(task.getTaskId());

                    return gateway.startTask(newTaskDto)
                            .onErrorResume(e ->  {
                                log.error("Task {} failed", task.getTaskId());
                                return mdb.delete(task.getTaskId());
                            })
                            .flatMap(x -> {
                                log.debug("Task {} status is PENDING", task.getTaskId());
                                task.setStatus(TaskQueueStatus.PENDING.name());
                                task.setUpdatedAt(OffsetDateTime.now());
                                return mdb.update(task);
                            });
                });
    }

    private Mono<Boolean> checkStatus(TaskQueueDto activeTask) {
        log.debug("Check status for task: {}", activeTask.getTaskId());

        return gateway.getTaskStatus(activeTask.getTaskId(), activeTask.getOptions().getDatasetId())
                .flatMap(response -> {
                    var endTime = activeTask.getUpdatedAt().plusSeconds(waiting / 1000);
                    log.debug("Loader status task {} is {}", response.getTaskId(), response.getStatus());
                    switch (response.getStatus()) {
                        case NOT_STARTED:
                            if (endTime.isBefore(OffsetDateTime.now())) {
                                log.error("Task loading time elapsed: {}", activeTask.getTaskId());
                                return mdb.delete(activeTask.getTaskId())
                                        .flatMap(x -> orchScheduler.sendLoaderStatus(activeTask.getTaskId(), LoaderStatusRequestDto.of(LoaderStatus.LOADING_ERROR)))
                                        .then(Mono.just(true));
                            } else {
                                return Mono.just(true);
                            }

                        case LOADING:
                            if (endTime.isBefore(OffsetDateTime.now())) {
                                log.error("Task loading time elapsed: {}", activeTask.getTaskId());
                                return mdb.delete(activeTask.getTaskId())
                                        .flatMap(x -> orchScheduler.sendLoaderStatus(activeTask.getTaskId(), LoaderStatusRequestDto.of(LoaderStatus.LOADING_ERROR)))
                                        .then(Mono.just(true));
                            } else {
                                log.debug("Task {} status is ACTIVE", activeTask.getTaskId());
                                if (activeTask.getStatus().equals("PENDING")) {
                                    activeTask.setStatus(TaskQueueStatus.ACTIVE.name());
                                }
                                activeTask.setUpdatedAt(OffsetDateTime.now());
                                return mdb.update(activeTask)
                                        .map(x -> true);
                            }

                        case LOADING_ERROR:
                        case ACCEPTING_DECLINED:
                            log.error("Task loading error: {}", activeTask.getTaskId());
                            return mdb.delete(activeTask.getTaskId())
                                    .flatMap(x -> orchScheduler.sendLoaderStatus(activeTask.getTaskId(), LoaderStatusRequestDto.of(LoaderStatus.LOADING_ERROR)))
                                    .then(Mono.just(true));

                        case ACCEPTING_COMPLETE:
                        case LOADING_COMPLETE:
                            log.info("Task completed: {}", activeTask.getTaskId());
                            return mdb.delete(activeTask.getTaskId());
                    }
                    return Mono.just(false);
                })
                .onErrorResume(e ->  {
                    log.error("Task {} failed", activeTask.getTaskId());
                    return mdb.delete(activeTask.getTaskId());
                });
    }

}
