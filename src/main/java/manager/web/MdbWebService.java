package loadmanager.web;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import common.dto.NewTaskDto;
import common.dto.TaskQueueDto;
import common.dto.TaskQueueStatus;
import webclientlib.WebClientConfiguration;
import webclientlib.WebClientService;

import java.time.Duration;
import java.util.UUID;

import static common.uri.GatewayUriConstants.TASK_QUEUE;
import static common.uri.GatewayUriConstants.MDB_API;
import static webclientlib.ClientServiceName.MDB;

@Service
@RequiredArgsConstructor
@Slf4j
public class MdbWebService {

    private final WebClientService mdbWebClient;
    private final WebClientConfiguration config;

    public Mono<TaskQueueStatus> add(NewTaskDto task) {
        log.debug("Add new task: {}", task);
        return mdbWebClient
                .get(MDB)
                .post()
                .uri(MDB_API + TASK_QUEUE)
                .body(Mono.just(task), NewTaskDto.class)
                .exchangeToMono(clientResponse -> clientResponse.statusCode().is2xxSuccessful() ?
                        clientResponse.bodyToMono(TaskQueueStatus.class) :
                        Mono.error(new RuntimeException("Error adding new task"))
                ).retryWhen(Retry.fixedDelay(config.getMaxAttempts(), Duration.ofSeconds(config.getDelayInSeconds())));
    }

    public Mono<Boolean> delete(UUID taskId) {
        log.debug("Delete task: {}", taskId);
        return mdbWebClient
                .get(MDB)
                .delete()
                .uri(uriBuilder -> uriBuilder
                        .path(MDB_API + TASK_QUEUE)
                        .pathSegment(taskId.toString())
                        .build()
                )
                .exchangeToMono(clientResponse -> clientResponse.statusCode().is2xxSuccessful() ?
                        clientResponse.bodyToMono(Boolean.class) :
                        Mono.error(new RuntimeException("Error deleting task"))
                ).retryWhen(Retry.fixedDelay(config.getMaxAttempts(), Duration.ofSeconds(config.getDelayInSeconds())));
    }

    public Mono<TaskQueueDto> getById(UUID taskId) {
        log.debug("Get task: {}", taskId);
        return mdbWebClient
                .get(MDB)
                .get()
                .uri(uriBuilder -> uriBuilder
                        .path(MDB_API + TASK_QUEUE)
                        .pathSegment(taskId.toString())
                        .build()
                )
                .exchangeToMono(clientResponse -> clientResponse.statusCode().is2xxSuccessful() ?
                        clientResponse.bodyToMono(TaskQueueDto.class) :
                        Mono.error(new RuntimeException("Error getting task"))
                ).retryWhen(Retry.fixedDelay(config.getMaxAttempts(), Duration.ofSeconds(config.getDelayInSeconds())));
    }

    public Mono<TaskQueueDto> findByStatus(TaskQueueStatus status) {
        log.debug("Find by task status: {}", status);
        return mdbWebClient
                .get(MDB)
                .get()
                .uri(uriBuilder -> uriBuilder
                        .path(MDB_API + TASK_QUEUE + "/find")
                        .queryParam("status", status.name())
                        .build()
                )
                .exchangeToMono(clientResponse -> clientResponse.statusCode().is2xxSuccessful() ?
                        clientResponse.bodyToMono(TaskQueueDto.class) :
                        Mono.error(new RuntimeException("Error finding task"))
                ).retryWhen(Retry.fixedDelay(config.getMaxAttempts(), Duration.ofSeconds(config.getDelayInSeconds())));
    }

    public Mono<TaskQueueDto> findNext() {
        log.debug("Take the earliest task in the queue");
        return mdbWebClient
                .get(MDB)
                .get()
                .uri(MDB_API + TASK_QUEUE)
                .exchangeToMono(clientResponse -> clientResponse.statusCode().is2xxSuccessful() ?
                        clientResponse.bodyToMono(TaskQueueDto.class) :
                        Mono.error(new RuntimeException("Error getting one task"))
                ).retryWhen(Retry.fixedDelay(config.getMaxAttempts(), Duration.ofSeconds(config.getDelayInSeconds())));
    }

    public Mono<TaskQueueDto> update(TaskQueueDto task) {
        log.debug("Update task: {}", task.getTaskId());
        return mdbWebClient
                .get(MDB)
                .patch()
                .uri(MDB_API + TASK_QUEUE)
                .body(Mono.just(task), TaskQueueDto.class)
                .exchangeToMono(clientResponse -> clientResponse.statusCode().is2xxSuccessful() ?
                        clientResponse.bodyToMono(TaskQueueDto.class) :
                        Mono.error(new RuntimeException("Error updating task"))
                ).retryWhen(Retry.fixedDelay(config.getMaxAttempts(), Duration.ofSeconds(config.getDelayInSeconds())));
    }

}
