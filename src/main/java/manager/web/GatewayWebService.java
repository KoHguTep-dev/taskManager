package loadmanager.web;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import common.dto.NewTaskDto;
import common.dto.TaskQueueDto;
import common.dto.response.LoaderResponseBody;
import common.loader.LoaderStatus;
import orchschedulerclient.client.body.LoaderStatusRequestDto;
import webclientlib.WebClientConfiguration;
import webclientlib.WebClientService;

import java.time.Duration;
import java.util.UUID;

import static common.uri.GatewayUriConstants.*;
import static webclientlib.ClientServiceName.GATEWAY;


@Service
@RequiredArgsConstructor
@Slf4j
public class GatewayWebService {
    private final WebClientService webService;
    private final WebClientConfiguration config;


    public Mono<Boolean> startTask(NewTaskDto newTask) {
        return webService.get(GATEWAY)
                .post()
                .uri(LOADER + LOADER_START)
                .body(Mono.just(newTask), TaskQueueDto.class)
                .exchangeToMono(response -> {
                    if (response.statusCode().is2xxSuccessful()) {
                        return Mono.just(true);
                    }
                    log.error("Error when starting task: {}", newTask.getTaskId().toString());
                    return Mono.error(new RuntimeException("Error when starting task: " + newTask.getTaskId().toString()));
                }).retryWhen(Retry.fixedDelay(config.getMaxAttempts(), Duration.ofSeconds(config.getDelayInSeconds())));
    }

    public Mono<LoaderResponseBody> getTaskStatus(UUID taskId, UUID datasetId) {
        return webService.get(GATEWAY)
                .get()
                .uri(uriBuilder -> uriBuilder
                        .path(LOADER + LOADER_STATUS)
                        .queryParam("taskId", taskId)
                        .queryParam("datasetId", datasetId)
                        .build())
                .exchangeToMono(response -> {
                    if (response.statusCode().is2xxSuccessful()) {
                        return response.bodyToMono(LoaderResponseBody.class);
                    }
                    log.error("Error getting task status: {}", taskId);
                    LoaderResponseBody body = new LoaderResponseBody();
                    body.setTaskId(taskId);
                    body.setDatasetId(datasetId);
                    body.setStatus(LoaderStatus.LOADING_ERROR);
                    return Mono.just(body);
                }).retryWhen(Retry.fixedDelay(config.getMaxAttempts(), Duration.ofSeconds(config.getDelayInSeconds())));
    }

    public Mono<Boolean> stopTask(UUID taskId, UUID datasetId) {
        return webService.get(GATEWAY)
                .patch()
                .uri(uriBuilder -> uriBuilder
                        .path(LOADER + LOADER_STOP)
                        .queryParam("taskId", taskId)
                        .queryParam("datasetId", datasetId)
                        .build(taskId)
                )
                .exchangeToMono(response -> {
                    if (response.statusCode().is2xxSuccessful()) {
                        return response.bodyToMono(Boolean.class);
                    }
                    log.error("Error getting task status: {}", taskId);
                    return Mono.error(new RuntimeException("Error getting task status: " + taskId));
                }).retryWhen(Retry.fixedDelay(config.getMaxAttempts(), Duration.ofSeconds(config.getDelayInSeconds())));
    }

    public Mono<Boolean> sendStatusToOrchScheduler(UUID taskId, LoaderStatus status) {
        var loaderStatusRequestDto = LoaderStatusRequestDto.of(status);

        return webService.get(GATEWAY)
                .put()
                .uri(SCHEDULER + "/" + taskId + SCHEDULER_STATUS)
                .body(Mono.just(loaderStatusRequestDto), LoaderStatusRequestDto.class)
                .exchangeToMono(response -> {
                    if (response.statusCode().is2xxSuccessful()) {
                        return Mono.just(true);
                    }
                    return Mono.error(new RuntimeException("Error while sending loader status"));
                }).retryWhen(Retry.fixedDelay(config.getMaxAttempts(), Duration.ofSeconds(config.getDelayInSeconds())));
    }
}
