package loadmanager.service;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import common.dto.NewTaskDto;
import common.dto.TaskQueueStatus;
import common.loader.LoaderStatus;
import loadmanager.web.GatewayWebService;
import loadmanager.web.MdbWebService;
import orchschedulerclient.client.OrchSchedulerClientService;
import orchschedulerclient.client.body.LoaderStatusRequestDto;

import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class ManagerService {

    private final MdbWebService mdb;
    private final GatewayWebService gateway;
    private final OrchSchedulerClientService orchScheduler;

    @SneakyThrows
    public Mono<TaskQueueStatus> add(NewTaskDto task) {
        return mdb.add(task);
    }

    public Mono<Boolean> delete(UUID taskId) {
        return mdb.getById(taskId)
                .flatMap(task -> {
                    if (TaskQueueStatus.QUEUED.name().equals(task.getStatus())) {
                        return mdb.delete(taskId)
                                .flatMap(x -> orchScheduler.sendLoaderStatus(taskId, LoaderStatusRequestDto.of(LoaderStatus.LOADING_ERROR)))
                                .then(Mono.just(true));
                    } else {
                        return gateway.stopTask(taskId, task.getOptions().getDatasetId())
                                .flatMap(x -> mdb.delete(taskId));
                    }
                })
                .switchIfEmpty(Mono.error(new RuntimeException("Task " + taskId + " not found")));
    }

}
