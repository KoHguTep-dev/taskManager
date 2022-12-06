package loadmanager.controller;

import lombok.AllArgsConstructor;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;
import common.dto.NewTaskDto;
import common.dto.TaskQueueStatus;
import ru.gnivc.eds.loadmanager.service.ManagerService;

import java.util.UUID;

import static common.uri.GatewayUriConstants.*;


@RestController
@RequestMapping(LOADER)
@AllArgsConstructor
@Validated
public class TaskController {

    private final ManagerService service;


    @PostMapping(TASK_MANAGER_ADD)
    public Mono<TaskQueueStatus> addTask(@RequestBody NewTaskDto task) {
        return service.add(task);
    }

    @DeleteMapping(TASK_MANAGER_REMOVE + "/{taskId}")
    public Mono<Boolean> removeTaskFromQueue(@PathVariable UUID taskId) {
        return service.delete(taskId);
    }

}
