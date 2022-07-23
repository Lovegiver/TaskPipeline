package com.citizenweb.tooling.taskpipeline.core.utils;

import com.citizenweb.tooling.taskpipeline.core.model.Monitor;
import com.citizenweb.tooling.taskpipeline.core.model.Monitorable;
import com.citizenweb.tooling.taskpipeline.core.model.Pipeline;
import com.citizenweb.tooling.taskpipeline.core.model.WorkGroup;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.util.CollectionUtils;

import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This <b>DTO</b> contains a whole {@link Pipeline} state data.<br>
 * Like a <b>composite</b> object, the PipelineDTO contains the pipeline's {@link Monitor} state but also
 * all other {@link Monitorable} it contains.<br>
 * <ul>
 *     <li>Pipeline contains a collection of {@link WorkGroup}s</li>
 *     <li>Each {@link WorkGroup} contains a collection of {@link com.citizenweb.tooling.taskpipeline.core.model.Task}s</li>
 * </ul>
 * A PipelineDTO contains all fields from the Monitor plus a collection of MonitorDTO.
 */
@Data
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class PipelineDTO {
    @EqualsAndHashCode.Include
    private final String id;
    private final String processingType;
    private final String processingStatus;
    private final String startTime;
    private final String endTime;
    private final long duration;
    private final int rank;
    private final Set<PipelineDTO>  monitorables;

    public PipelineDTO(Monitorable monitorable) {
        Set<PipelineDTO> components = Collections.emptySet();
        Monitor monitor = monitorable.getMonitor();
        this.id = monitor.getId();
        this.processingType = monitor.getType().name();
        this.processingStatus = monitor.getStatus().name();
        this.startTime = monitor.getStartTime() != null ?
                monitor.getStartTime().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) : "";
        this.endTime = monitor.getEndTime() != null ?
                monitor.getEndTime().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) : "";
        this.duration = monitor.getDuration();
        this.rank = monitor.getRank();
        /* Here, we're building the Set<MonitorDTO>  monitorables in a recursive way */
        if (monitorable instanceof Pipeline) {
            var workGroups = ((Pipeline) monitorable).getWorkGroups();
            if (!CollectionUtils.isEmpty(workGroups)) {
                components = workGroups.stream()
                        .map(PipelineDTO::new)
                        .collect(Collectors.toSet());
            }
        }
        if (monitorable instanceof WorkGroup) {
            var tasks = ((WorkGroup) monitorable).getTasks();
            if (!CollectionUtils.isEmpty(tasks)) {
                components = tasks.stream()
                        .map(PipelineDTO::new)
                        .collect(Collectors.toSet());
            }
        }
        this.monitorables = components;
    }
}
