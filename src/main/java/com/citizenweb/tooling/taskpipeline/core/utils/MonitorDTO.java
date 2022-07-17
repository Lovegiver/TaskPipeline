package com.citizenweb.tooling.taskpipeline.core.utils;

import com.citizenweb.tooling.taskpipeline.core.model.Monitor;
import com.citizenweb.tooling.taskpipeline.core.model.Monitorable;
import com.citizenweb.tooling.taskpipeline.core.model.Pipeline;
import com.citizenweb.tooling.taskpipeline.core.model.WorkPath;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.util.CollectionUtils;

import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This <b>DTO</b> contains a whole {@link Pipeline} state data.<br>
 * Like a <b>composite</b> object, the MonitorDTO contains the pipeline's {@link Monitor} state but also
 * all other {@link Monitorable} it contains.<br>
 * <ul>
 *     <li>Pipeline contains a collection of {@link WorkPath}s</li>
 *     <li>Each {@link WorkPath} contains a collection of {@link com.citizenweb.tooling.taskpipeline.core.model.Task}s</li>
 * </ul>
 * A MonitorDTO contains all fields from the Monitor plus a collection of MonitorDTO.
 */
@Data
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class MonitorDTO {
    @EqualsAndHashCode.Include
    private final String id;
    private final String processingType;
    private final String processingStatus;
    private final String startTime;
    private final String endTime;
    private final long duration;
    private final int rank;
    private final Set<MonitorDTO>  monitorables;

    public MonitorDTO(Monitorable monitorable) {
        Set<MonitorDTO> components = Collections.emptySet();
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
            var paths = ((Pipeline) monitorable).getWorkPaths();
            if (!CollectionUtils.isEmpty(paths)) {
                components = ((Pipeline) monitorable).getWorkPaths().stream()
                        .map(MonitorDTO::new)
                        .collect(Collectors.toSet());
            }
        }
        if (monitorable instanceof WorkPath) {
            var tasks = ((WorkPath) monitorable).getTasks();
            if (!CollectionUtils.isEmpty(tasks)) {
                components = ((WorkPath) monitorable).getTasks().stream()
                        .map(MonitorDTO::new)
                        .collect(Collectors.toSet());
            }
        }
        this.monitorables = components;
    }
}
