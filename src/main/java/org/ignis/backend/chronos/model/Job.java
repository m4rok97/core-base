/*
 * Copyright (C) 2019 César Pomar
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.ignis.backend.chronos.model;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import lombok.Getter;
import lombok.AllArgsConstructor;
import lombok.Builder;

/**
 *
 * @author César Pomar
 */
@Getter
@Builder
@AllArgsConstructor
public class Job implements Serializable {

    private String schedule;
    private Set<String> parents;

    private String name;
    private String command;
    private long successCount;
    private long errorCount;
    private String executor;
    private String executorFlags;
    private String taskInfoData;
    private int retries;
    private String owner;
    private String ownerName;
    private String description;
    private String lastSuccess;
    private String lastError;
    private double cpus;
    private double disk;
    private double mem;
    private boolean disabled;
    private long errorsSinceLastSuccess;
    private long maxCompletionTime;
    private List<Fetch> fetch;
    private boolean highPriority;
    private String runAsUser;
    private Container container;
    private List<EnvironmentVariable> environmentVariables;
    private boolean shell;
    private List<String> arguments;
    private boolean softError;
    private boolean dataProcessingJobType;
    private List<Constraint> constraints;
    private boolean concurrent;
}
