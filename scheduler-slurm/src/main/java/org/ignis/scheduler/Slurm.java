/*
 * Copyright (C) 2022 César Pomar
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
package org.ignis.scheduler;

import org.ignis.scheduler.model.*;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.IntStream;

/**
 * @author César Pomar
 */
public class Slurm implements IScheduler {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Slurm.class);

    public static final String NAME = "slurm";

    private boolean executorContainers;
    private final String cmd;

    public Slurm(String url) {
        cmd = url;
        executorContainers = false;
    }

    private void run(List<String> args, String script) throws ISchedulerException {
        List<String> cmdArgs = new ArrayList<>();
        cmdArgs.add(cmd);
        cmdArgs.addAll(args);
        ProcessBuilder builder = new ProcessBuilder(cmdArgs);
        try {
            builder.inheritIO().redirectInput(ProcessBuilder.Redirect.PIPE);
            Process slurm = builder.start();
            slurm.getOutputStream().write(script.getBytes(StandardCharsets.UTF_8));
            slurm.getOutputStream().close();
            int exitCode = slurm.waitFor();
            if (exitCode != 0) {
                throw new ISchedulerException("slurm returns exit code != 0, (" + exitCode + ")");
            }
        } catch (IOException e) {
            throw new ISchedulerException("slurm error", e);
        } catch (InterruptedException ignored) {
        }
    }

    private String esc(String v) {
        v = v.replace("\n", "\\n");
        if (v.contains(" ")) {
            return "\"" + v + "\"";
        }
        return v;
    }

    private void parseSingulauryContainerArgs(StringBuilder script, IContainerInfo c) throws ISchedulerException {
        script.append("singularity exec");
        script.append(" --writable-tmpfs --pid --cleanenv");
        for (IBind bind : c.getBinds()) {
            script.append(" --bind \"").append(bind.getHostPath()).append(":").append(bind.getContainerPath());
            script.append(":").append(bind.isReadOnly() ? "ro" : "rw").append("\"");
        }

        for (Map.Entry<String, String> entry : c.getEnvironmentVariables().entrySet()) {
            script.append(" --env ").append(esc(entry.getKey() + "=" + entry.getValue()));
        }
        script.append(" --env IGNIS_JOB_ID=${IGNIS_JOB_ID}");
        script.append(" --env IGNIS_JOB_NAME=${IGNIS_JOB_NAME}");
        script.append(" --env SCHEDULER_PATH=${SCHEDULER_PATH}");

        script.append(' ').append(c.getImage()).append(' ').append(c.getCommand());
        for (String arg : c.getArguments()) {
            script.append(' ').append(esc(arg));
        }
        script.append('\n');
    }

    private void parseUdockerContainerArgs(StringBuilder script, IContainerInfo c) throws ISchedulerException {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    private void parseContainerArgs(StringBuilder script, IContainerInfo c, String wd) throws ISchedulerException {
        String flagPort = c.getSchedulerParams().get("port");
        script.append("srun");
        if (flagPort == null) {
            script.append(" --resv-ports=").append(c.getPorts().size());
        }
        script.append(" bash - <<'EOF'").append("\n");
        script.append("#!/bin/bash").append("\n\n");

        for (IBind b : c.getBinds()) {
            if (b.getHostPath().equals(wd)) {
                script.append("export SCHEDULER_PATH='").append(b.getContainerPath()).append("'\n");
                break;
            }
        }

        if (flagPort != null) {
            int initPort = Integer.parseInt(flagPort);
            int endPort = initPort + c.getPorts().size();
            script.append("export SLURM_STEP_RESV_PORTS=").append(initPort).append("-").append(endPort).append('\n');
        }

        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(c);
            script.append("export CONTAINER_INFO=").append(new String(Base64.getEncoder().encode(bos.toByteArray()))).append("\n");
        } catch (IOException e) {
            throw new ISchedulerException("IO error", e);
        }

        String jobId = "${SLURM_PROCID}";
        String groupId = "${SLURM_JOB_NAME}-${SLURM_JOB_ID}";

        script.append("export IGNIS_JOB_ID=").append(jobId).append("\n");
        script.append("export IGNIS_JOB_NAME=").append(groupId).append("\n");

        String wd2 = wd.endsWith("/") ? wd : wd + "/";
        script.append("mkdir -p ").append(wd2).append("${IGNIS_JOB_NAME}\n");
        script.append("env --null > ").append(wd2).append("${IGNIS_JOB_NAME}/${SLURM_PROCID}.env\n");
        script.append("echo 1 > ").append(wd2).append("${IGNIS_JOB_NAME}/${SLURM_PROCID}.ok\n");

        String platform = c.getSchedulerParams().get("platform");
        if (platform == null) {
            throw new ISchedulerException("ignis.scheduler.param.platform not defined");
        }

        if (platform.equalsIgnoreCase("udocker")) {
            parseUdockerContainerArgs(script, c);
        } else if (platform.equalsIgnoreCase("singularity")) {
            parseSingulauryContainerArgs(script, c);
        } else {
            throw new ISchedulerException("ignis.scheduler.param.platform=" + platform + " is not valid");
        }

        script.append("EOF").append("\n");
    }

    private void parseSlurmArgs(StringBuilder script, IContainerInfo c, int instances) throws ISchedulerException {
        script.append("#SBATCH --nodes=").append(instances).append('\n');
        script.append("#SBATCH --cpus-per-task=").append(c.getCpus()).append('\n');
        script.append("#SBATCH --mem=").append(c.getMemory() / 1000 / 1000).append('\n');
    }

    public void createJob(String time, String name, String args, String wd, IContainerInfo driver, IContainerInfo executors, int instances) throws ISchedulerException {
        StringBuilder script = new StringBuilder();
        script.append("#!/bin/bash").append('\n');
        script.append("#SBATCH --job-name=").append(name).append('\n');
        script.append("#SBATCH --time=").append(time).append('\n');
        parseSlurmArgs(script, driver, 1);
        if (!args.isEmpty()) {
            script.append("#SBATCH ").append(args).append('\n');
        }
        parseContainerArgs(script, driver, wd);
        script.append("#SBATCH hetjob").append('\n');
        parseSlurmArgs(script, executors, instances);
        if (!args.isEmpty()) {
            script.append("#SBATCH ").append(args).append('\n');
        }
        parseContainerArgs(script, executors, wd);
        if (Boolean.getBoolean("ignis.debug")) {
            LOGGER.info("Debug: slurm script{ \n    " + script.toString().replace("\n", "\n    ") + "\n}\n");
        }
        run(List.of(), script.toString());
    }

    private IContainerInfo parseContainerInfo(String id) throws ISchedulerException {
        String path = System.getenv("SCHEDULER_PATH");
        String groupId = System.getenv("IGNIS_JOB_NAME");
        File jobFolder = new File(path, groupId);
        String envText;
        try {
            File ok = new File(jobFolder, id + ".ok");
            if (!ok.exists()) {
                LOGGER.warn(ok.getPath() + " not found, waiting 30s before continue");
                try {
                    Thread.sleep(30000);
                } catch (InterruptedException e) {
                }
            }
            envText = Files.readString(new File(jobFolder, id + ".env").toPath(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new ISchedulerException(e.getMessage(), e);
        }
        Map<String, String> env = new HashMap<>();
        for (String var : envText.split("\0")) {
            int sep = var.indexOf('=');
            env.put(var.substring(0, sep), var.substring(sep + 1));

        }

        IContainerInfo request;
        try (ByteArrayInputStream bis = new ByteArrayInputStream(Base64.getDecoder().decode(env.get("CONTAINER_INFO")));
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            request = (IContainerInfo) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new ISchedulerException("IO error", e);
        }
        IContainerInfo.IContainerInfoBuilder builder = IContainerInfo.builder();
        builder.id(id);
        builder.host(env.get("SLURMD_NODENAME"));
        builder.image(request.getImage());
        builder.command(request.getCommand());
        builder.arguments(request.getArguments());
        builder.cpus(request.getCpus());
        builder.memory(request.getMemory());
        builder.swappiness(request.getSwappiness());
        int initPort = Integer.parseInt(env.get("SLURM_STEP_RESV_PORTS").split("-")[0]);
        List<IPort> ports = new ArrayList<>();
        for (IPort p : request.getPorts()) {
            int n = initPort + ports.size();
            ports.add(new IPort(n, n, p.getProtocol()));
        }
        builder.ports(ports);
        builder.networkMode(INetworkMode.HOST);
        builder.binds(request.getBinds());
        builder.volumes(request.getVolumes());
        builder.preferedHosts(request.getPreferedHosts());
        builder.hostnames(request.getHostnames());
        builder.environmentVariables(request.getEnvironmentVariables());
        builder.schedulerParams(request.getSchedulerParams());
        return builder.build();
    }

    @Override
    public String createGroup(String name) throws ISchedulerException {
        return "";
    }

    @Override
    public void destroyGroup(String group) throws ISchedulerException {
    }

    @Override
    public String createDriverContainer(String group, String name, IContainerInfo container) throws ISchedulerException {
        throw new ISchedulerException(NAME + " scheduler must be used with ignis-slurm");
    }

    @Override
    public List<String> createExecutorContainers(String group, String name, IContainerInfo container, int instances) throws ISchedulerException {
        if (executorContainers) {
            throw new ISchedulerException(NAME + " scheduler allows only one cluster");
        }
        IContainerInfo ref = parseContainerInfo("1");
        boolean flag = true;

        flag &= ref.getImage().equals(container.getImage());
        flag &= ref.getCpus() == container.getCpus();
        flag &= ref.getMemory()== container.getMemory();
        flag &= ref.getBinds()== container.getBinds();
        flag &= ref.getVolumes()== container.getVolumes();
        flag &= ref.getHostnames()== container.getHostnames();
        flag &= ref.getEnvironmentVariables().equals(container.getEnvironmentVariables());
        flag &= ref.getSchedulerParams().equals(container.getSchedulerParams());

        if (!flag) {
            if (Boolean.getBoolean("ignis.debug")) {
                LOGGER.info("Debug: " + ref + "\n" + container);
            }
            throw new ISchedulerException("Driver cannot change properties that affect the container executor. Use ignis-slurm.");
        }

        return IntStream.range(1, instances + 1).mapToObj(i -> "" + i).toList();
    }

    @Override
    public IContainerStatus getStatus(String id) throws ISchedulerException {
        return IContainerStatus.RUNNING;
    }

    @Override
    public List<IContainerStatus> getStatus(List<String> ids) throws ISchedulerException {
        return ids.stream().map(s -> IContainerStatus.RUNNING).toList();
    }

    @Override
    public IContainerInfo getDriverContainer(String id) throws ISchedulerException {
        return parseContainerInfo(id);
    }

    @Override
    public List<IContainerInfo> getExecutorContainers(List<String> ids) throws ISchedulerException {
        return ids.stream().map(this::parseContainerInfo).toList();
    }

    @Override
    public IContainerInfo restartContainer(String id) throws ISchedulerException {
        return parseContainerInfo(id);
    }

    @Override
    public void destroyDriverContainer(String id) throws ISchedulerException {
    }

    @Override
    public void destroyExecutorInstances(List<String> ids) throws ISchedulerException {
        executorContainers = false;
    }

    @Override
    public void healthCheck() throws ISchedulerException {
    }

    @Override
    public String getName() {
        return NAME;
    }

}
