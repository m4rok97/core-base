package org.ignis.submitter;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;

import org.ignis.logging.ILogger;
import org.ignis.properties.IKeys;
import org.ignis.properties.IProperties;
import org.ignis.properties.IPropertyException;
import org.ignis.scheduler3.IScheduler;
import org.ignis.scheduler3.ISchedulerException;
import org.ignis.scheduler3.ISchedulerFactory;
import org.slf4j.LoggerFactory;

import picocli.CommandLine;

public abstract class BaseJob implements Callable<Integer> {

    protected IProperties props;

    protected IScheduler scheduler;

    @CommandLine.Option(names = {"--debug"}, description = "display debugging information")
    protected boolean debug;

    protected boolean verbose;

    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "show this help message and exit")
    protected boolean help;

    protected Map<String, String> propertyArg;

    protected String propertyFile;

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(BaseJob.class);

    public final Integer call() {
        try {
            ILogger.init(debug, verbose);
            init();
            run();
        } catch (Exception ex) {
            if (Boolean.getBoolean(IKeys.DEBUG)) {
                LOGGER.error(ex.getMessage(), ex);
            } else {
                System.err.println(ex.getMessage());
            }
            return -1;
        }
        return 0;
    }

    private void init() throws Exception {
        var defaults = new IProperties();
        defaults.load(getClass().getClassLoader().getResourceAsStream("etc/ignis.yaml"));
        props = new IProperties(defaults);
        props.setProperty(IKeys.WDIR, System.getProperty("user.dir"));
        LOGGER.info("Loading environment variables");
        props.fromEnv(System.getenv());

        try {
            LOGGER.info("Loading configuration file");
            File conf = new File(props.getString(IKeys.HOME), "etc/ignis.yaml");
            if (conf.exists()) {
                props.load(conf.getPath());
            }
        } catch (IPropertyException | IOException ex) {
            LOGGER.warn("Error loading ignis.yaml, ignoring", ex);
        }
        if (propertyFile != null) {
            props.load(propertyFile);
        }
        if (propertyArg != null) {
            props.fromMap(propertyArg);
        }
        if (props.hasProperty(IKeys.OPTIONS)) {
            try {
                props.load64(props.getProperty(IKeys.OPTIONS));
            } catch (IOException ex) {
                LOGGER.warn("Options load error", ex);
            }
            props.rmProperty(IKeys.OPTIONS);
        }
        props.setProperty(IKeys.DEBUG, String.valueOf(debug));
        if (props.getBoolean(IKeys.DEBUG)) {
            System.setProperty(IKeys.DEBUG, "true");
            LOGGER.info("DEBUG enabled");
        } else {
            System.setProperty(IKeys.DEBUG, "false");
        }

        LOGGER.info("Loading scheduler");
        if(!props.hasProperty(IKeys.SCHEDULER_NAME) && props.hasProperty(IKeys.CONTAINER_PROVIDER)){
            props.setProperty(IKeys.SCHEDULER_NAME, props.getProperty(IKeys.CONTAINER_PROVIDER));
        }
        String url = "", type = null;
        try {
            url = props.getProperty(IKeys.SCHEDULER_URL, null);
            type = props.getProperty(IKeys.SCHEDULER_NAME);

            LOGGER.info("Checking scheduler " + type);
            scheduler = ISchedulerFactory.create(type, url);
            scheduler.healthCheck();
            LOGGER.info("Scheduler " + type + (url != null ? (" '" + url + "'") : "") + "...OK");
        } catch (ISchedulerException ex) {
            LOGGER.error("Scheduler " + type + (url != null ? (" '" + url + "'") : "") + "...Fails");
            throw ex;
        }
    }

    public abstract void run() throws Exception;
}