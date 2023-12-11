package org.ignis.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;

public final class ILogger {

    private ILogger() {
    }

    public static void init(boolean debug, boolean verbose) {
        try {
            LoggerContext context = (LoggerContext) LogManager.getContext(false);
            Configuration config = context.getConfiguration();
            LoggerConfig rootConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
            rootConfig.removeAppender("INFO");//disable logger
            if (debug) {
                rootConfig.addAppender(config.getAppender("DEBUG"), Level.DEBUG, null);
            } else {
                rootConfig.addAppender(config.getAppender("INFO"), verbose ? Level.INFO : Level.ERROR, null);
            }
            context.updateLoggers();
        } catch (Exception ex) {
            System.err.println("Logger error");//in case the logger was disabled
            System.exit(-1);
        }


        if (!debug) {


        }
    }
}
