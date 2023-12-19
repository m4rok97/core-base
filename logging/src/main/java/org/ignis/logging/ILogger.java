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
            context.setConfigLocation(ILogger.class.getResource("/logger.properties").toURI());

            Configuration config = context.getConfiguration();
            if(!config.getAppenders().containsKey("INFO") || !config.getAppenders().containsKey("DEBUG")){
                throw new RuntimeException("Logger appenders not found");
            }
            LoggerConfig rootConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
            if (debug) {
                rootConfig.addAppender(config.getAppender("DEBUG"), Level.DEBUG, null);
            } else {
                rootConfig.addAppender(config.getAppender("INFO"), verbose ? Level.INFO : Level.ERROR, null);
            }
            context.updateLoggers();
        } catch (Exception ex) {
            if(debug){
                ex.printStackTrace();
            } else {
                System.err.println("Logger error: " + ex.getLocalizedMessage() );
            }
            System.exit(-1);
        }
    }
}
