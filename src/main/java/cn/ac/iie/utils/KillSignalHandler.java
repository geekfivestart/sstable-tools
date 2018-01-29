package cn.ac.iie.utils;

import cn.ac.iie.drive.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;
import sun.misc.SignalHandler;

/**
 * Used for remote debuggin
 */
public class KillSignalHandler implements SignalHandler {
    private static final Logger LOG= LoggerFactory.getLogger(KillSignalHandler.class);
    @Override
    public void handle(Signal arg0) {
        Driver.debug.set(false);
        LOG.info("Exiting!! See you later");
    }
}