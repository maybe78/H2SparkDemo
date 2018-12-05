package ru.maybe;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;

class Main {
    public static final boolean DEBUG = true;
    private static final Logger logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);

    public static void main(String[] args) throws Exception {
        int generationSecondsPeriod = 1;
        int generationStartSecondsDelay = 3;
        int requestUrlPort = 8090;
        int generationPerDay = 20;
        int visitsLimitPerDay = 10;
        int cafeTotal = 5;

        String cafeNamePrefix = "CAFE";

        ThreadLocal<Integer> dataGenThread = ThreadLocal.withInitial(() -> 1);
        Timer t = new Timer(true);

        logger.setLevel(DEBUG ? Level.DEBUG : Level.INFO);

        H2DataBase cafeDb = new H2DataBase(H2DataBase.H2DBType.FILE);

        logger.info("Create db structure and generate data with params defined: ");
        CafeData cd = new CafeData(cafeDb, generationPerDay, visitsLimitPerDay, cafeTotal, cafeNamePrefix);

        logger.info("Start a scheduled task for db record generation");
        TimerTask cafeDataGen = cd.getGenerator();
        t.scheduleAtFixedRate(cafeDataGen, generationStartSecondsDelay, (long) dataGenThread.get() * generationSecondsPeriod * 1000);

        logger.info("Start an http server for url request handling");
        HttpRequestHandler.processRequests(requestUrlPort);
    }
}