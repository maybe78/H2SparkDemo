package ru.maybe;

import ch.qos.logback.classic.Logger;
import org.h2.tools.DeleteDbFiles;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/**
 * Jdbc connection String format  for H2 is  a user defined db name):
 * DB in file : jdbc:h2:~/%s;AUTO_SERVER=true
 * In Memory type  DB: jdbc:h2:mem:%s
 */

class H2DataBase extends SQLDataBase {
    private static final Logger logger = (Logger) LoggerFactory.getLogger(H2DataBase.class.getName());
    private static String DB_NAME = "test";
    private static String DB_PATH = "~/";

    public H2DataBase(H2DBType type) throws ClassNotFoundException, SQLException {
        Class.forName("org.h2.Driver");
        if (type.equals(H2DBType.FILE)) {
            DeleteDbFiles.execute(DB_PATH, DB_NAME, false);
        }
        String jdbc = "jdbc:h2:" + type.getValue();
        logger.info("Connecting with jdbc string: \"{}\"", jdbc);
        this.setConnection(jdbc);
        getConnection().setAutoCommit(true);
    }

    public static void setDbName(String name) {
        DB_NAME = name;
    }

    public enum H2DBType {
        FILE(DB_PATH + DB_NAME),
        MEMORY("mem:");
        String value;

        H2DBType(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}

