package ru.sbrf.data;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.gson.Gson;
import org.h2.tools.DeleteDbFiles;
import org.slf4j.LoggerFactory;
import spark.Spark;

import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;


public class Main {
    private static final boolean DEBUG = true;
    private static final Logger logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    private static final int CAFE_COUNT = 5;
    private static final String CAFE_NAME_PREFIX = "Cafe";
    private static final int DAILY_VISIT_GEN_LIMIT = 20;
    private static final ThreadLocal<Integer> genPeriod = ThreadLocal.withInitial(() -> 1);
    private static boolean fileDb = true;
    private static Connection connection = null;

    public static void main(String[] args) {
        logger.setLevel(DEBUG ? Level.DEBUG : Level.INFO);
        H2DataBase.init();

        TimerTask visitGenerator = new
                DBDataGenerator();
        Timer t = new Timer(true);
        t.scheduleAtFixedRate(visitGenerator, 5000, (long) genPeriod.get() * 1000);
        logger.debug("Data generator started");

        RequestHandler.processRequests();
        logger.debug("Request handler is on");
    }

    private static class H2DataBase {
        private static final String DB_NAME = "cafedb";
        private static final String DB_CONNECTION_MEMORY = String.format("jdbc:h2:mem:%s", DB_NAME);
        private static final String DB_CONNECTION_FILE = String.format("jdbc:h2:~/%s;AUTO_SERVER=true", DB_NAME);
        private static final String TABLE_NAME_COLUMN_HEADER = "TABLE_NAME";

        private H2DataBase() {
        }

        private static void init() {
            try {
                Class.forName("org.h2.Driver");
                if (fileDb) {
                    DeleteDbFiles.execute("~/", DB_NAME, false);
                }
                connection = DriverManager.getConnection(fileDb ? DB_CONNECTION_FILE : DB_CONNECTION_MEMORY);
                connection.setAutoCommit(true);
                flushTables();
                DBDataGenerator.createTables();
                DBDataGenerator.populateCafeDataTable();
            } catch (SQLException e) {
                logger.error("Initialization error", e);
            } catch (ClassNotFoundException e) {
                logger.error("DB driver class not found.");
            }
        }

        private static Map getTables() {
            return sqlQueryExec("SHOW TABLES;", TABLE_NAME_COLUMN_HEADER);
        }

        private static void flushTables() {
            Map tables = getTables();
            if (tables.size() <= 0) {
                logger.debug("Setup tables for the new DataBase");
                for (Object table : tables.values()) {
                    sqlQueryExec(String.format("DROP TABLE %s; ", table));
                }
            }
        }

        private static Map sqlQueryExec(String sql, String... returnedColumnsHeaders) {
            int columnsCount = returnedColumnsHeaders.length;
            Map<String, List<String>> table = new HashMap<>();
            try (Statement stmt = connection.createStatement()) {
                if (columnsCount > 0) {
                    try (
                            ResultSet rs = stmt.executeQuery(sql)) {
                        while (rs.next()) {
                            for (String c : returnedColumnsHeaders) {
                                List<String> column = new ArrayList<>();
                                if (table.containsKey(c)) {
                                    column = table.get(c);
                                }
                                column.add(rs.getObject(c).toString());
                                table.put(c, column);
                            }
                        }
                        connection.commit();
                    }
                }
            } catch (SQLException e) {
                logger.error("Error executing the query: " + sql, e);
            }
            if (DEBUG) logger.debug("SQL =>:\t{0}", sql);
            return table;
        }

        private static void insertRecord(int id, Date date, String cafeName, int visits) {
            DateFormat df = new SimpleDateFormat("yyyy-MM-yy");
            String dateDb = String.format("DATE\'%s\'", df.format(date));
            String sql = String.format(
                    "INSERT INTO VISIT_DATA VALUES (%d, \'%s\', %s, %d);", id, cafeName, dateDb, visits);
            sqlQueryExec(sql);
        }

        private static Map fetchCafeAverageVisits() {
            return sqlQueryExec(
                    "SELECT CAFE, " +
                            "AVG(VISIT_COUNT) as AVERAGE " +
                            "from VISIT_DATA " +
                            "group by CAFE " +
                            "order by CAFE;", "CAFE", "AVERAGE");
        }

        private static Map getCafeNames() {
            return sqlQueryExec("SELECT ID, NAME FROM CAFE_LIST GROUP BY ID;",
                    "ID", "NAME");
        }
    }

    private static class DBDataGenerator extends TimerTask {
        private final Calendar date = Calendar.getInstance();
        private int todayVisitGens = 1;
        private int currentCafe = 0;
        private int recordCount = 0;

        private static void createTables() {
            H2DataBase.sqlQueryExec(
                    "CREATE TABLE CAFE_LIST (\n" +
                            "  ID int,\n" +
                            "  NAME VARCHAR(255),\n" +
                            "  PRIMARY KEY(ID) " +
                            ");"
            );
            H2DataBase.sqlQueryExec(
                    "CREATE TABLE VISIT_DATA (" +
                            " ID int AUTO_INCREMENT NOT NULL, " +
                            " CAFE VARCHAR(255), " +
                            " DATE DATE, " +
                            " VISIT_COUNT INT NOT NULL, " +
                            " PRIMARY KEY (ID) " +
                            //" FOREIGN KEY (CAFE) REFERENCES CAFE_LIST(ID) "+
                            " );"
            );
            logger.debug("DB Tables created.");
            H2DataBase.sqlQueryExec("SHOW TABLES;", H2DataBase.TABLE_NAME_COLUMN_HEADER);
        }

        private static void populateCafeDataTable() {
            for (int i = 1; i <= CAFE_COUNT; i++) {
                String sql = String.format(
                        "INSERT INTO CAFE_LIST VALUES (%d, \'%s\');", i, String.format("%s_%d", CAFE_NAME_PREFIX, i));
                H2DataBase.sqlQueryExec(sql);
            }
        }

        @Override
        public void run() {
            int randomVisitCount = new Random().nextInt(11);
            List allCafeNames = (List) H2DataBase.getCafeNames().get("NAME");
            H2DataBase.insertRecord(recordCount, date.getTime(), allCafeNames.get(currentCafe).toString(), randomVisitCount);
            currentCafe++;
            todayVisitGens++;
            recordCount++;
            if (currentCafe >= CAFE_COUNT) currentCafe = 0;
            if (todayVisitGens >= DAILY_VISIT_GEN_LIMIT) {
                date.add(Calendar.DATE, 1);
                todayVisitGens = 1;
            }
        }
    }

    private static class RequestHandler {
        private RequestHandler() {
        }

        private static void processRequests() {
            Gson gson = new Gson();
            Spark.get("/average", (request, response) -> H2DataBase.fetchCafeAverageVisits(), gson::toJson);
        }
    }

}

