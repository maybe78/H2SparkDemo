package ru.maybe;

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


class Main {
    private static final boolean DEBUG = false;
    private static final Logger logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    private static final ThreadLocal<Integer> dataGenThread = ThreadLocal.withInitial(() -> 1);

    private Main() {
    }

    public static void main(String[] args) {
        logger.setLevel(DEBUG ? Level.DEBUG : Level.INFO);
        H2DataBase.init();

        TimerTask visitGenerator = new CafeDataGenerator();
        Timer t = new Timer(true);
        t.scheduleAtFixedRate(visitGenerator, 5000, (long) dataGenThread.get() * 1000);
        logger.debug("Data generator started");

        RequestHandler.processRequests();
        logger.debug("Request handler is on");
    }

    private static class H2DataBase {
        private static boolean fileDb = true;
        private static Connection connection = null;
        private static final String DB_NAME = "cafedb";
        private static String dbConnection = String.format("jdbc:h2:mem:%s", DB_NAME);
        private static final String DB_CONNECTION_FILE = String.format("jdbc:h2:~/%s;AUTO_SERVER=true", DB_NAME);
        private static final String TABLE_NAME_COLUMN_HEADER = "TABLE_NAME";

        private H2DataBase() {
        }

        private static void init() {
            try {
                Class.forName("org.h2.Driver");
                if (fileDb) {
                    DeleteDbFiles.execute("~/", DB_NAME, false);
                    dbConnection = DB_CONNECTION_FILE;
                }
                connection = DriverManager.getConnection(dbConnection);
                connection.setAutoCommit(true);
                flushTables();
                CafeDataGenerator.createTables();
                CafeDataGenerator.populateCafeDataTable();
            } catch (SQLException e) {
                logger.error("Initialization error", e);
            } catch (ClassNotFoundException e) {
                logger.error("DB driver class not found.", e);
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
                    sqlQueryExec(String.format("drop table if exists %s;", table));
                    logger.debug("Table {0} dropped.", table);
                }
            }
        }

        private static Map sqlQueryExec(String sql, String... returnedColumnsHeaders) {
            int columnsCount = returnedColumnsHeaders.length;
            Map<String, List<String>> table = new HashMap<>();
            try (Statement stmt = connection.createStatement()) {
                if (columnsCount > 0) {
                    try (ResultSet rs = stmt.executeQuery(sql)) {
                        while (rs.next()) {
                            for (String c : returnedColumnsHeaders) {
                                List<String> column = table.getOrDefault(c, new ArrayList<>());
                                column.add(rs.getObject(c).toString());
                                table.put(c, column);
                            }
                        }
                    }
                } else {
                    stmt.execute(sql);
                }
                connection.commit();
                logger.info("SQL =>:\t{0}", sql);
            } catch (SQLException e) {
                logger.error("Error executing the query: " + sql, e);
            }
            return table;
        }
    }

    private static class CafeDataGenerator extends TimerTask {
        private static final String CAFE_NAME_PREFIX = "Cafe";
        private static final int CAFE_COUNT = 5;
        private static final int DAILY_VISIT_GEN_LIMIT = 20;
        private static int todayVisitGens = 1;
        private static int currentCafe = 0;
        private static int recordCount = 0;
        private static List allCafeNames = Collections.emptyList();

        private static void generateVisits() {
            final Calendar date = Calendar.getInstance();
            if (allCafeNames.isEmpty())
                allCafeNames = (List) getCafeNames().get("NAME");
            int randomVisitCount = new Random().nextInt(11);
            CafeDataGenerator.insertRecord(recordCount, date.getTime(), allCafeNames.get(currentCafe).toString(), randomVisitCount);
            currentCafe++;
            todayVisitGens++;
            recordCount++;
            if (currentCafe >= CAFE_COUNT)
                currentCafe = 0;
            if (todayVisitGens >= DAILY_VISIT_GEN_LIMIT) {
                date.add(Calendar.DATE, 1);
                todayVisitGens = 1;
            }
        }

        private static Map getCafeNames() {
            return H2DataBase.sqlQueryExec("SELECT ID, NAME FROM CAFE_LIST GROUP BY ID;",
                    "ID", "NAME");
        }

        private static void insertRecord(int id, Date date, String cafeName, int visits) {
            DateFormat df = new SimpleDateFormat("yyyy-MM-yy");
            String dateDb = String.format("DATE\'%s\'", df.format(date));
            String sql = String.format(
                    "INSERT INTO VISIT_DATA VALUES (%d, \'%s\', %s, %d);", id, cafeName, dateDb, visits);
            H2DataBase.sqlQueryExec(sql);
        }

        private static void populateCafeDataTable() {
            for (int i = 1; i <= CAFE_COUNT; i++) {
                H2DataBase.sqlQueryExec(String.format(
                        "INSERT INTO CAFE_LIST VALUES (%d, \'%s\');", i, String.format("%s_%d", CAFE_NAME_PREFIX, i)));
            }
        }

        @Override
        public void run() {
            generateVisits();
        }

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
            H2DataBase.sqlQueryExec("SHOW TABLES;", H2DataBase.TABLE_NAME_COLUMN_HEADER);
        }
    }

    private static class RequestHandler {
        private RequestHandler() {
        }

        private static void processRequests() {
            Gson gson = new Gson();
            Spark.get("/average", (request, response) -> getAverageVisits(), gson::toJson);
        }

        private static Map getAverageVisits() {
            return H2DataBase.sqlQueryExec(
                    "SELECT CAFE, " +
                            "AVG(VISIT_COUNT) as AVERAGE " +
                            "from VISIT_DATA " +
                            "group by CAFE " +
                            "order by CAFE;", "CAFE", "AVERAGE"
            );
        }
    }

}