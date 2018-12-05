package ru.maybe;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static ru.maybe.SQLDataBase.Table.Structure.SQLType.*;

public class CafeData {
    private static String dbDateFormat = "yyy-MM-dd";
    /**
     * Table is represented as array of Cells, each storing column header and data type
     */
    private static SQLDataBase.Table.Structure visitDataStruct = new SQLDataBase.Table.Structure(new SQLDataBase.Table.Structure.Cell[]{
            new SQLDataBase.Table.Structure.Cell("ID", INT, "AUTO_INCREMENT NOT NULL"),
            new SQLDataBase.Table.Structure.Cell("CAFE", VARCHAR),
            new SQLDataBase.Table.Structure.Cell("DATE", DATE),
            new SQLDataBase.Table.Structure.Cell("VISIT_COUNT", INT, "NOT NULL")
    });
    private static SQLDataBase.Table.Structure cafeListStruct = new SQLDataBase.Table.Structure(new SQLDataBase.Table.Structure.Cell[]{
            new SQLDataBase.Table.Structure.Cell("NAME", VARCHAR),
            new SQLDataBase.Table.Structure.Cell("ID", INT)
    });
    private static SQLDataBase.Table cafeTable = new SQLDataBase.Table("CAFE_LIST", cafeListStruct);
    private static SQLDataBase.Table visitTable = new SQLDataBase.Table("VISIT_DATA", visitDataStruct);
    private VisitGenerator generator;
    private int visitLimit;
    private int generationsLimit;
    private int cafeInstances;
    private String namePref;
    private List<String> cafeList;

    /**
     * @param db                - Database uset for tables and data storage
     * @param generationsPerDay - Amount of data generations before day is switched to tomorrow
     * @param cafeVisitLimit    - Maximum visit amount for cafe in one data generation
     * @param cafeTotal         - total amount of cafe instances to generate data for
     * @param cafeNamePrefix    - Name prefix used for auto-generated instances
     */
    CafeData(H2DataBase db, int generationsPerDay, int cafeVisitLimit, int cafeTotal, String cafeNamePrefix) {
        this.visitLimit = cafeVisitLimit;
        this.generationsLimit = generationsPerDay;
        this.cafeInstances = cafeTotal;
        this.namePref = cafeNamePrefix;
        this.generator = new VisitGenerator();
        this.cafeList = new ArrayList<>();
        writeToDb(db);
    }

    public static Map getAverageCafeVisits() {
        return visitTable.select(
                new String[]{"CAFE as name", "AVG(VISIT_COUNT) as traffic_avg_day"},
                new String[]{"name", "traffic_avg_day"},
                "group by name order by name");
    }


    private void writeToDb(H2DataBase database) {
        database.createTable(cafeTable);
        database.createTable(visitTable);
        insertCafeNames(cafeTable);
    }

    public int getCafeCount() {
        return this.cafeInstances;
    }

    public int getVisitLimit() {
        return this.visitLimit;
    }

    public VisitGenerator getGenerator() {
        return this.generator;
    }

    public List<String> getCafeNames() {
        if (this.cafeList.isEmpty()) {
            String[] names = Arrays.stream(cafeTable.structure.getData())
                    .map(SQLDataBase.Table.Structure.Cell::getName).toArray(String[]::new);
            cafeList.addAll(cafeTable.select(names, names).values());
        }
        return cafeList;
    }

    private void insertCafeNames(SQLDataBase.Table cafeNames) {
        for (int i = 0; i < this.cafeInstances; i++) {
            String name = String.format("'%s%d'", namePref, i + 1);
            cafeNames.insert(new String[]{name, String.valueOf(i)});
            this.cafeList.add(name);
        }
    }

    public class VisitGenerator extends TimerTask {
        private Calendar today;
        private int genNumber = 0;

        private VisitGenerator() {
            this.genNumber = 0;
            this.today = Calendar.getInstance();
        }

        @Override
        public void run() {
            for (int ind = 0; ind < cafeInstances; ind++) {
                makeVisit(visitTable, ind);
                processNext();
            }
        }

        private void insertVisit(SQLDataBase.Table visit, String[] data) {
            visit.insert(data);
        }

        private void makeVisit(SQLDataBase.Table visitTable, int cafeDbIndex) {
            DateFormat df = new SimpleDateFormat(dbDateFormat);
            if (cafeDbIndex < 0)
                cafeDbIndex = new Random().nextInt(getCafeCount());
            int visits = new Random().nextInt(getVisitLimit());
            insertVisit(visitTable, new String[]{
                    String.valueOf(genNumber),
                    String.format("%s", getCafeNames().get(cafeDbIndex)),
                    String.format("DATE\'%s\'", df.format(today.getTime())),
                    String.valueOf(visits)
            });
        }

        private void processNext() {
            genNumber++;
            if (genNumber % generationsLimit == 0) {
                today.add(Calendar.DATE, 1);
            }
        }
    }
}
