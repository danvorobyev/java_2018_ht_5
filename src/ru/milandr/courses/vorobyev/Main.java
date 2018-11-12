package ru.milandr.courses.vorobyev;

import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

public class Main {
    private static int getResultSetRowCount(ResultSet resultSet) {
        int size;
        try {
            resultSet.last();
            size = resultSet.getRow();
            resultSet.beforeFirst();
        } catch (SQLException ex) {
            return 0;
        }
        return size;
    }

    private static boolean listCompare(List<?> l1, List<?> l2) {
        ArrayList<?> list1 = new ArrayList<>(l1);
        AtomicBoolean flag = new AtomicBoolean(true);
        l2.forEach(i -> {
            if (!list1.remove(i)) {
                flag.set(false);
            }
        });
        return list1.isEmpty() & flag.get();
    }

    private static List<Map<String, String>> resultSetToList(ResultSet rs, List<String> columnNames) {
        List<Map<String, String>> list = new ArrayList<>();
        int rowSize = getResultSetRowCount(rs);
        IntStream intStream1 = IntStream.range(0, rowSize);
        intStream1.forEach(i -> {
            try {
                if (rs.next()) {
                    Map<String, String> map = new HashMap<>();
                    list.add(map);
                    IntStream intStream2 = IntStream.range(0, columnNames.size());
                    intStream2.forEach(j ->
                    {
                        try {
                            map.put(columnNames.get(j), rs.getString(j + 1));
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    });
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
        return list;
    }


    public static void main(String[] args) {
        Scanner in = new Scanner(System.in);
        System.out.println("Enter database 1 name ");
        String db1 = in.nextLine();
        System.out.println("Enter login for " + db1 + " ");
        String db1Login = in.nextLine();
        System.out.println("Enter password for " + db1 + " ");
        String db1Password = in.nextLine();
        System.out.println("Enter database 2 name " + " ");
        String db2 = in.nextLine();
        System.out.println("Enter login for " + db2 + " ");
        String db2Login = in.nextLine();
        System.out.println("Enter password for " + db1 + " ");
        String db2Password = in.nextLine();
        System.out.println("Enter table 1 name ");
        String db1Table = in.nextLine();
        System.out.println("Enter table 2 name: ");
        String db2Table = in.nextLine();
        System.out.println("Enter column names(separate them with a comma) ");
        String allColumnNames = in.nextLine();
        List<String> columnNames = Arrays.asList(allColumnNames.split(","));

        String db1Url = "jdbc:postgresql://localhost:5432/" + db1;
        String db2Url = "jdbc:postgresql://localhost:5432/" + db2;

        try (Connection db1Connection = DriverManager.getConnection(
                db1Url, db1Login, db1Password);
             Connection db2Connection = DriverManager.getConnection(
                     db2Url,
                     db2Login, db2Password);
             Statement db1Statement = db1Connection.createStatement(
                     ResultSet.TYPE_SCROLL_INSENSITIVE,
                     ResultSet.CONCUR_UPDATABLE);
             Statement db2Statement = db2Connection.createStatement(
                     ResultSet.TYPE_SCROLL_INSENSITIVE,
                     ResultSet.CONCUR_UPDATABLE)) {

            ResultSet rs1 = db1Statement.executeQuery("SELECT " + allColumnNames + " FROM " + db1Table);
            ResultSet rs2 = db2Statement.executeQuery("SELECT " + allColumnNames + " FROM " + db2Table);

            List db1List = resultSetToList(rs1, columnNames);
            List db2List = resultSetToList(rs2, columnNames);

            System.out.println(listCompare(db1List, db2List));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
