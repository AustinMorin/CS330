package Assign3;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

class TestAssign3 {

    static final String DEF_WriterParams = "writerparams.txt";

    static Connection writerConn = null;
    static Scanner input;

    public static void main(String[] args) throws Exception {
        Properties writerProps = new Properties();
        writerProps.load(new FileInputStream(DEF_WriterParams));
        setupWriter(writerProps);
        
        input = new Scanner(new File("testdata.txt"));
        
        int testno = 0;
        while (input.hasNextLine()) {
            String query = getQuery();
            if (query != null) {
                System.out.println("Query: " + query);
                String[][] data = getData();
                testno += 1;
                System.out.printf("Test #%d ...", testno);
                System.out.flush();
                String[][] results = runTest(query);
                if (testResults(data, results)) {
                    System.out.println(" OK");
                } else {
                    System.out.println(" Failed!");
                    printFailure(data, results);
                    System.out.println();
                }
            }
        }
        
        closeWriter();
    }

    static void setupWriter(Properties connectProps) throws SQLException, ClassNotFoundException {
    	Class.forName("com.mysql.jdbc.Driver");
        String dburl = connectProps.getProperty("dburl");
        String username = connectProps.getProperty("user");
        writerConn = DriverManager.getConnection(dburl, connectProps);
        System.out.printf("Writer connection %s %s established.%n", dburl, username);
    } 

    static void closeWriter() throws SQLException {
        writerConn.close();
    }
    
    static String getQuery() {
        String line = input.nextLine().trim();
        if (line.length() == 0)
            return null;
        String query = line;
        while (true) {
            line = input.nextLine().trim();
            if (line.length() == 0)
                return query;
            query = query + " " + line;
        }
    }
    
    static String[][] getData() {
        List<String> lines = new ArrayList<String>();
        String firstLine = input.nextLine().trim();
        while (input.hasNextLine()) {
            String nextLine = input.nextLine().trim();
            if (nextLine.length() == 0)
                break;
            lines.add(nextLine);
        }
        String[][] result = new String[lines.size() + 1][];
        result[0] = firstLine.split("\\t");
        int i = 0;
        for (String line : lines) {
            i ++;
            result[i] = line.split("\\t");
        }
        return result;
    }
    
    static String[][] runTest(String query) throws SQLException {
        List<String[]> results = new ArrayList<String[]>(); 
        Statement stmt = writerConn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        ResultSetMetaData rsmd = rs.getMetaData();
        int colCount = rsmd.getColumnCount();
        String[] headers = new String[colCount];
        for (int i = 0; i < colCount; i++) {
            headers[i] = rsmd.getColumnLabel(i + 1);
        }
        results.add(headers);
        while (rs.next()) {
            String[] rowData = new String[colCount];
            for (int i = 0; i < colCount; i++) {
                rowData[i] = rs.getString(i + 1);
            }
            results.add(rowData);
        }
        rs.close();
        stmt.close();
        return results.toArray(new String[0][]);
    }
    
    static boolean testResults(String[][] data, String[][] results) {
        if (data.length != results.length)
            return false;
        for (int i = 1; i < results.length; i ++) {
            if (!Arrays.equals(data[i], results[i])) {
                return false;
            }
        }
        return true;
    }
    
    static void printFailure(String[][] data, String[][] results) {
        printArray("Expected", data, Integer.MAX_VALUE);
        printArray("Actual", results, data.length + 10);
        System.out.println();
    }
    
    static void printArray(String title, String[][] data, int limit) {
        System.out.print(title);
        if (data.length > limit)
            System.out.printf(" (only showing %d of %d rows)", limit - 1, data.length - 1);
        if (data.length == 1) {
            System.out.println(" zero rows");
            return;
        }
        System.out.println(" ...");
        int[] widths = new int[data[0].length];
        Arrays.fill(widths, 0);
        for (String[] row : data) {
            for (int c = 0; c < row.length; c++) {
                if (row[c].length() > widths[c])
                    widths[c] = row[c].length();
            }
        }
        String format = "";
        for (int w : widths) {
            format = format + String.format("%%%ds ", w);
        }
        format = format + "%n";
        boolean firstRow = true;
        int numrow = 0;
        for (Object[] row : data) {
            System.out.printf(format, row);
            numrow++;
            if (numrow >= limit)
                break;
        }
    }
}
