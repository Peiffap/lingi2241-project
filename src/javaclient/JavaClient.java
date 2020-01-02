package javaclient;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;

public class JavaClient {

    private static Random rnd = new Random();

    public static void main(String[] args) {
        try {
            // Load the JDBC driver, so your Java program can talk with your database.
            // You have to download the driver (called Connector/J) from
            //   https://mariadb.com/kb/en/library/about-mariadb-connector-j/
            // and add the jar file to your project, otherwise you will get a
            // ClassNotFound exception.
            Class.forName("org.mariadb.jdbc.Driver");

            // Connect to the database. Very similar to using the mysql commandline tool.
            // Of course, you have to change the IP address and username and password.
            try(Connection con = DriverManager.getConnection(
                    "jdbc:mysql://192.168.0.11:3306", "user", "password")) {
                // Launch tests related to influence of query type on response time.
                queryInfluence(con, 30);
            }
        }
        catch (Exception e) {
            System.out.println(e);
        }
    }

    private static void queryInfluence(Connection con, int n) throws IOException, SQLException {
        ReentrantLock avlock = new ReentrantLock();
        ReentrantLock csvlock = new ReentrantLock();

        // Write the data to a csv file which can be turned into plots by the Python code.
        FileWriter csvWriter = null;
        try {
            csvWriter = new FileWriter("../data/query_influence.csv");
        } catch (IOException e) {
            e.printStackTrace();
        }
        assert csvWriter != null;
        csvWriter.append("query_type");
        csvWriter.append(",");
        csvWriter.append("time");
        csvWriter.append(",");
        csvWriter.append("is_average");
        csvWriter.append("\n");

        // For every query type, run the tests a certain number of times.
        String[] types = {"GetAverage", "Select", "Write"};
        for (String queryType : types) {
            long av = 0;
            SQLThread[] threads = new SQLThread[n];
            switch (queryType) {
                case "GetAverage":
                    for (int i = 0; i < n; i++) {
                        int startRow = Math.abs(rnd.nextInt() % 2000000);
                        int nRows = Math.abs(rnd.nextInt() % 1000);
                        threads[i] = new SQLThread(con, queryType, startRow, nRows, -1);
                        threads[i].start();
                        long tl = threads[i].getTime();
                        avlock.lock();
                        try {
                            av += tl;
                        } finally {
                            avlock.unlock();
                        }
                        String t = Long.toString(tl);
                        csvlock.lock();
                        try {
                            csvWriter.append(queryType);
                            csvWriter.append(",");
                            csvWriter.append(t);
                            csvWriter.append(",");
                            csvWriter.append("no");
                            csvWriter.append("\n");
                        } finally {
                            csvlock.unlock();
                        }
                    }
                    for (t : threads) {
                        t.join();
                    }
                    break;
                case "Select":
                    for (int i = 0; i < n; i++) {
                        int startRow = Math.abs(rnd.nextInt() % 2000000);
                        int nRows = 1000000;
                        threads[i] = new SQLThread(con, queryType, startRow, -1, nRows);
                        threads[i].start();
                        long tl = threads[i].getTime();
                        avlock.lock();
                        try {
                            av += tl;
                        } finally {
                            avlock.unlock();
                        }
                        String t = Long.toString(tl);
                        csvlock.lock();
                        try {
                            csvWriter.append(queryType);
                            csvWriter.append(",");
                            csvWriter.append(t);
                            csvWriter.append(",");
                            csvWriter.append("no");
                            csvWriter.append("\n");
                        } finally {
                            csvlock.unlock();
                        }
                    }
                    for (t : threads) {
                        t.join();
                    }
                    break;
                case "Write":
                    for (int i = 0; i < n; i++) {
                        threads[i] = new SQLThread(con, queryType, -1, -1, -1);
                        threads[i].start();
                        long tl = threads[i].getTime();
                        avlock.lock();
                        try {
                            av += tl;
                        } finally {
                            avlock.unlock();
                        }
                        String t = Long.toString(tl);
                        csvlock.lock();
                        try {
                            csvWriter.append(queryType);
                            csvWriter.append(",");
                            csvWriter.append(t);
                            csvWriter.append(",");
                            csvWriter.append("no");
                            csvWriter.append("\n");
                        } finally {
                            csvlock.unlock();
                        }
                    }
                    for (t : threads) {
                        t.join();
                    }
                    break;
                default:
                    System.out.println("Unknown query; aborting.");
                    System.exit(1);
                    break;
            }
            csvWriter.append(queryType);
            csvWriter.append(",");
            csvWriter.append(av/n);
            csvWriter.append(",");
            csvWriter.append("yes");
            csvWriter.append("\n");
        }
        csvWriter.flush();
        csvWriter.close();
    }

    // Example query where most of the processing happens on the server.
    public static long testGetAverage(Connection con,int startRow,int numberOfRows) throws SQLException {
        // Send the query to the database.
        PreparedStatement stmt = con.prepareStatement("SELECT AVG(t.salary) FROM (SELECT salary FROM employees.salaries LIMIT ? OFFSET ?) as t");
        stmt.setInt(1, numberOfRows);
        stmt.setInt(2, startRow);
        long s = System.nanoTime();
        ResultSet rs = stmt.executeQuery();
        long e = System.nanoTime();

        // Get the result of the query. The while-loop is not really needed
        // here because our example query will only return one result line
        while (rs.next()) {
            double averageSalary=rs.getDouble(1);  // get column 1 of the result
        }
        return e - s;
    }

    // Example query that generates some network traffic because data is sent from the server
    // to the client.
    public static long testSelect(Connection con,int startRow,int numberOfRows) throws SQLException {
        PreparedStatement stmt = con.prepareStatement("SELECT * FROM employees.salaries LIMIT ? OFFSET ?");
        stmt.setInt(1, numberOfRows);
        stmt.setInt(2, startRow);
        long s = System.nanoTime();
        ResultSet rs = stmt.executeQuery();
        long e = System.nanoTime();
        while (rs.next()) {
            // we are not really doing anything with the data...
            int employeeNumber=rs.getInt(1);
            int salary=rs.getInt(2);
            Date from=rs.getDate(3);
            Date to=rs.getDate(4);
        }
        return e - s;
    }

    // Example query that writes to the database.
    public static long testWrite(Connection con) throws SQLException {
        // random employee
        int employeeNumber=10001+(Math.abs(rnd.nextInt()) % 100000);
        // random dates
        long r=Math.abs(rnd.nextLong()) % 100000000000000L;
        Date from=new java.sql.Date(r);
        Date to=new java.sql.Date(r+10000000);

        try {
            PreparedStatement stmt = con.prepareStatement("INSERT INTO employees.salaries VALUE (?,123,?,?)");
            stmt.setInt(1,employeeNumber);
            stmt.setDate(2,from);
            stmt.setDate(3,to);
            long s = System.nanoTime();
            stmt.executeUpdate();
            long e = System.nanoTime();
            return e - s;
        }
        catch(SQLIntegrityConstraintViolationException e) {
            // The salaries table uses the employee number and the from-date as primary key.
            // Since we are generating random employee numbers and random dates, there is a certain
            // probability that the row already exists. Let's just ignore errors for this test.
        } finally {
            return -1;
        }

        // Clean up your database afterwards with
        //   DELETE from employees.salaries WHERE salary=123
    }
}

class SQLThread extends Thread {

    private Connection con;
    private String type;
    private int startRow, nRowsAv, nRowsSelect;
    private long time;

    public SQLThread(Connection con, String type, int startRow, int nRowsAv, int nRowsSelect) {
        this.con = con;
        this.type = type;
        this.startRow = startRow;
        this.nRowsAv = nRowsAv;
        this.nRowsSelect = nRowsSelect;
    }

    @Override
    public void run() {
        try {
            switch (this.type) {
                case "GetAverage":
                    this.time = JavaClient.testGetAverage(this.con, this.startRow, this.nRowsAv);
                    break;
                case "Select":
                    this.time = JavaClient.testSelect(this.con, this.startRow, this.nRowsSelect);
                    break;
                case "Write":
                    this.time = JavaClient.testWrite(this.con);
                    break;
                default
                    ;
                    break;
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public long getTime() {
        return this.time;
    }
}