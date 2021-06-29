import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EmailScraper2 implements Runnable {
    private static int emailCountTotal = 0;
    private static int emailCount = 0;
    private static Set<String> emailList = new HashSet<>();
    private static Set<String> emailList_safe = Collections.synchronizedSet(emailList);
    private static Queue<String> hyperlinksList = new LinkedBlockingQueue<>();

    public static int getEmailCount() {
        return emailCount;
    }
    public static void addFirstToHyperlinksList(String url) {
        if (hyperlinksList.size() == 0) {
            hyperlinksList.add(url);
        }
    }

    public void getEmail(Document doc) {

        Pattern p = Pattern.compile("[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z]+");
        Matcher matcher = p.matcher(doc.text());
        try {
            while (matcher.find()) {

                String forDB = "('" + matcher.group() + "')";
                emailList.add(forDB);
                //System.out.println(matcher.group());
            }
        } catch (NoSuchElementException ignored) {
        }
    }

    public static String emailListToString(){
        StringBuilder emailS = new StringBuilder();
        for (String s: emailList_safe) {
            emailS.append(s);
            emailS.append(", ");
        }
        emailS.deleteCharAt(emailS.length() -1);
        emailS.deleteCharAt(emailS.length() -1);
        //System.out.println(emailS);
        return emailS.toString();
    }

    public void getHyperlinks(Document doc) {

        Elements elements = doc.select("a[href]");
        for (Element e : elements) {
            String link = e.attr("abs:href");
            //String link = e.attr("href");
            //if (link.startsWith("http")) {
            //    //System.out.println(link);
            hyperlinksList.add(link);
            //}
        }

    }

    public static void addToDatabase() {
        String connectionUrl = // specifies how to connect to the database
                "jdbc:sqlserver://mco364.ckxf3a0k0vuw.us-east-1.rds.amazonaws.com;"
                        + "database=GershonBinder;"
                        + "user=admin364;"
                        + "password=mco364lcm;"
                        + "encrypt=false;"
                        + "trustServerCertificate=false;"
                        + "loginTimeout=30;";

        try (Connection conn = DriverManager.getConnection(connectionUrl);
             Statement stmt = conn.createStatement();
        ) {
            // Execute a query
            System.out.println("Inserting records into the table...");
            String sql = "INSERT INTO Emails VALUES" + emailListToString();

            stmt.executeUpdate(sql);
            System.out.println("db addition complete...");
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }


    @Override
    public void run() {
        //System.out.println("New Thread..." + Thread.currentThread().getId());
        //System.out.println(hyperlinksList.size());
        try {
            String currentURL = null;
            if (hyperlinksList.size() > 0) {
                currentURL = hyperlinksList.remove();
            }
            if (currentURL != null) {
                Document doc = Jsoup.connect(currentURL).get();
                if (hyperlinksList.size() < 10) {
                    System.out.println("hyperList size:----" + hyperlinksList.size() );
                    getHyperlinks(doc);
                }
                //long startTime = System.nanoTime();
                //System.out.println("Start hl " + Thread.currentThread().getId());
                getEmail(doc);
                //long endTime = System.nanoTime();
                //System.out.println("End hl " + Thread.currentThread().getId() + "---" + ((endTime - startTime)/ 1000) + " x?_seconds");
                //System.out.println();

            }
        } catch (IOException | NoSuchElementException ignored) {
            System.out.println("Thread closing from error...");
            Thread.currentThread().interrupt();
            //e.printStackTrace();
        }

        //System.out.println("Thread ended..."+Thread.currentThread().getId());
    }


    public static void main(String[] args) throws IOException {
        EmailScraper2 scraper = new EmailScraper2();
        addFirstToHyperlinksList("https://www.touro.edu/");
        Runnable r1 = new EmailScraper2();

        // creates a thread pool with MAX_T no. of
        // threads as the fixed pool size(Step 2)
        ExecutorService pool = Executors.newFixedThreadPool(10);

        // passes the Task objects to the pool to execute (Step 3)
        while (emailCountTotal < 10_000) {

            if (hyperlinksList.size() > 0) {
                try {
                    //System.out.println("execute thread **************");
                    pool.execute(r1);
                } catch (NoSuchElementException ignored) {
                }
            }
            if (emailCount != emailList.size()) {
                System.out.println("emailList size:" + emailList_safe.size());
            }
            emailCount = emailList.size();
            if (emailCount >= 50) {
                synchronized (r1) {
                    addToDatabase();
                    emailCountTotal += emailCount;
                    emailCount = 0;
                    emailList.clear();
                    emailList_safe.clear();
                    System.out.println("---------------------------------");
                    System.out.println("TOTAL EMAILS:" + emailCountTotal);
                    System.out.println("---------------------------------");
                }
            }
        }


        // pool shutdown ( Step 4)
        pool.shutdown();
        //System.out.println(emailListToString());
    }


}

