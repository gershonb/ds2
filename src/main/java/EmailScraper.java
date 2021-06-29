import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EmailScraper implements Runnable {
    private static HashSet<String> emailList = new HashSet<>();
    private static ArrayBlockingQueue<String> hyperlinksListQueue = new ArrayBlockingQueue<>(10_000);
    private static ArrayBlockingQueue<String> hyperlinksList = new ArrayBlockingQueue<>(1_000_000);
    //private static ArrayBlockingQueue<String> hyperlinksList =  new ArrayBlockingQueue(hyperlinksListQueue);
    private static HashSet<String> usedLinksHSet = new HashSet<>();
    private static Collection<String> usedLinks =  Collections.synchronizedCollection(usedLinksHSet);
    private static int emailcount = 0;
    private static int x;


    public static HashSet<String> getEmailList() {
        return emailList;
    }

    public static Queue<String> getHyperlinksList() {
        return (Queue<String>) hyperlinksList;
    }

    public static void addFirstToHyperlinksList(String url) {
        if (hyperlinksList.size() == 0) {
            hyperlinksList.add(url);
        }
    }

    private Document readSite(String urlWebpage) throws IOException {
        return Jsoup.connect(urlWebpage).get();
    }

    public void getEmail(Document doc) {
        Pattern p = Pattern.compile("[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z]+");
        Matcher matcher = p.matcher(doc.text());
        while (matcher.find()) {
            if(!emailList.contains(matcher.group())) System.out.println(matcher.group());
            synchronized (this) {
                emailList.add(matcher.group());
            }
        }
    }

    public void getHyperlinks(Document doc) {

        Elements elements = doc.select("a[href]");
        synchronized (this) {
            for (Element e : elements) {
                String link = e.attr("href");
                if (!usedLinks.contains(link)) hyperlinksList.add(e.attr("href"));
            }
        }
    }

    public static String emailListToString() {
        StringBuilder sb = new StringBuilder();
        for (String s : emailList) {
            sb.append("\"");
            sb.append(s);
            sb.append("\", ");

        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public static void  addToDatabase() {

        String connectionUrl = // specifies how to connect to the database
                "jdbc:sqlserver://mco364.ckxf3a0k0vuw.us-east-1.rds.amazonaws.com;"
                        + "database=GershonBinder;"
                        + "user=admin364;"
                        + "password=mco364lcm;"
                        + "encrypt=false;"
                        + "trustServerCertificate=false;"
                        + "loginTimeout=30;";
        ResultSet resultSet = null;
        StringBuilder insertSql2SB = new StringBuilder();
        //insert 1000 emails into dbs
        insertSql2SB.append("INSERT INTO [dbo].[EMAILS]\n" +
                "           ([EmailId]\n" +
                "           ,[Address]\n" +
                "     VALUES\n");
        int i = 0;
        for (String s: emailList) {
            insertSql2SB.append("(").append(i).append(", '").append(s).append("')\n");


        }
        System.out.println(insertSql2SB.toString());
        String insertSql2  = insertSql2SB.toString();
            try (Connection connection = DriverManager.getConnection(connectionUrl);
                 PreparedStatement prepEmail = connection.prepareStatement(insertSql2, Statement.RETURN_GENERATED_KEYS);) {
                {
                    prepEmail.executeQuery();
                    resultSet = prepEmail.getGeneratedKeys();
                    while (resultSet.next()) {
                        System.out.println(resultSet.getInt(1));
                    }
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
       //reset emailList and update email count
        emailcount += emailList.size();
            emailList.clear();
        }


    @Override
    public void run()  {
        while (emailList.size() < 10_000) {
            String currentURL = null;
            synchronized (this) {
                if (hyperlinksList.size() > 0) {
                    currentURL = hyperlinksList.remove();
                }

            }
            if (currentURL != null) {
                Document doc = null;
                try {
                    doc = Jsoup.connect(currentURL).get();
                } catch (Exception e) {
                    continue;
                }
                try {
                    getHyperlinks(doc);
                }
                catch(IllegalStateException ignored){}
                getEmail(doc);
                try{
                    //System.out.println(emailListToString());
                    if(x != emailList.size()) {System.out.println(emailList.size() + " - " + currentURL); x  = emailList.size();}

                }
                catch(StringIndexOutOfBoundsException ignored){

                }

            }

            //if +50 add to database
            synchronized (this) {
                if (emailList.size() % 6 == 0 && emailList.size() > 5) {
                    System.out.println("ADDING TO DATABASE");
                    addToDatabase();
                    System.out.println("COMPLETE - ADDING TO DATABASE");
                }
            }

        }
    }


    public static void main(String[] args) throws IOException {
        EmailScraper scraper = new EmailScraper();
        addFirstToHyperlinksList("https://www.touro.edu/");
        Runnable r1 = new EmailScraper();
        // creates a thread pool with MAX_T no. of
        // threads as the fixed pool size(Step 2)
        ExecutorService pool = Executors.newFixedThreadPool(20);

        // passes the Task objects to the pool to execute (Step 3)
        for (int i = 0; i < 20; i++) {
            pool.execute(r1);
        }

        // pool shutdown ( Step 4)
        //pool.shutdown();

    }


}
