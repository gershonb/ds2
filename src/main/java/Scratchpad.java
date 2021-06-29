import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.net.SocketTimeoutException;

public class Scratchpad {
    public static void test() {
        StringBuilder emailS = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            emailS.append(i);
            emailS.append(", ");
        }
        System.out.println(emailS.toString());
        emailS.deleteCharAt(emailS.length() -1);
        emailS.deleteCharAt(emailS.length() -1);
        System.out.println(emailS.toString());
    }

    public static void main(String[] args) throws IOException {
        test();
    }
}

