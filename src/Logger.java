import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Logger {
//    Logger class has one static function that output the messages to the Console
    public static void info(final String id, final String msg) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        System.out.println(dtf.format(now) + " (" + id + "): " + msg);
    }
}
