import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Connect {
    String url;
    String user;
    String password;
    Connection connection;
    public Connect(String url, String user, String password){
        this.url = url;
        this.user = user;
        this.password = password;
        ConnectDB();
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public Connection getConnection() {
        return connection;
    }
    public void ConnectDB(){
        try{
            connection = DriverManager.getConnection(url, user, password);
        } catch(Exception e){
            e.printStackTrace();
        }
    }
    public void writeLogToFile(String filePath, String status, String errorMessage) {
        LocalDateTime currentDateTime = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String formattedDateTime = currentDateTime.format(formatter);

        String logData = String.format("date: %s, module: Load data to File, status=\"%s\", message_error: %s%n",
                formattedDateTime, status, errorMessage);

        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(filePath, true));
            writer.write(logData);
            System.out.println("Log has been written successfully.");
        } catch (IOException e) {
            System.err.println("Error writing to the log file: " + e.getMessage());
        }
    }
}
