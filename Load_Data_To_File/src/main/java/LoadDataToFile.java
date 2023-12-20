import com.opencsv.CSVWriter;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class LoadDataToFile {
    private Config configReader;
    Connect connection;
    String url;
    String user;
    String password;
    String csv;
    String moduleFile;
    String moduleSuccess;
    String moduleLoad;
    String moduleProcess;
    String csvFilePath;


    public LoadDataToFile(Config configReader) {
        this.configReader = new Config();
        loadConfig();
    }



    //2. Load config module
    public void loadConfig() {
        url = configReader.getProperty("url");
        user = configReader.getProperty("username");
        password = configReader.getProperty("password");
        moduleLoad = configReader.getProperty("Module.LoadDataToFile");
        moduleSuccess = configReader.getProperty("Module.success");
        moduleFile = configReader.getProperty("Module.LogsError");
        moduleProcess = configReader.getProperty("Module.process");
        csv = configReader.getProperty("csv");
        csvFilePath = configReader.getProperty("csvFilePath");
    }
    private static final String USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36";
    public void crawlAndSaveData(String url) {
        try {

            Connection.Response response = Jsoup.connect(url).userAgent(USER_AGENT).execute();
            if (response.statusCode() != 200) {
                System.err.println("Failed to fetch data. HTTP Status Code: " + response.statusCode());
                return ;
            }

            String charset = response.charset();
            System.out.println("Charset of the webpage: " + charset);

            Document document = response.parse();
            Elements tables = document.select("table.rightcl");

            List<String[]> allData = new ArrayList<>();

            for (Element table : tables) {
                Elements rows = table.select("tbody tr");

                String province = normalizeProvince(rows.get(0).select("td.tinh a").text());
                String area = normalizeProvince(rows.get(1).select("td.matinh").text());
                String date_lottery = getCurrentDate();

                List<String[]> data = new ArrayList<>();
                for (int i = 2; i < rows.size(); i++) {
                    Element row = rows.get(i);
                    String name_prize = row.select("td").attr("class");
                    String result = row.select("div").text();

                    String created_at = getCurrentTimestamp();
                    String updated_at = getCurrentTimestamp();
                    String created_by = "YourCreatedBy";
                    String updated_by = "YourUpdatedBy";

                    String[] rowData = {province, area, date_lottery, name_prize, result, created_at, updated_at, created_by, updated_by};
                    data.add(rowData);
                }

                allData.addAll(data);
            }
            String csvFile = "XOSO_" + new SimpleDateFormat("dd_MM_yyyy").format(new Date());
            String filePath = csvFilePath + csvFile + ".csv";
            System.out.println("File path: " + filePath);

            // Ensure the directory exists, create if not
            File directory = new File(csvFilePath);
            if (!directory.exists()) {
                directory.mkdirs();  // Creates the directory if it doesn't exist
            }

            // 8. chạy saveToCSV()
            saveToCSV(allData, filePath);

            System.out.println("Data successfully crawled and saved to CSV file");
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Failed to crawl data from source: " + e.getMessage());
        }
    }

    private static String normalizeProvince(String input) {
        return input.replaceAll("\\p{M}", ""); // Remove diacritics
    }

    private static void saveToCSV(List<String[]> data, String filePath) {
        //9 Check success
        try (CSVWriter writer = new CSVWriter(new OutputStreamWriter(new FileOutputStream(filePath), StandardCharsets.ISO_8859_1))) {
            // Write CSV header
            writer.writeNext(new String[]{"Province", "area", "date_lottery", "name_prize", "result", "created_at", "updated_at", "created_by", "updated_by" });

            for (String[] rowData : data) {
                String[] results = rowData[4].split("\\s+");

                for (String result : results) {
                    String[] newRowData = {
                            normalizeProvince(rowData[0]),  // province
                            rowData[1],  // area
                            rowData[2],  // date_lottery
                            rowData[3],  // name_prize
                            result,      // result
                            rowData[5],  // created_at
                            rowData[6],  // updated_at
                            rowData[7],  // created_by
                            rowData[8]   // updated_by
                    };
                    writer.writeNext(newRowData);
                }
            }
            System.out.println("Data successfully saved to CSV file: " + filePath);
        } catch (IOException e) {
            e.printStackTrace();
            //9.1 insert record into logs with writeLog()
            System.err.println("Failed to save data to CSV: " + e.getMessage());
        }
    }

    private static String getCurrentTimestamp() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return dateFormat.format(new Date());
    }
    private static String getCurrentDate() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
    }

    private void udpateStatusDataFiles() {
        java.sql.Connection connect = connection.getConnection();
        String query = "UPDATE control.data_files SET status = ?, note = ?, updated_at = current_date(), updated_by = ? WHERE id = ?";
        // 10. check update successfully status to table data_files
        try {
            PreparedStatement pre = connect.prepareStatement(query);
            pre.setString(1,moduleLoad);
            pre.setString(2, moduleSuccess);
            pre.setString(3, "root");
            pre.setInt(4, 1);
            pre.executeUpdate();
            pre.close();
            String logFilePath = configReader.getProperty("Module.LogsError");

            connection.writeLogToFile(logFilePath, "ERROR", "Invalid file path");
        } catch (Exception e) {
            e.printStackTrace();
            // 10.1 insert record into logs wite writeLog()
            writeLog(e.getMessage());
        }
    }

    public void insertLogProcess() {
        java.sql.Connection connect = connection.getConnection();
        String query = "INSERT INTO control.Logs(event, status) VALUES (?, ?)";
        try {
            PreparedStatement pre = connect.prepareStatement(query);
            pre.setString(1, moduleLoad);
            pre.setString(2, moduleProcess);
            pre.executeUpdate();
            pre.close();
        } catch (SQLException e) {
            writeLog(e.getMessage());
        }
    }

    // 2.3 add source path to table data_file_configs
    private void insertSourcePath(String sourcePath) {
        String csvFile = "XOSO_" + new SimpleDateFormat("dd_MM_yyyy").format(new Date());
        String location = csvFilePath + csvFile + ".csv";
        java.sql.Connection connect = connection.getConnection();
        String query = "INSERT INTO control.data_file_configs(name,source_path, location)" +
                "VALUES (?, ?, ?)";
        // 6. check success
        try {
            PreparedStatement pre = connect.prepareStatement(query);
            pre.setString(1, moduleLoad);
            pre.setString(2, sourcePath);
            pre.setString(3, location);
            pre.executeUpdate();
            pre.close();
        } catch (Exception e) {
            // 6.1 insert record into logs with writeLog()
            writeLog(e.getMessage());
        }
    }

    // 2.4 add news row to table data_files with id of data_file_configs
    public void insertDataFiles() {
        connection = new Connect(url, user, password);
        String query = "INSERT INTO control.data_files(name, row_count, status, note, created_at, updated_at, created_by, updated_by)" +
                "VALUES (?, ?, ?, ?, current_date(), current_date(), ?, ?)";

        // 7. check success
        try {
            PreparedStatement pre = connection.getConnection().prepareStatement(query);

            // Replace "csv" with the actual variable or value you intend to use
            String csvValue = "XOSO_" + new SimpleDateFormat("dd_MM_yyyy").format(new Date());
            pre.setString(1, csvValue);
            pre.setInt(2, countCSVLines(loadFilePath() + csvValue + ".csv"));
            pre.setString(3, moduleLoad);
            pre.setString(4, moduleSuccess);
            pre.setString(5, "root");
            pre.setString(6, "root");
            pre.executeUpdate();
            pre.close();
        } catch (Exception e) {
            //7.1 insert record into logs with writeLog()
            writeLog(e.getMessage());
        }
    }

    public int countCSVLines(String filePath) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        int lineCount = 0;
        while (reader.readLine() != null) {
            lineCount++;
        }
        return lineCount;
    }

    public void writeLog(String note) {
        java.sql.Connection connect = connection.getConnection();
        String query = "INSERT control.Logs(event, status, note) VALUES (?, ?, ?)";
        try {
            PreparedStatement pre = connect.prepareStatement(query);
            pre.setString(1, moduleLoad);
            pre.setString(2, "fail");
            pre.setString(3, note);
            pre.executeUpdate();
            pre.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public String loadFilePath() {

        String filePath = "";
        PreparedStatement ps = null;
        ResultSet rs = null;
        if (filePath == null || filePath.isEmpty()) {
            connection.writeLogToFile(moduleFile, "error", "Invalid file path");
        } else {
            filePath = csvFilePath;
            System.out.println(filePath);
        }
        return filePath;
    }
    public void run() {
        // 3. Connect database control
        connection = new Connect(url, user, password);
        try {
            //4. Checking connection to database control
            java.sql.Connection connect = connection.getConnection();
            if (connect == null) {
                // 4.1 insert record into logs with writeLogToFile()
                connection.writeLogToFile(moduleFile , "error", "Lỗi kết nối");
                return;
            }

            //4.2 Insert status "PROCESSING" into Log Process
            insertLogProcess();
            Statement statement = connect.createStatement();
            String sourcePath = "https://www.minhngoc.net.vn/xo-so-truc-tiep/mien-nam.html";
            //5 insert source_path to data_file_configs
            insertSourcePath(sourcePath);
            //6.2 insert news row in data_files with id of data_file_configs
            insertDataFiles();
            //7.2. run phương thức crawlAndSaveData(sourcePath)
            crawlAndSaveData(sourcePath);
            //9.2 update data_files status
            udpateStatusDataFiles();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        Config config = new Config();
        LoadDataToFile dataLoader = new LoadDataToFile(config); // Assuming Config class exists
        dataLoader.run();

    }

//    public static void main(String[] args) {
//        LoadDataToFile webScraper = new LoadDataToFile();
//        webScraper.crawlData("https://www.minhngoc.net.vn/xo-so-truc-tiep/mien-nam.html");
//    }
}
//    public static void main(String[] args) {
//        String url = "https://www.minhngoc.net.vn/ket-qua-xo-so/mien-nam.html";
//        String csvFilePath = "D:\\lottery_results.csv";
//
//        crawlAndSaveToCsv(url, csvFilePath);
//    }
//
//    public static void crawlAndSaveToCsv(String url, String csvFilePath) {
//        if (url == null || url.isEmpty()) {
//           // writeLog("connect to source failed");
//            return;
//        }
//
//        Document doc;
//
//        try {
//            doc = Jsoup.connect(url).get();
//            String category = doc.select("div.title > a").text(); // Modify this selector based on your actual HTML structure
//            Elements items = doc.select("table.bkqmiennam > tbody > tr");
//
//            // Open CSV file for writing
//            try (FileWriter writer = new FileWriter(csvFilePath)) {
//                // Write CSV header
//                writer.write("province,area,date_lottery,name_prize,result,created_at,updated_at,created_by,updated_by\n");
//
//                for (Element item : items) {
//                    String province = item.select("td.tinh > a").text();
//                    String areaCode = item.select("td.matinh").text();
//
//                    // Extract giai8 to giaidb
//                    Elements prizeElements = item.select("td[class^='giai']");
//                    for (Element prize : prizeElements) {
//                        // Add current date and time
//                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//                        String currentDateTime = sdf.format(new Date());
//
//                        // Write CSV line for each prize
//                        StringBuilder line = new StringBuilder();
//                        line.append(province).append(",").append(areaCode).append(",").append(currentDateTime)
//                                .append(",").append(category).append(",").append(prize.select("div").text())
//                                .append(",").append(currentDateTime).append(",").append(currentDateTime)
//                                .append(",").append("your_created_by_value").append(",").append("your_updated_by_value");
//
//                        writer.write(line.toString() + "\n");
//                    }
//                }
//            }
//
//        } catch (IOException e) {
//           // writeLog(e.getMessage());
//        }
//    }

//    public static void crawlAndSaveData(String url) {
//        try {
//            Connection.Response response = Jsoup.connect(url).userAgent(USER_AGENT).execute();
//            if (response.statusCode() != 200) {
//                System.err.println("Failed to fetch data. HTTP Status Code: " + response.statusCode());
//                return;
//            }
//
//            Document document = response.parse();
//            Elements tables = document.select("table.rightcl");
//
//            List<String[]> allData = new ArrayList<>();
//
//            for (Element table : tables) {
//                Elements rows = table.select("tbody tr");
//
//                String province = rows.get(0).select("td.tinh a").text();
//                String area = rows.get(1).select("td.matinh").text();
//                String date_lottery = getCurrentDate();
//
//                List<String[]> data = new ArrayList<>();
//                for (int i = 2; i < rows.size(); i++) {
//                    Element row = rows.get(i);
//                    String name_prize = row.select("td").attr("class");
//                    String result = row.select("div").text();
//
//                    String created_at = getCurrentTimestamp();
//                    String updated_at = getCurrentTimestamp();
//                    String created_by = "YourCreatedBy";
//                    String updated_by = "YourUpdatedBy";
//
//                    String[] rowData = {province, area, date_lottery, name_prize, result, created_at, updated_at, created_by, updated_by};
//                    data.add(rowData);
//                }
//
//                allData.addAll(data);
//            }
//
//            // Lưu dữ liệu vào một file CSV duy nhất
//            saveToCSV(allData, "D:\\WH\\xoso_all_province7.csv");
//
//            System.out.println("Data successfully crawled and saved to CSV file");
//        } catch (IOException e) {
//            e.printStackTrace();
//            System.err.println("Failed to crawl data from source: " + e.getMessage());
//        }
//    }
//
//    private static void saveToCSV(List<String[]> data, String filePath) {
//        try (CSVWriter writer = new CSVWriter(new OutputStreamWriter(new FileOutputStream(filePath), StandardCharsets.UTF_8))) {
//            // Write CSV header
//            writer.writeNext(new String[]{"Province", "area", "date_lottery", "name_prize", "result", "created_at", "updated_at", "created_by", "updated_by" });
//
//            for (String[] rowData : data) {
//                String[] results = rowData[4].split("\\s+");
//
//                for (String result : results) {
//                    String[] newRowData = {
//                            rowData[0],  // province
//                            rowData[1],  // area
//                            rowData[2],  // date_lottery
//                            rowData[3],  // name_prize
//                            result,      // result
//                            rowData[5],  // created_at
//                            rowData[6],  // updated_at
//                            rowData[7],  // created_by
//                            rowData[8]   // updated_by
//                    };
//                    writer.writeNext(newRowData);
//                }
//            }
//            System.out.println("Data successfully saved to CSV file: " + filePath);
//        } catch (IOException e) {
//            e.printStackTrace();
//            System.err.println("Failed to save data to CSV: " + e.getMessage());
//        }
//    }


//    public static void crawlAndSaveData(String url) {
//        try {
//            Connection.Response response = Jsoup.connect(url).userAgent(USER_AGENT).execute();
//            if (response.statusCode() != 200) {
//                System.err.println("Failed to fetch data. HTTP Status Code: " + response.statusCode());
//                return;
//            }
//
//            Document document = response.parse();
//            Elements tables = document.select("table.rightcl");
//
//            for (Element table : tables) {
//                Elements rows = table.select("tbody tr");
//
//                String province = rows.get(0).select("td.tinh a").text();
//                String area = rows.get(1).select("td.matinh").text();
//                String date_lottery = getCurrentDate();
//
//                List<String[]> data = new ArrayList<>();
//                for (int i = 2; i < rows.size(); i++) {
//                    Element row = rows.get(i);
//                    String name_prize = row.select("td").attr("class");
//                    String result = row.select("div").text();
//
//                    String created_at = getCurrentTimestamp();
//                    String updated_at = getCurrentTimestamp();
//                    String created_by = "YourCreatedBy";
//                    String updated_by = "YourUpdatedBy";
//
//                    String[] rowData = {province, area, date_lottery, name_prize, result, created_at, updated_at, created_by, updated_by};
//                    data.add(rowData);
//                }
//
//                // Lưu dữ liệu của mỗi tỉnh vào một file CSV riêng biệt
//                saveToCSV(data, province);
//            }
//
//            System.out.println("Data successfully crawled and saved to CSV files");
//        } catch (IOException e) {
//            e.printStackTrace();
//            System.err.println("Failed to crawl data from source: " + e.getMessage());
//        }
//    }
//
//    private static void saveToCSV(List<String[]> data, String province) {
//        String filePath = "D:\\WH\\xoso5_province_" + province + ".csv";
//
//        try (CSVWriter writer = new CSVWriter(new FileWriter(filePath))) {
//            // Write CSV header
//            writer.writeNext(new String[]{"Province", "Area", "Date_Lottery", "Name_Prize", "Result", "Created_At", "Updated_At", "Created_By", "Updated_By" });
//
//            for (String[] rowData : data) {
//                String[] results = rowData[4].split("\\s+");
//
//                for (String result : results) {
//                    // Thêm một dòng mới cho mỗi kết quả
//                    String[] newRowData = {
//                            rowData[0],  // province
//                            rowData[1],  // area
//                            rowData[2],  // date_lottery
//                            rowData[3],  // name_prize
//                            result,      // result
//                            rowData[5],  // created_at
//                            rowData[6],  // updated_at
//                            rowData[7],  // created_by
//                            rowData[8]   // updated_by
//                    };
//                    writer.writeNext(newRowData);
//                }
//            }
//            System.out.println("Data successfully saved to CSV file: " + filePath);
//        } catch (IOException e) {
//            e.printStackTrace();
//            System.err.println("Failed to save data to CSV: " + e.getMessage());
//        }
//    }



