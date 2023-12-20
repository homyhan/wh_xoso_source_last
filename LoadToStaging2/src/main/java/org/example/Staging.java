package org.example;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.*;
import java.sql.*;
import java.util.Date;
import java.text.SimpleDateFormat;

public class Staging {

    private ConfigReader configReader;
    static String urlControl;
    String userControl;
    String passControl;
    String urlStaging;
    String userStaging;
    String passStaging;
    String moduleLoad;
    String moduleTransform;
    String modulePreviousProcess;
    String moduleProcess;
    String moduleSuccess;
    String filePathLogs;
    Connect controllerConnection;
    String moduleFile;
    String csv;

    public Staging(ConfigReader configReader) {
        this.configReader = configReader;
        loadConfig();
    }

    // 2. Load config module
    public void loadConfig() {
        urlControl = configReader.getProperty(ConfigReader.ConfigurationProperty.STAGING_CONTROL_URL.getPropertyName());
        userControl = configReader.getProperty(ConfigReader.ConfigurationProperty.STAGING_CONTROL_USERNAME.getPropertyName());
        passControl = configReader.getProperty(ConfigReader.ConfigurationProperty.STAGING_CONTROL_PASSWORD.getPropertyName());

        urlStaging = configReader.getProperty(ConfigReader.ConfigurationProperty.STAGING_CONTROL_URL.getPropertyName());
        userStaging = configReader.getProperty(ConfigReader.ConfigurationProperty.STAGING_USERNAME.getPropertyName());
        passStaging = configReader.getProperty(ConfigReader.ConfigurationProperty.STAGING_PASSWORD.getPropertyName());

        modulePreviousProcess = configReader.getProperty(ConfigReader.ConfigurationProperty.MODULE_PREVIOUS_PROCESS.getPropertyName());
        moduleLoad = configReader.getProperty(ConfigReader.ConfigurationProperty.MODULE_LOAD_STAGING.getPropertyName());
        moduleTransform = configReader.getProperty(ConfigReader.ConfigurationProperty.MODULE_TRANSFORM.getPropertyName());
        moduleProcess = configReader.getProperty(ConfigReader.ConfigurationProperty.MODULE_PROCESS.getPropertyName());
        moduleSuccess = configReader.getProperty(ConfigReader.ConfigurationProperty.MODULE_SUCCESS.getPropertyName());
        moduleFile = configReader.getProperty(ConfigReader.ConfigurationProperty.MODULE_FILE_LOGS_ERROR.getPropertyName());
        csv = configReader.getProperty(ConfigReader.ConfigurationProperty.CSV.getPropertyName());
    }

    public boolean checkPreviousProgress() {
        boolean result;
        // 3. Connect to database control
        controllerConnection = new Connect(urlControl, userControl, passControl);
        Connection connectControl = controllerConnection.getConnection();

        // 4.Checking connection to database control
        if (connectControl == null) {

            // 4.1.Insert new record failed into file log
            controllerConnection.writeLogToFile(moduleFile, "fail", "connect control failed");
            return false; // kết thúc
        }
        System.out.println("Ket noi thanh cong");
        // 4.2. Select * from control.data_files where name = "Extract data" and create_at = CURRENT_DATE() and status = "successful"
        String query = "SELECT * FROM control2.data_files WHERE name = '" + modulePreviousProcess + "' AND created_at = CURRENT_DATE() AND status = '" + moduleSuccess + "'";
        try {
            Statement stmt = connectControl.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            ResultSetMetaData metaData = rs.getMetaData();
            int column = metaData.getColumnCount();
            System.out.println("Ket noi 1");
            // 5. Check query results
            if (rs.next()) {

                result = true;
                String test = "";
                for (int i = 1; i <= column; i++) {
                    String columnName = metaData.getColumnName(i);
                    String data = rs.getString(columnName);
                    test += columnName + " " + data + "\t";
                }
            } else {
                result = false;
            }
        } catch (SQLException e) {
            throw new RuntimeException();
        }
        return result;
    }

    public void execute() {
        // result = false -> kết thúc
        if (!checkPreviousProgress()) {
            return;
        }

        // result = true
        // 6. Insert into control.logs(event, status) values ('Load to Staging', 'in process')
        insertLogProcess();

        // 7. Connect database staging
        Connect connectStaging = new Connect(urlStaging, userStaging, passStaging);

        try {
            Connection connectionStaging = connectStaging.getConnection();
            System.out.println("ex1");
            // 8. Checking connection to database staging
            // fail
            if (connectionStaging == null) {

                // 8.1. Insert control.logs(event, status, note) values ('Load to Staging', 'fail', 'connect to staging failed')
                writeLog();

                // kết thúc
                return;
            }

            // success
            // 8.2. Check if the StagingXoso table exists
            // false
            if (!doesTableExist(connectionStaging, "StagingXoso")) {
                // 8.2.1. Create table StagingXoso: province, area, date_lottery, name_prize, result, create_at, update_at, create_by, update_by
                createStagingXosoTable(connectionStaging);
            }
            // true
            else {

                // 8.2.2. Truncate table StagingXoso
                truncateTable();
            }

            // 9. Get the folder containing files from the location attribute of the data_file_configs table (select location form control.data_files_configs where create_at = CURRENT_DATE() order by create_at DESC limit 1) + Xoso_dd_MM_yyyy.csv
            String filePath = loadFilePath() + csv + new SimpleDateFormat("dd_MM_yyyy").format(new Date()) + ".csv";

            // 11. Record file information in the table data_files
            // INSERT INTO control.data_files(name, row_count, status, note, created_at, updated_at, created_by, updated_by) VALUES (?, ?, ?, ?, current_date(), ?, ?, ?)
            insertDataFiles();

            // 12. Insert data from Xoso_dd_MM_yyyy.csv into the StagingXoso table: province, area, date_lottery, name_prize, result, create_at, update_at, create_by, update_by
            insertDataIntoStagingXoso(filePath);

            // 13. Insert into control.logs(event, status) values ('Load to Staging', 'successful')
            insertLogSuccess();

            // 14. Close all connect
            connectionStaging.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public boolean doesTableExist(Connection connection, String tableName) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet tables = metaData.getTables(null, null, tableName, null);
        return tables.next();
    }

    public void createStagingXosoTable(Connection connection) throws SQLException {
        // Create StagingXoso table
        Statement statement = connection.createStatement();
        statement.executeUpdate("CREATE TABLE staging2.StagingXoso (id int auto_increment PRIMARY KEY, province TEXT, area TEXT, date_lottery TEXT, " +
                "name_prize TEXT, result TEXT, create_at TEXT, update_at TEXT, create_by TEXT," +
                " update_by TEXT)");
        statement.close();
    }

    public void insertLogProcess() {
        Connection connection = controllerConnection.getConnection();
        String query = "INSERT INTO control2.logs(event, status) VALUES (?, ?)";
        try {
            PreparedStatement pre = connection.prepareStatement(query);
            pre.setString(1, moduleLoad);
            pre.setString(2, moduleProcess);
            pre.executeUpdate();
            pre.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void insertLogSuccess() {
        Connection connection = controllerConnection.getConnection();
        String query = "INSERT INTO control2.logs(event, status, create_at) VALUES (?, ?, current_date())";
        try {
            PreparedStatement pre = connection.prepareStatement(query);
            pre.setString(1, moduleLoad);
            pre.setString(2, moduleSuccess);
            pre.executeUpdate();
            pre.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void writeLog() {
        Connection connection = controllerConnection.getConnection();
        String query = "INSERT control2.logs(event, status, note) VALUES (?, ?, ?)";
        try {
            PreparedStatement pre = connection.prepareStatement(query);
            pre.setString(1, moduleLoad);
            pre.setString(2, "fail");
            pre.setString(3, "connect to staging failed");
            pre.executeUpdate();
            pre.close();
        } catch (SQLException e) {
            throw new RuntimeException();
        }
    }

    public void truncateTable() {
        Connection connection = controllerConnection.getConnection();
        String query = "TRUNCATE TABLE staging2.StagingXoso";
        System.out.println("truncateTable StagingXoso success");
        try {
            PreparedStatement pre = connection.prepareStatement(query);
            pre.executeUpdate();
            pre.close();
        } catch (SQLException e) {
            throw new RuntimeException();
        }
    }

    public void insertDataIntoStagingXoso(String filePath) {
        Connection connection = controllerConnection.getConnection();
        try {
            PreparedStatement insertStatement = connection.prepareStatement(
                    "INSERT INTO staging2.StagingXoso (province, area, date_lottery, name_prize, result, create_at, update_at, create_by, update_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");

            // 10. Read data from file Xoso_dd_MM_yyyy.csv
            CSVReader reader = new CSVReader(new FileReader(filePath));
            String[] line;
            reader.skip(1);
            System.out.println("Da them data vao StagingXoso table");
            while ((line = reader.readNext()) != null) {

                insertStatement.setString(1, line[0]);
                insertStatement.setString(2, line[1]);
                insertStatement.setString(3, line[2]);
                insertStatement.setString(4, line[3]);
                insertStatement.setString(5, line[4]);
                insertStatement.setString(6, line[5]);
                insertStatement.setString(7, line[6]);
                insertStatement.setString(8, line[7]);
                insertStatement.setString(9, line[8]);


                insertStatement.executeUpdate();

            }

            insertStatement.close();
        } catch (SQLException | IOException | CsvValidationException e) {
            e.printStackTrace();
        }
    }

    public String loadFilePath() {
        Connection connection = controllerConnection.getConnection();
        String filePath = null;
        PreparedStatement ps = null;
        ResultSet rs = null;


            try {
                ps = connection.prepareStatement("SELECT location FROM control2.data_file_configs ORDER BY create_at DESC LIMIT 1");
                rs = ps.executeQuery();

                if (rs.next()) {
                    // Lấy giá trị từ cột "location"
                    filePath = rs.getString("location");
                }
                ps.close();
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        return filePath;
        }


    public void insertDataFiles(){
        Connection connection = controllerConnection.getConnection();
        String query = "INSERT INTO control2.data_files(name, row_count, status, note, created_at, updated_at, created_by, updated_by)" +
                "VALUES (?, ?, ?, ?, current_date(), current_date(), ?, ?)";
        try{
            PreparedStatement pre = connection.prepareStatement(query);
            pre.setString(1, csv + new SimpleDateFormat("dd_MM_yyyy").format(new Date()));
            pre.setInt(2, countCSVLines(loadFilePath() + csv + new SimpleDateFormat("dd_MM_yyyy").format(new Date()) + ".csv"));
            pre.setString(3, moduleSuccess);
            pre.setString(4, "Data import success");
            pre.setString(5, "root");
            pre.setString(6, "root");
            pre.executeUpdate();
            pre.close();
        } catch(Exception e){
            e.printStackTrace();
        }
    }

    // đếm dòng dữ liệu trong file csv
    public int countCSVLines(String filePath) throws IOException {
        int lineCount = 0;

        // Mở file để đọc
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;

            br.readLine();
            // Đọc từng dòng
            while ((line = br.readLine()) != null) {
                lineCount++;
            }
        }

        return lineCount;
    }

    public static void main(String[] args) {
        ConfigReader conf = new ConfigReader();
        Staging staging = new Staging(conf);
        staging.execute();
    }
}

