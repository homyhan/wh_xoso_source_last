package org.example;

import java.sql.*;

public class Main {
    private ConfigReader configReader;
    private ConnectDB connectDBControl;
    String urlControl;
    String userControl;
    String passControl;

    String urlDW;
    String userDW;
    String passDW;

    String urlDM;
    String userDM;
    String passDM;
    String moduleName;
    String columns;
    String filePathLogs;
    String previousModule;
    int idLog;
    public Main(ConfigReader configReader) {
        this.configReader = configReader;
        loadConfig();
    }
    // 2. load config module
    public void loadConfig() {
        // load config module
        moduleName = configReader.getProperty(ConfigReader.ConfigurationProperty.MODULE_LOAD_TO_DATA_MART.getPropertyName());
        columns = configReader.getProperty(ConfigReader.ConfigurationProperty.MODULE_COLUMNS_NEW_LOTTERY.getPropertyName());
        filePathLogs = configReader.getProperty(ConfigReader.ConfigurationProperty.MODULE_FILE_LOGS_ERROR.getPropertyName());
        previousModule = configReader.getProperty(ConfigReader.ConfigurationProperty.MODULE_PREVIOUS_MODULE.getPropertyName());

        // load config dbControl
        urlControl = configReader.getProperty(ConfigReader.ConfigurationProperty.STAGING_CONTROL_URL.getPropertyName());
        userControl = configReader.getProperty(ConfigReader.ConfigurationProperty.STAGING_CONTROL_USERNAME.getPropertyName());
        passControl = configReader.getProperty(ConfigReader.ConfigurationProperty.STAGING_CONTROL_PASSWORD.getPropertyName());

        // load config db DW
        urlDW = configReader.getProperty(ConfigReader.ConfigurationProperty.DATA_WAREHOUSE_URL.getPropertyName());
        userDW = configReader.getProperty(ConfigReader.ConfigurationProperty.DATA_WAREHOUSE_USERNAME.getPropertyName());
        passDW= configReader.getProperty(ConfigReader.ConfigurationProperty.DATA_WAREHOUSE_PASSWORD.getPropertyName());

        // load config db DM
        urlDM = configReader.getProperty(ConfigReader.ConfigurationProperty.DATA_MART_URL.getPropertyName());
        userDM= configReader.getProperty(ConfigReader.ConfigurationProperty.DATA_MART_USERNAME.getPropertyName());
        passDM= configReader.getProperty(ConfigReader.ConfigurationProperty.DATA_MART_PASSWORD.getPropertyName());
    }
    public boolean checkPreviousProgress(){
        boolean result = false;
        // 3. connect database control
        connectDBControl = new ConnectDB(urlControl,userControl, passControl, filePathLogs);
        Connection connectionControl = connectDBControl.getConnection();
        System.out.println(connectionControl);
        // 4. Checking connection to database control
        if(connectionControl == null){
            //4.1 Insert new record failed into file log
            // ghi log vào file nếu kết nối thất bại
            System.out.println("URL control: "+urlControl);
            connectDBControl.writeLogToFile(filePathLogs, "fail", "connect control failed");
            return false; //  kết thúc chương trình
        }
        // 4.2 Select  * from logs where event = "transform aggregate" and DATE(create_at) = CURDATE() and status="successful"
        String queryPreviousProcess = "SELECT * FROM logs where event='" + previousModule + "' AND DATE(create_at) = CURDATE() AND status='successful'";
                try {
            Statement stmtControl = connectionControl.createStatement();
            ResultSet rs = stmtControl.executeQuery(queryPreviousProcess);
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            // 5. Check query results
            if(rs.next()){
                result = true;
                String test = "";
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    String data = rs.getString(columnName);
                    test += columnName + " " + data + "\t";
                }
                System.out.println(test);
            }else{
                result = false;
            }
            //
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }
    private boolean checkProcessEverRun() {
        boolean result = false;
        Connection connectionControl = connectDBControl.getConnection();
        // 5.1 Select  * from logs where event = "transform aggregate" and DATE(create_at) = CURDATE() and status="successful"
        String queryProcess = "SELECT * FROM logs where event='" + moduleName + "' AND DATE(create_at) = CURDATE() AND status='successful'";
        try {
            Statement stmtControl = connectionControl.createStatement();
            ResultSet rs = stmtControl.executeQuery(queryProcess);
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            // 6. Check query results
            if(rs.next()){
                result = true;
            }else{
                result = false;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    public void executeApp() {
        // 5. Check query results
        if (!checkPreviousProgress()) {
            return;
        }
        // 6. Check query results
        if (checkProcessEverRun()) {
            return;
        }
        // insert logs
        // 6.1 Insert new record into table control.log with event="Load to data mart",status="in process" (INSERT INTO logs(event, status) VALUES ('Load to data mart','in process'))
        insertLogsProcess("in process", "");
        // 7. Connect database data_warehouse
        ConnectDB connectDW = new ConnectDB(urlDW, userDW, passDW, filePathLogs, idLog, connectDBControl.getConnection());
        try {
            // Kết nối đến Data Warehouse
            Connection connectionDW = connectDW.getConnection();
            // 8. Checking connection to data_warehouse
            if(connectionDW == null) {
                // 8. 1 Insert new record into table control.log with event="load to data mart",status="fail", note="content error"
                //(INSERT INTO logs(event, status,note) VALUES ('load to data mart','fail', 'connect data_warehouse failed'))
                insertLogsProcess("fail", "connect data_warehouse failed");
                return;
            }
            // Kết nối đến Data Mart
            // Truy vấn SQL để lấy dữ liệu từ bảng trong Data Warehouse
            // 8.2 Get rows in table lottery_aggregate (SELECT * FROM lottery_aggregate where DATE(date) = CURDATE())
            String sqlSelect = "SELECT * FROM lottery_aggregate WHERE DATE(date_lottery) = CURDATE()";

            Statement stmtDW = connectionDW.createStatement();
            ResultSet rs = stmtDW.executeQuery(sqlSelect);

            // 9. Connect database data_mart
            ConnectDB connectDM = new ConnectDB(urlDM, userDM, passDM, filePathLogs, idLog, connectDBControl.getConnection());
            Connection connectionDM = connectDM.getConnection();
            // 10. Checking connection to data_mart
            if(connectionDM == null) {
               // 10.1 Insert new record into table control.log with event="load to data mart",status="fail", note="content error"
                //(INSERT INTO logs(event, status,note) VALUES ('load to data mart','fail', 'connect data_mart failed'))
                insertLogsProcess("fail", "connect data_mart failed");
                return;
            }
            // PreparedStatement để chèn dữ liệu vào Data Mart
            String sqlInsert = createQueryInsertToDataMart("present_xoso");
            PreparedStatement pstmtDM = connectionDM.prepareStatement(sqlInsert);
            String[] columnsArr = columns.split(",");
            // Duyệt qua kết quả từ Data Warehouse và chèn vào Data Mart
            // 10.2 Insert rows into table news_articles_temp
            while (rs.next()) {
                // Lấy dữ liệu từ kết quả truy vấn DW và chèn vào DM
                for(int i=0; i< columnsArr.length; i++){
                    String column = columnsArr[i];
                    pstmtDM.setString(i+1, rs.getString(column));
                }
                // Thực hiện chèn dữ liệu vào Data Mart
                pstmtDM.executeUpdate();
            }
            // 12. Insert new record into table control.log with event="load to data mart",status="successful"
            //(INSERT INTO logs(event, status) VALUES ('load to data mart','successful'))
            insertLogsProcess("successful", "");
            // Đóng các kết nối
            // 13. Close all connect database
            rs.close();
            stmtDW.close();
            pstmtDM.close();
            connectionDW.close();
            connectionDM.close();
//            connectDBControl.getConnection().close();
            System.out.println("Data transfer from DW to DM completed.");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void updateStatusProcess(String status) {
        String sqlUpdate = "UPDATE logs SET status=? WHERE id=?";
        Connection connectionControl = connectDBControl.getConnection();
        try {
            PreparedStatement pstmtControl = connectionControl.prepareStatement(sqlUpdate);
            pstmtControl.setString(1, status);
            pstmtControl.setInt(2, idLog);
            pstmtControl.executeUpdate();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

//    private void insertLogsProcess() {
//        Connection connection = connectDBControl.getConnection();
//        String sqlInsert = "INSERT INTO logs(event, status) VALUES (?, ?)";
//        try {
//            PreparedStatement preparedStatement = connection.prepareStatement(sqlInsert,Statement.RETURN_GENERATED_KEYS);
//            preparedStatement.setString(1, moduleName);
//            preparedStatement.setString(2, "in process");
//            preparedStatement.executeUpdate();
//            ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
//            if (generatedKeys.next()) {
//                idLog = generatedKeys.getInt(1);
//            }
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//    }

    private void insertLogsProcess(String status, String note) {
        Connection connection = connectDBControl.getConnection();
        String sqlInsert = "INSERT INTO logs(event, status, note) VALUES (?, ?, ?)";
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sqlInsert,Statement.RETURN_GENERATED_KEYS);
            preparedStatement.setString(1, moduleName);
//            preparedStatement.setString(2, "in process");
            preparedStatement.setString(2, status);
            preparedStatement.setString(3, note);
            preparedStatement.executeUpdate();
            ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
            if (generatedKeys.next()) {
                idLog = generatedKeys.getInt(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    private void renameTable(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        // Đổi tên bảng present_news_articles => articles_temp
        String queryRenameToTemp = "RENAME TABLE present_news_articles TO articles_temp";
        statement.executeUpdate(queryRenameToTemp);
        // Đổi tên bảng new_articles_temp => present_news_articles
        String queryRenameToPresent = "RENAME TABLE news_articles_temp TO present_news_articles";
        statement.executeUpdate(queryRenameToPresent);
        // Đổi tên bảng articles_temp => news_articles_temp
        String queryRenameToArticleTemp = "RENAME TABLE articles_temp TO news_articles_temp";
        statement.executeUpdate(queryRenameToArticleTemp);
        // Xóa dữ liệu bảng new_articles_temp
        String truncateQuery = "TRUNCATE TABLE news_articles_temp";
        statement.executeUpdate(truncateQuery);
        System.out.println("Đã đổi tên bảng thành công!");
    }

    private String createQueryInsertToDataMart(String tableName) {
        String valuesField = "";
        String values = "";
        String[] columnsArr = columns.split(",");
        for(int i=0; i< columnsArr.length - 1; i++){
            valuesField += "`"+columnsArr[i]+"`,";
            values += "?,";
        }
        valuesField += "`"+columnsArr[columnsArr.length-1]+"`";
        values += "?";
        String result = "INSERT INTO "+ tableName +"("+valuesField+") VALUES ("+values+")" ;
        return result;
    }

    public static void main(String[] args) {
        ConfigReader configReader = new ConfigReader();
        Main main = new Main(configReader);
        // 1. Run LoadDataMart.jar
        main.executeApp();
    }
}