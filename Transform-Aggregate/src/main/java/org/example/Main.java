package org.example;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Main {
    private ConfigReader configReader;
    private ConnectDB connectDBControl;
    String urlControl;
    String userControl;
    String passControl;

    String urlDW;
    String userDW;
    String passDW;
    String moduleName;
    String columns;
    String filePathLogs;
    String previousModule;
    String sqlModuleDefault;
    String dateFrom;
    String dateTo;
    int idLog;
    public Main(ConfigReader configReader) {
        this.configReader = configReader;
        // 3.2 Load config module
        loadConfig();
    }
    // 3.2 Load config module
    public void loadConfig() {
        // load config module
        moduleName = configReader.getProperty(ConfigReader.ConfigurationProperty.MODULE_TRANSFORM_AGGREGATE.getPropertyName());
        columns = configReader.getProperty(ConfigReader.ConfigurationProperty.MODULE_COLUMNS_NEW_LOTTERY.getPropertyName());
        filePathLogs = configReader.getProperty(ConfigReader.ConfigurationProperty.MODULE_FILE_LOGS_ERROR.getPropertyName());
        previousModule = configReader.getProperty(ConfigReader.ConfigurationProperty.MODULE_PREVIOUS_MODULE.getPropertyName());
        sqlModuleDefault = configReader.getProperty(ConfigReader.ConfigurationProperty.MODULE_QUERY_GET_LOTTERY_DEFAULT.getPropertyName());

        // load config dbControl
        urlControl = configReader.getProperty(ConfigReader.ConfigurationProperty.STAGING_CONTROL_URL.getPropertyName());
        userControl = configReader.getProperty(ConfigReader.ConfigurationProperty.STAGING_CONTROL_USERNAME.getPropertyName());
        passControl = configReader.getProperty(ConfigReader.ConfigurationProperty.STAGING_CONTROL_PASSWORD.getPropertyName());

        // load config db DW
        urlDW = configReader.getProperty(ConfigReader.ConfigurationProperty.DATA_WAREHOUSE_URL.getPropertyName());
        userDW = configReader.getProperty(ConfigReader.ConfigurationProperty.DATA_WAREHOUSE_USERNAME.getPropertyName());
        passDW= configReader.getProperty(ConfigReader.ConfigurationProperty.DATA_WAREHOUSE_PASSWORD.getPropertyName());
    }
    public boolean checkPreviousProgress(){
        boolean result = false;
        // 4. connect database control
        connectDBControl = new ConnectDB(urlControl,userControl, passControl, filePathLogs);
        Connection connectionControl = connectDBControl.getConnection();
        System.out.println(connectionControl);
        // 5. Checking connection to database control
        if(connectionControl == null){
            //5.1 Insert new record failed into file log
            // ghi log vào file nếu kết nối thất bại
            System.out.println("URL control: "+urlControl);
            connectDBControl.writeLogToFile(filePathLogs, "fail", "connect control failed");
            return false; //  kết thúc chương trình
        }
        // 5.2 Select  * from logs where event = "transform aggregate" and DATE(create_at) = CURDATE() and status="successful"
        String queryPreviousProcess = "SELECT * FROM logs where event='" + previousModule + "' AND DATE(create_at) = CURDATE() AND status='successful'";
        try {
            Statement stmtControl = connectionControl.createStatement();
            ResultSet rs = stmtControl.executeQuery(queryPreviousProcess);
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            // 6. Check query results
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
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    public void executeApp() {
        // 6. Check query results
        if (!checkPreviousProgress()) {
            return;
        }
        // 7. Check query results
        if (checkProcessEverRun()) {
            return;
        }
        // insert logs
        // 7.1 Insert new record into table control.log with event="transform aggregate",status="in process"
        //(INSERT INTO logs(event, status) VALUES ('transform aggregate','in process'))
        insertLogsProcess("in process", "");
        // 8. Connect database data_warehouse
        ConnectDB connectDW = new ConnectDB(urlDW, userDW, passDW, filePathLogs, idLog, connectDBControl.getConnection());
        try {
            Connection connectionDW = connectDW.getConnection();
            // 9. Checking connection to data_warehouse
            if(connectionDW == null) {

               //9.1 Insert new record into table control.log with event="transform aggregate",status="fail", note="content error"
              
                //(INSERT INTO logs(event, status, note) VALUES ('transform aggregate','fail', 'connect data_warehouse failed'))
//                connectDW.writeLogs();
                insertLogsProcess("fail", "connect data_warehouse failed");
                return;
            }
            // Kết nối đến Data Mart
            // Truy vấn SQL để lấy dữ liệu từ bảng trong Data Warehouse
            // 9.2 Query get rows in table dimDate, dimaArea, dimPrize, dimProvince, factLotery
            String sqlSelect = createQuerySelectData();
            System.out.println("SQL: " +  sqlSelect);
            Statement stmtDW = connectionDW.createStatement();
            ResultSet rs = stmtDW.executeQuery(sqlSelect);

            // PreparedStatement để chèn dữ liệu vào table lottery_aggregate
            String sqlInsert = createQueryInsertToNewsArticles("lottery_aggregate");
            System.out.println(sqlInsert);
            PreparedStatement pstmtDW = connectionDW.prepareStatement(sqlInsert);
            String[] columnsArr = columns.split(",");
            // 10. Insert rows into table lottery_aggregate
            while (rs.next()) {
                // Lấy dữ liệu từ kết quả truy vấn DW và chèn vào DM
                for(int i=0; i< columnsArr.length; i++){
                    String column = columnsArr[i];
                    pstmtDW.setString(i+1, rs.getString(column));
                }
                // Thực hiện chèn dữ liệu vào Data Mart
                pstmtDW.executeUpdate();
            }
            // 11. Insert new record into table control.log with event="transform aggregate",status="successful",
            //(INSERT INTO logs(event, status) VALUES ('transform aggregate','successful'))
            insertLogsProcess("successful", "");
            // Đóng các kết nối
            // 12. Close all connect database
            rs.close();
            stmtDW.close();
            pstmtDW.close();
            connectionDW.close();
            System.out.println("Data transform aggregate completed.");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private boolean checkProcessEverRun() {
        boolean result = false;
        Connection connectionControl = connectDBControl.getConnection();
        // 6.1 Select  * from logs where event = "transform aggregate" and DATE(create_at) = CURDATE() and status="successful"
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

    private String createQuerySelectData() {
        String sql = sqlModuleDefault;
        if(dateFrom != null && dateTo != null){
            sql += " AND d.full_date BETWEEN '"+dateFrom+"'" + " AND " + "'" +dateTo+"'";
            return sql;
        }
        if(dateFrom != null){
            sql += " AND DATE(d.full_date) = " + "'"+dateFrom+"'";
            return sql;
        }
        sql += " AND DATE(d.full_date) = CURDATE() ";
        return sql;
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

    private String createQueryInsertToNewsArticles(String tableName) {
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
    public static boolean checkFormatDate(String date) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        dateFormat.setLenient(false);
        try {
            dateFormat.parse(date);
            return true;
        } catch (ParseException e) {
            return false;
        }
    }

    public String getDateFrom() {
        return dateFrom;
    }

    public void setDateFrom(String dateFrom) {
        this.dateFrom = dateFrom;
    }

    public String getDateTo() {
        return dateTo;
    }

    public void setDateTo(String dateTo) {
        this.dateTo = dateTo;
    }

    public static void main(String[] args) {
        // 2. Check length parameter module (parameter.length > 2)
        if(args.length > 2){
            // 2.1 Display error
            System.out.print("Error command: \n" + "command only 2 parameter");
            return;
        }
        ConfigReader configReader = new ConfigReader();
        Main main = new Main(configReader);
        // 3. Check args.length == 0
        if(args.length == 0){
            main.executeApp();
            return;
        }
        if(args.length == 1){
            // 3.1 Get parameter module
            String date = args[0];
            // 3.1.1 Check format parameter
            //(format parameter is yyyy-mm-dd)
            if(!checkFormatDate(date)){
                // 3.1.2 Display error
                System.out.print("Error command: \n" + "example: java -jar Transform-Aggregate.jar yyyy-mm-dd");
                return;
            }
            main.setDateFrom(date);
            main.executeApp();
            return;
        }
        if(args.length == 2){
            // 3.1 Get parameter module
            String dateFrom = args[0];
            String dateTo = args[1];
            // 3.1.1 Check format parameter
            //(format parameter is yyyy-mm-dd)
            if(!checkFormatDate(dateFrom) || !checkFormatDate(dateTo)){
                // 3.1.2 Display error
                System.out.print("Error command: \n" + "example: java -jar Transform-Aggregate.jar yyyy-mm-dd yyyy-mm-dd");
                return;
            }
            main.setDateFrom(dateFrom);
            main.setDateTo(dateTo);
            main.executeApp();
        }
    }
}