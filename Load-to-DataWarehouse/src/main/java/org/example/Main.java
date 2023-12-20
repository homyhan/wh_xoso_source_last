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

    String urlStaging;
    String userStaging;
    String passStaging;
    String moduleName;
    String columns;
    String columnDimDate;
    String columnDimArea;
    String columnDimPrize;
    String columnDimProvince;
    String columnsFactLottery;
    String filePathLogs;
    String previousModule;
    int idLog;
    public Main(ConfigReader configReader) {
        this.configReader = configReader;
        // 2. Load config module
        loadConfig();
    }
    // 2. load config module
    public void loadConfig() {
        // load config module
        moduleName = configReader.getProperty(ConfigReader.ConfigurationProperty.MODULE_LOAD_TO_DATA_WAREHOUSE.getPropertyName());
        columns = configReader.getProperty(ConfigReader.ConfigurationProperty.MODULE_COLUMNS_LOTTERY_AGGREGATE.getPropertyName());
        columnDimDate = configReader.getProperty(ConfigReader.ConfigurationProperty.DATA_WAREHOUSE_INSERT_DIM_DATE.getPropertyName());
        columnDimArea = configReader.getProperty(ConfigReader.ConfigurationProperty.DATA_WAREHOUSE_INSERT_DIM_AREA.getPropertyName());
        columnDimPrize = configReader.getProperty(ConfigReader.ConfigurationProperty.DATA_WAREHOUSE_INSERT_DIM_PRIZE.getPropertyName());
        columnDimProvince = configReader.getProperty(ConfigReader.ConfigurationProperty.DATA_WAREHOUSE_INSERT_DIM_PROVINCE.getPropertyName());
        columnsFactLottery = configReader.getProperty(ConfigReader.ConfigurationProperty.DATA_WAREHOUSE_INSERT_FACT_LOTTERY.getPropertyName());
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

        // load config db Staging
        urlStaging = configReader.getProperty(ConfigReader.ConfigurationProperty.STAGING_URL.getPropertyName());
        userStaging = configReader.getProperty(ConfigReader.ConfigurationProperty.STAGING_USERNAME.getPropertyName());
        passStaging = configReader.getProperty(ConfigReader.ConfigurationProperty.STAGING_PASSWORD.getPropertyName());
    }
    public boolean checkPreviousProgress(){
        boolean result = false;
        // 3. connect database control
        connectDBControl = new ConnectDB(urlControl, userControl, passControl, filePathLogs);
        Connection connectionControl = connectDBControl.getConnection();
        // 4. Checking connection to database control
        if(connectionControl == null){
            //4.1 Insert new record failed into file log
            // ghi log vào file nếu kết nối thất bại
            connectDBControl.writeLogToFile(filePathLogs, "fail", "connect control failed");
            return false; //  kết thúc chương trình
        }
        // 4.2 Select  * from logs where event = "Transform field" and DATE(create_at) = CURDATE() and status="successful"
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
        // 5.1 Select  * from logs where event = "load to data warehouse" and DATE(create_at) = CURDATE() and status="successful"
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
        // 6.1 Insert new record into table control.log with event="load to data warehouse",status="in process" (INSERT INTO logs(event, status) VALUES ('Load to data warehouse','in process'))
        insertLogsProcess("in process", "");

        try {
            // 7. Connect database staging
            ConnectDB connectStaging = new ConnectDB(urlStaging, userStaging, passStaging, filePathLogs, idLog, connectDBControl.getConnection());
            Connection connectionDM = connectStaging.getConnection();
            // 8. Checking connection to staging
            if(connectionDM == null) {
                // 8. 1 Insert new record into table control.log with event="load to data warehouse",status="fail", note="content error"
                //(INSERT INTO logs(event, status,note) VALUES ('load to data warehouse','fail', 'connect database staging failed'))
                insertLogsProcess("fail", "connect staging failed");
                return;
            }
            // Truy vấn SQL để lấy dữ liệu từ bảng trong Staging
            // 8.2 Get data in  staging.xoso, staging.area
            String sqlSelect = configReader.getProperty(ConfigReader.ConfigurationProperty.STAGING_QUERY_DATA.getPropertyName());
            Statement stmtDW = connectionDM.createStatement();
            ResultSet rs = stmtDW.executeQuery(sqlSelect);
            // 9. Connect database data_warehouse
            ConnectDB connectDW = new ConnectDB(urlDW, userDW, passDW, filePathLogs, idLog, connectDBControl.getConnection());
            Connection connectionDW = connectDW.getConnection();
            // 10. Checking connection to data_warehouse
            if(connectionDW == null) {
                //10.1 Insert new record into table control.log with event="load to data warehouse",status="fail", note="content error"
                //(INSERT INTO logs(event, status,note) VALUES ('load to data warehouse','fail', 'connect data_warehouse failed'))
                insertLogsProcess("fail", "connect data_warehouse failed");
                return;
            }
            // PreparedStatement để chèn dữ liệu vào data_warehouse
            String sqlInsertDimDate = createQueryInsertDimTime("dimdate", columnDimDate);
            String sqlInsertDimArea = createQueryInsertDim("dimarea", columnDimArea);
            String sqlInsertDimPrize = createQueryInsertDim("dimprize", columnDimPrize);
            String sqlInsertDimProvince = createQueryInsertDim("dimprovince", columnDimProvince);
            String sqlInsertFactLottery = createQueryInsertDim("factlottery", columnsFactLottery);
            PreparedStatement pstmtDimDate = connectionDW.prepareStatement(sqlInsertDimDate, Statement.RETURN_GENERATED_KEYS);
            PreparedStatement pstmtDimArea = connectionDW.prepareStatement(sqlInsertDimArea, Statement.RETURN_GENERATED_KEYS);
            PreparedStatement pstmtDimPrize = connectionDW.prepareStatement(sqlInsertDimPrize, Statement.RETURN_GENERATED_KEYS);
            PreparedStatement pstmtDimProvince = connectionDW.prepareStatement(sqlInsertDimProvince, Statement.RETURN_GENERATED_KEYS);
            PreparedStatement pstmtFactLottery = connectionDW.prepareStatement(sqlInsertFactLottery, Statement.RETURN_GENERATED_KEYS);
            String[] columnsArr = columns.split(",");
            String[] columnsArrDimDate = columnDimDate.split(",");
            String[] columnsArrDimArea = columnDimArea.split(",");
            String[] columnsArrDimProvince = columnDimProvince.split(",");
            String[] columnsArrDimPrize = columnDimPrize.split(",");
            String[] columnFactLottery = columnsFactLottery.split(",");
            ResultSet generatedKeys;
            // Duyệt qua kết quả từ staging và chèn vào data_warehouse
//            10.2 Insert rows into table dimDate, dimaArea, dimPrize, dimProvince, factLotery
            while (rs.next()) {
                int dimDateId =0, dimAreaId = 0, dimProvinceId = 0, dimPrizeId = 0;
                // Lấy dữ liệu từ kết quả truy vấn DW và chèn vào DM
                for(int i=0; i< columnsArrDimProvince.length; i++){
                    String column = "province";
                    pstmtDimProvince.setString(i+1, rs.getString(column));
                }
                pstmtDimProvince.executeUpdate();
                generatedKeys = pstmtDimProvince.getGeneratedKeys();
                if (generatedKeys.next()) {
                    dimProvinceId = generatedKeys.getInt(1);
                }
                pstmtFactLottery.setInt(1, dimProvinceId);
                System.out.println(dimProvinceId);

                String columnDate = columnsArrDimDate[0];
                String date = rs.getString(columnDate);
                pstmtDimDate.setString(1, date);
                pstmtDimDate.setString(2, date);
                pstmtDimDate.setString(3, date);
                pstmtDimDate.setString(4, date);
                pstmtDimDate.executeUpdate();
                generatedKeys = pstmtDimDate.getGeneratedKeys();
                if (generatedKeys.next()) {
                    dimDateId = generatedKeys.getInt(1);
                }
                pstmtFactLottery.setInt(2, dimDateId);

                for(int i=0; i< columnsArrDimPrize.length; i++){
                    String column = "name_prize";
                    pstmtDimPrize.setString(i+1, rs.getString(column));
                }
                pstmtDimPrize.executeUpdate();
                generatedKeys = pstmtDimPrize.getGeneratedKeys();
                if (generatedKeys.next()) {
                    dimPrizeId = generatedKeys.getInt(1);
                }
                pstmtFactLottery.setInt(3, dimPrizeId);
                pstmtFactLottery.setString(4, rs.getString("result"));
                for(int i=0; i< columnsArrDimArea.length; i++){
                    String column = "area";
                    pstmtDimArea.setString(i+1, rs.getString(column));
                }
                pstmtDimArea.executeUpdate();
                generatedKeys = pstmtDimArea.getGeneratedKeys();
                if (generatedKeys.next()) {
                    dimAreaId = generatedKeys.getInt(1);
                }
                pstmtFactLottery.setInt(5, dimAreaId);
                pstmtFactLottery.executeUpdate();
                // Thực hiện chèn dữ liệu vào Data Mart
            }
            // 11. Insert new record into table control.log with event="load to data warehouse",status="successful"
            //(INSERT INTO logs(event, status) VALUES ('load to data warehouse','successful'))
            insertLogsProcess("successful", "");
            // Đóng các kết nối
            // 12. Close all connect database
            rs.close();
            stmtDW.close();
            pstmtDimDate.close();
            connectionDW.close();
            connectionDM.close();
//            connectDBControl.getConnection().close();
            System.out.println("Data transfer from Staging to DW completed.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private String createQueryInsertDim(String tableName, String columns) {
        String valuesField = "";
        String values = "";
//        String[] columnsArr = columnDimTime.split(",");
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

    private String createQueryInsertDimTime(String tableName, String columns) {
        String valuesField = "";
        String values = "";
        String[] columnsArr = columns.split(",");
        for(int i=0; i< columnsArr.length - 1; i++){
            valuesField += "`"+columnsArr[i]+"`,";

        }
        values += "?,DAY(?),MONTH(?), YEAR(?)";
        valuesField += "`"+columnsArr[columnsArr.length-1]+"`";
        String result = "INSERT INTO "+ tableName +"("+valuesField+") VALUES ("+values+")" ;
        return result;
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
        // 1. Run LoadDwTool.jar
        main.executeApp();
    }
}