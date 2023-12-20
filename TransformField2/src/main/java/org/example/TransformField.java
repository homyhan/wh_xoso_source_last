package org.example;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TransformField {

    private ConfigReader configReader;
    static String urlControl;
    String userControl;
    String passControl;
    String urlStaging;
    String userStaging;
    String passStaging;
    String moduleLoad;
    String moduleTransform;
    String moduleProcess;
    String moduleSuccess;
    String filePathLogs;
    Connect controllerConnection;
    String moduleFile;

    public TransformField(ConfigReader configReader){
        this.configReader = configReader;
        loadConfig();
    }

    // 2. Load module config
    public void loadConfig(){
        urlControl = configReader.getProperty(ConfigReader.ConfigurationProperty.STAGING_CONTROL_URL.getPropertyName());
        userControl = configReader.getProperty(ConfigReader.ConfigurationProperty.STAGING_CONTROL_USERNAME.getPropertyName());
        passControl = configReader.getProperty(ConfigReader.ConfigurationProperty.STAGING_CONTROL_PASSWORD.getPropertyName());

        urlStaging = configReader.getProperty(ConfigReader.ConfigurationProperty.STAGING_CONTROL_URL.getPropertyName());
        userStaging = configReader.getProperty(ConfigReader.ConfigurationProperty.STAGING_USERNAME.getPropertyName());
        passStaging = configReader.getProperty(ConfigReader.ConfigurationProperty.STAGING_PASSWORD.getPropertyName());

        moduleLoad = configReader.getProperty(ConfigReader.ConfigurationProperty.MODULE_LOAD_STAGING.getPropertyName());
        moduleTransform = configReader.getProperty(ConfigReader.ConfigurationProperty.MODULE_TRANSFORM.getPropertyName());
        moduleProcess = configReader.getProperty(ConfigReader.ConfigurationProperty.MODULE_PROCESS.getPropertyName());
        moduleSuccess = configReader.getProperty(ConfigReader.ConfigurationProperty.MODULE_SUCCESS.getPropertyName());
        moduleFile = configReader.getProperty(ConfigReader.ConfigurationProperty.MODULE_FILE_LOGS_ERROR.getPropertyName());
    }

    public boolean checkPreviousProgress(){
        boolean result;
        // 3. Connect to database control
        controllerConnection = new Connect(urlControl, userControl, passControl);
        Connection connectControl = controllerConnection.getConnection();

        // 4.Checking connection to database control
        if (connectControl == null){

            // 4.1.Insert new record failed into file log
            controllerConnection.writeLogToFile(moduleFile, "fail", "connect control failed");
            return false; // kết thúc
        }

        // 4.2. Select * from control.logs where event = "Load data to Staging" and create_at = CURRENT_DATE() and status = "successful"
        String query = "SELECT * FROM control2.logs WHERE event = '" + moduleLoad + "' AND create_at = CURRENT_DATE() AND status = '" + moduleSuccess + "'";
        try{
            Statement stmt = connectControl.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            ResultSetMetaData metaData = rs.getMetaData();
            int column = metaData.getColumnCount();

            // 5. Check query results
            if (rs.next()){
                result = true;
                String test = "";
                for(int i = 1; i <= column; i++){
                    String columnName = metaData.getColumnName(i);
                    String data = rs.getString(columnName);
                    test += columnName + " " + data + "\t";
                }
            } else{
                result = false;
            }
        } catch(SQLException e){
            throw new RuntimeException();
        }
        return result;
    }

    public void execute(){
        // result = false -> kết thúc
        if (!checkPreviousProgress()) {
            return;
        }

        // result = true
        // 6. Insert into control.logs(event, status) values ('Transform field', 'in process')
        insertLogProcess();

        // 7. Connect database staging
        Connect connectStaging = new Connect(urlStaging, userStaging, passStaging);

        try {
            Connection connectionStaging = connectStaging.getConnection();

            // 8. Checking connection to database staging
            // fail
            if (connectionStaging == null) {

                // 8.1. Insert control.logs(event, status, note) values ('Transform field', 'fail', 'connect to staging failed')
                writeLog();

                // kết thúc
                return;
            }

            // 9. Truncate table xoso and area
            truncateTableXoso();
            truncateTableArea();
            System.out.println("Tables 'Xoso' and 'Area' truncated successfully.");

            // 10. Get rows form StagingNews table
            ResultSet stagingXosoResultSet = getStagingXosoRows(connectionStaging);

            // 11. Insert into Xoso and area tables
            insertIntoXosoAndArea(stagingXosoResultSet, connectionStaging);

            // 12. Insert into control.logs(event, status, create_at) values ('Transform', 'successful', current_date())
            insertLogSuccess();

            // 13. Close all connect
            connectionStaging.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void insertLogProcess(){
        Connection connection = controllerConnection.getConnection();
        String query = "INSERT INTO control2.logs(event, status) VALUES (?, ?)";
        try{
            PreparedStatement pre = connection.prepareStatement(query);
            pre.setString(1, moduleTransform);
            pre.setString(2, moduleProcess);
            pre.executeUpdate();
            pre.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void insertLogSuccess(){
        Connection connection = controllerConnection.getConnection();
        String query = "INSERT INTO control2.logs(event, status, create_at) VALUES (?, ?, current_date())";
        try{
            PreparedStatement pre = connection.prepareStatement(query);
            pre.setString(1, moduleTransform);
            pre.setString(2, moduleSuccess);
            pre.executeUpdate();
            pre.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void writeLog(){
        Connection connection = controllerConnection.getConnection();
        String query = "INSERT control2.logs(event, status, note) VALUES (?, ?, ?)";
        try{
            PreparedStatement pre = connection.prepareStatement(query);
            pre.setString(1, moduleTransform);
            pre.setString(2, "fail");
            pre.setString(3, "connect to staging failed");
            pre.executeUpdate();
            pre.close();
        } catch(SQLException e){
            throw new RuntimeException();
        }
    }

    public void truncateTableXoso(){
        String query = "TRUNCATE TABLE `staging2`.`xoso`";
        try{
            Connection connection = controllerConnection.getConnection();
            PreparedStatement pre = connection.prepareStatement(query);
            pre.executeUpdate();
            pre.close();
        } catch(SQLException e){
            throw new RuntimeException();
        }
    }

    public void truncateTableArea() {
        String tableName = "area";
        String query = "TRUNCATE TABLE `staging2`.`" + tableName + "`";
        try {
            Connection connection = controllerConnection.getConnection();
            PreparedStatement pre = connection.prepareStatement(query);
            pre.executeUpdate();
            pre.close();
        } catch (SQLException e) {
            throw new RuntimeException("Error truncating Area table: " + e.getMessage(), e);
        }
    }

    private static ResultSet getStagingXosoRows(Connection connection) throws SQLException {
        // Get rows from StagingXoso table
        Statement statement = connection.createStatement();
        return statement.executeQuery("SELECT * FROM `staging2`.`StagingXoso`");
    }

    private static void insertIntoXosoAndArea(ResultSet resultSet, Connection connection) throws SQLException {
        // Insert into Xoso and Area tables
        PreparedStatement insertAreaStatement = connection.prepareStatement(
                "INSERT INTO `staging2`.`area` (title, create_at, update_at) VALUES (?, ?, ?)", Statement.RETURN_GENERATED_KEYS);

        PreparedStatement insertXosoStatement = connection.prepareStatement(
                "INSERT INTO `staging2`.`xoso` (province, areaId, date_lottery, name_prize, result, create_at, update_at, create_by, update_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");

        try {
            while (resultSet.next()) {
                String areaTitle = resultSet.getString("area");
                String createAt = resultSet.getString("create_at");
                String updateAt = resultSet.getString("update_at");
                 String dateLotteryString = resultSet.getString("date_lottery");

                 String test =  resultSet.getString("create_at")        ;

                System.out.println("in ra date_lottery");
                System.out.println(dateLotteryString);
                System.out.println("In createat:");
                System.out.println(test);
               int areaId = getAreaId(connection, areaTitle, createAt, updateAt);
                System.out.println(areaId);
                if (areaId == -1) {

                    // Insert into Area
                    insertAreaStatement.setString(1, resultSet.getString("area"));
//                    insertCategoryStatement.setString(2, resultSet.getString("create_at"));
//                    insertCategoryStatement.setString(3, resultSet.getString("update_at"));
                    insertAreaStatement.setTimestamp(2, convertStringToTimestamp(resultSet.getString("create_at")));
                    insertAreaStatement.setTimestamp(3, convertStringToTimestamp(resultSet.getString("update_at")));

                    int affectedRowsArea = insertAreaStatement.executeUpdate();
                    if (affectedRowsArea > 0) {
                        ResultSet generatedKeysArea = insertAreaStatement.getGeneratedKeys();
                        if (generatedKeysArea.next()) {
                            areaId = generatedKeysArea.getInt(1);
                        }
                    } else{
                        areaId = getAreaId(connection, resultSet.getString("area"), resultSet.getString("create_at"), resultSet.getString("update_at"));
                    }
                }
                // Insert into Xoso
                insertXosoStatement.setString(1, resultSet.getString("province"));
                insertXosoStatement.setInt(2, areaId);
                insertXosoStatement.setString(3, resultSet.getString("date_lottery"));
                insertXosoStatement.setString(4, resultSet.getString("name_prize"));
                insertXosoStatement.setString(5, resultSet.getString("result"));
//                insertXosoStatement.setString(6, resultSet.getString("create_at"));
//                insertXosoStatement.setString(7, resultSet.getString("update_at"));
                insertXosoStatement.setTimestamp(6, convertStringToTimestamp(resultSet.getString("create_at")));
                insertXosoStatement.setTimestamp(7, convertStringToTimestamp(resultSet.getString("update_at")));

                insertXosoStatement.setString(8, resultSet.getString("create_by"));
                insertXosoStatement.setString(9, resultSet.getString("update_by"));

                insertXosoStatement.executeUpdate();


            }
        } catch (ParseException e) {
            throw new RuntimeException(e);
        } finally {
            insertXosoStatement.close();
            insertAreaStatement.close();
        }
    }

    private static int getAreaId(Connection connection, String categoryTitle, String createAt, String updateAt) throws SQLException {
        // Check if Area already exists
        PreparedStatement selectAreaStatement = connection.prepareStatement(
//                "SELECT id FROM staging2.area WHERE title = ? AND create_at = ? AND update_at = ?"
                "SELECT id FROM staging2.area WHERE title = ?AND create_at = STR_TO_DATE(?, '%m/%d/%Y %H:%i')AND update_at = STR_TO_DATE(?, '%m/%d/%Y %H:%i');"
        );
        selectAreaStatement.setString(1, categoryTitle);
        selectAreaStatement.setString(2, createAt);
        selectAreaStatement.setString(3, updateAt);

        ResultSet areaResultSet = selectAreaStatement.executeQuery();
                                      System.out.println("Loi lay id");
        if (areaResultSet.next()) {
            // Area already exists, return its ID

            return areaResultSet.getInt("id");
        } else {
            // Area doesn't exist
            return -1;
        }
    }

//    private static Timestamp convertStringToTimestamp(String dateString) throws ParseException {
//        SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm");
//        Date parsedDate = dateFormat.parse(dateString);
//        return new Timestamp(parsedDate.getTime());
//    }

    private static Timestamp convertStringToTimestamp(String dateString) throws ParseException {
        if (dateString != null && !dateString.isEmpty()) {
            SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm");
            Date parsedDate = dateFormat.parse(dateString);
            return new Timestamp(parsedDate.getTime());
        } else {
            // Return an appropriate value when the date string is empty
            return null; // or throw an exception, depending on your requirements
        }
    }


    public static void main(String[] args) {
        ConfigReader conf = new ConfigReader();
        TransformField transform = new TransformField(conf);
        transform.execute();
    }
}
