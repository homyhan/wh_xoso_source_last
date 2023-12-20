package org.example;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ConfigReader {
    private Properties properties;
    public enum ConfigurationProperty {
        STAGING_CONTROL_URL("StagingControl.url"),
        STAGING_CONTROL_USERNAME("StagingControl.username"),
        STAGING_CONTROL_PASSWORD("StagingControl.password"),
        DATA_WAREHOUSE_URL("DataWarehouse.url"),
        DATA_WAREHOUSE_USERNAME("DataWarehouse.username"),
        DATA_WAREHOUSE_PASSWORD("DataWarehouse.password"),
        DATA_WAREHOUSE_INSERT_DIM_DATE("DataWarehouse.Insert.DimDate"),
        DATA_WAREHOUSE_INSERT_DIM_AREA("DataWarehouse.Insert.DimArea"),
        DATA_WAREHOUSE_INSERT_DIM_PRIZE("DataWarehouse.Insert.DimPrize"),
        DATA_WAREHOUSE_INSERT_DIM_PROVINCE("DataWarehouse.Insert.DimProvince"),
        DATA_WAREHOUSE_INSERT_FACT_LOTTERY("DataWarehouse.Insert.FactLottery"),
        STAGING_URL("Staging.url"),
        STAGING_USERNAME("Staging.username"),
        STAGING_PASSWORD("Staging.password"),
        STAGING_QUERY_DATA("Staging.QueryData"),
        MODULE_LOAD_TO_DATA_WAREHOUSE("Module.LoadToDataWarehouse"),
        MODULE_COLUMNS_LOTTERY_AGGREGATE("Module.Columns.Lottery_Aggregate"),
        MODULE_FILE_LOGS_ERROR("Module.FileLogsError"),
        MODULE_PREVIOUS_MODULE("Module.PreviousModule");

        private final String propertyName;

        ConfigurationProperty(String propertyName) {
            this.propertyName = propertyName;
        }

        public String getPropertyName() {
            return propertyName;
        }
    }

    public ConfigReader() {
        properties = new Properties();
        try {
            properties.load(new FileInputStream("./Config.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public String getProperty(String propertyName) {
        return properties.getProperty(propertyName);
    }

    public static void main(String[] args) {

    }
}
