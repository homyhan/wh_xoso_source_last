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
        STAGING_URL("Staging.url"),
        STAGING_USERNAME("Staging.username"),
        STAGING_PASSWORD("Staging.password"),
        MODULE_FILE_LOGS_ERROR("Module.FileLogsError"),
        MODULE_LOAD_STAGING("Module.LoadToStaging"),
        MODULE_TRANSFORM("Module.Transform"),
        MODULE_PROCESS("Module.process"),
        MODULE_SUCCESS("Module.success");

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
            properties.load(new FileInputStream("D:\\HAN\\WH\\TransformField2\\src\\main\\java\\org\\example\\Config.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public String getProperty(String propertyName) {
        return properties.getProperty(propertyName);
    }

    public static void main(String[] args) {
        String propertyName = ConfigurationProperty.STAGING_CONTROL_URL.getPropertyName();
        System.out.println("Property name: " + propertyName);
    }
}
