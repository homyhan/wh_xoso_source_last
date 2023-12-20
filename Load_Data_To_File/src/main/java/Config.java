import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {
    private Properties properties;
    public enum ConfigurationProperty {
        URL("url"),
        USERNAME("username"),
        PASSWORD("password"),
        DRIVER_CLASS_NAME("driverClassName"),
        MODULE_LOAD_FILE("Module.LoadDataToFile"),
        MODULE_PROCESS("Module.process"),
        MODULE_SUCCESS("Module.success"),
        CSV("csv"),
        CSVFILE_PATH("csvFilePath");


        private final String propertyName;

        ConfigurationProperty(String propertyName) {
            this.propertyName = propertyName;
        }

        public String getPropertyName() {
            return propertyName;
        }
    }

    public Config() {
        properties = new Properties();
        try {
            InputStream input = new FileInputStream("D:\\HK1NAM4\\DW_GK\\Load_Data_To_File\\src\\main\\java\\config.properties");
            properties.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getProperty(String propertyName) {
        return properties.getProperty(propertyName);
    }

    public static void main(String[] args) {
        String propertyName = ConfigurationProperty.URL.getPropertyName();
        System.out.println("Property name: " + propertyName);
    }
}
