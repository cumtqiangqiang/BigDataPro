package common.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigurationManager {
    private static Properties prop = new Properties();
    static {
        InputStream in = ConfigurationManager.class
                .getClassLoader().getResourceAsStream("my.properties");

        try {
            prop.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static boolean getBoolean(String key){
        String value = prop.getProperty(key);

        try{
            return Boolean.parseBoolean(value);
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;

    }

}
