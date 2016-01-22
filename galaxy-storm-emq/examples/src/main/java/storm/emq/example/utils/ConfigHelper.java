package storm.emq.example.utils;

import backtype.storm.utils.Utils;

import java.util.Map;

public class ConfigHelper {
    public static String getString(Map conf, String key) throws RuntimeException {
        String value = (String) conf.get(key);
        if (value == null) {
            throw new RuntimeException("Please set value for Key: " + key);
        }

        return value;
    }

    public static String getString(Map conf, String key, String defaultValue) {
        try {
            return getString(conf, key);
        } catch (RuntimeException e) {
            return defaultValue;
        }
    }

    public static Integer getInt(Map conf, String key) {
        Object value = conf.get(key);
        if (value == null) {
            throw new RuntimeException("Please set value for Key: " + key);
        }

        Integer result = Utils.getInt(value);
        if (null == result) {
            throw new IllegalArgumentException("Don't know how to convert null to int");
        }
        return result;
    }

    public static Long getLong(Map conf, String key) {
        Object value = conf.get(key);
        if (value == null) {
            throw new RuntimeException("Please set value for Key: " + key);
        }
        return (Long) value;
    }

    public static Map getTopologyConfig(String confFile) {
        return backtype.storm.utils.Utils.findAndReadConfigFile(confFile, true);
    }
}
