/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.lcs.log.core.file;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.TreeMap;

import com.google.common.base.Preconditions;

import com.xiaomi.infra.galaxy.lcs.log.core.LoggerConstants;


public class FileUtils {
  public static TreeMap<String, File> listFile(String topicPath, final String topicName) {
    File path = new File(topicPath);
    File[] fileList = path.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        if (name.startsWith(topicName)) {
          return true;
        }
        return false;
      }
    });

    TreeMap<String, File> fileTreeMap = new TreeMap<String, File>();
    if (fileList == null) {
      return fileTreeMap;
    }

    for (File file : fileList) {
      fileTreeMap.put(file.getName(), file);
    }

    return fileTreeMap;
  }

  public static void createFile(String filePath) throws IOException {
    File file = new File(filePath);
    if (!file.createNewFile()) {
      throw new IOException("create file: " + file.getName() + " failed");
    }
  }

  public static void deleteFile(String filePath) throws IOException {
    File file = new File(filePath);
    if (!file.delete()) {
      throw new IOException("delete file: " + file.getName() + " failed");
    }
  }


  public static String renameTempFilePath(String tempFilePath, String filePath) throws IOException {
    Preconditions.checkArgument(
        isTempFilePath(tempFilePath), "tempFilePath: " + tempFilePath +
            " should end with: " + LoggerConstants.TEMP_FILE_NAME_SUFFIX);
    File tempFile = new File(tempFilePath);
    File file = new File(filePath);

    if (!tempFile.renameTo(file)) {
      throw new IOException("rename tempFileName: " + tempFilePath +
          " to fileName: " + filePath + " failed");
    }

    return filePath;
  }

  public static String formatFileNamePrefix(String topicName) {
    return topicName + "_";
  }

  public static String formatTopicFilePath(String rootFilePath, String topicName) {
    return rootFilePath + File.separator + topicName;
  }

  public static String formatTempTopicFilePath(String topicFilePath) {
    return topicFilePath +
        File.separator + LoggerConstants.TEMP_FILE_PATH;
  }

  public static String formatFilePath(String topicPath, String topicName, long fileIndex) {
    return topicPath + File.separator + formatFileNamePrefix(topicName) +
        String.format(LoggerConstants.FILE_NAME_FORMAT, fileIndex);
  }

  public static String formatTempFilePath(String topicFilePath, String topicName, long fileIndex) {
    String topicTempFilePath = formatTempTopicFilePath(topicFilePath);
    return formatFilePath(topicTempFilePath, topicName, fileIndex) +
        LoggerConstants.TEMP_FILE_NAME_SUFFIX;
  }

  public static boolean isTempFilePath(String filePath) {
    return filePath.endsWith(LoggerConstants.TEMP_FILE_NAME_SUFFIX);
  }

  public static long getFileIndexByFilePath(String filePath) {
    return Long.valueOf(filePath.substring(
        filePath.length() - LoggerConstants.FILE_NAME_FORMAT.length(),
        filePath.length()));
  }

  public static long getFileIndexByTempFilePath(String tempFilePath) {
    Preconditions.checkArgument(
        isTempFilePath(tempFilePath), "tempFilePath: " + tempFilePath +
            " should end with: " + LoggerConstants.TEMP_FILE_NAME_SUFFIX);

    return Long.valueOf(tempFilePath.substring(
        tempFilePath.length() - LoggerConstants.FILE_NAME_FORMAT.length() - LoggerConstants.TEMP_FILE_NAME_SUFFIX.length(),
        tempFilePath.length() - LoggerConstants.TEMP_FILE_NAME_SUFFIX.length()));
  }
}
