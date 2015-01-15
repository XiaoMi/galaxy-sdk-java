package com.xiaomi.infra.galaxy.sds.android.examples;

import android.app.Activity;
import android.os.Bundle;
import android.os.Environment;
import android.view.Menu;
import de.mindpipe.android.logging.log4j.LogConfigurator;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class SdsBasicDemo extends Activity {

  private final Logger LOG = LoggerFactory.getLogger(SdsBasicDemo.class);
  private static String tableName = "android-example-table";
  private static String endpoint = "http://cnbj-s0.sds.api.xiaomi.com";

  public static void configure() {
    final LogConfigurator logConfigurator = new LogConfigurator();
    logConfigurator
        .setFileName(Environment.getExternalStorageDirectory() + File.separator + "SdsBasicDemo.log");
    logConfigurator.setRootLevel(Level.INFO);
    logConfigurator.configure();
  }
  /**
   * Called when the activity is first created.
   *
   * @param savedInstanceState If the activity is being re-initialized after
   *                           previously being shut down then this Bundle contains the data it most
   *                           recently supplied in onSaveInstanceState(Bundle). <b>Note: Otherwise it is null.</b>
   */
  @Override
  public void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);
    configure();
    Runnable sdsAccess = new Runnable() {
      @Override public void run() {
        try {
          TableCreator tableCreator = new TableCreator(tableName, endpoint);
          tableCreator.createTable();
          TableAccessor tableAccessor = new TableAccessor(tableName, endpoint);
          tableAccessor.putData();
          tableAccessor.getData();
          tableAccessor.scanData();
        } catch (Exception e) {
          LOG.error("Some errors occur when accesses sds");
          throw new RuntimeException ("Some errors occur when accesses sds");
        }
      }
    };
    new Thread(sdsAccess).start();
    setContentView(R.layout.activity_main);
  }

  @Override
  public boolean onCreateOptionsMenu(Menu menu) {
    // Inflate the menu; this adds items to the action bar if it is present.
    getMenuInflater().inflate(com.xiaomi.infra.galaxy.sds.android.examples.R.menu.main, menu);
    return true;
  }

}

