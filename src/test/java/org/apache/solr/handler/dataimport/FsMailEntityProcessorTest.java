package org.apache.solr.handler.dataimport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.handler.dataimport.DataConfig.Entity;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FsMailEntityProcessorTest {

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  @Test
  @Ignore
  public void test() {
    FsMailEntityProcessor p = new FsMailEntityProcessor();
    Entity entity = new Entity();
    entity.allAttributes = new HashMap<String, String>();
    entity.allAttributes.put("dataDir", System.getProperty("dataDir"));
    entity.allAttributes.put("ignoreFrom", System.getProperty("ignoreFrom"));
    Context ctx = new ContextImpl(entity, new VariableResolverImpl(), null, null, null, null, null);

    p.firstInit(ctx);
    p.init(ctx);
    Map<String, Object> row = null;
    while((row = p.nextRow()) != null) {
      System.out.println(row);
    }
    p.close();
  }

  /**
   * The bug this guards against: mail backfilled today carries an old *received* date in its file
   * name, so selecting on the name skipped it forever. Selection must follow the file's mtime.
   */
  @Test
  public void backfilledFileIsAcceptedDespiteOldNameDate() throws Exception {
    FsMailEntityProcessor p = new FsMailEntityProcessor();
    // received in July, written to disk just now
    File f = file("2026/07/28", "nku_20260728T152056_e5818.mail.gz", minutesAgo(1));

    assertTrue(p.shouldAcceptFile(f, minutesAgo(10)));
  }

  @Test
  public void fileWrittenBeforeSinceIsRejected() throws Exception {
    FsMailEntityProcessor p = new FsMailEntityProcessor();
    // received today, but already indexed by an earlier run
    File f = file("2026/08/13", "dsi_20260813T101915_8ba5c.mail.gz", minutesAgo(30));

    assertFalse(p.shouldAcceptFile(f, minutesAgo(10)));
  }

  @Test
  public void nullSinceAcceptsEverything() throws Exception {
    FsMailEntityProcessor p = new FsMailEntityProcessor();
    File f = file("2013/05/11", "user_20130511T092053_abcde.mail", minutesAgo(500000));

    assertTrue(p.shouldAcceptFile(f, null));
  }

  @Test
  public void nonMailAndUnparseableNamesAreRejected() throws Exception {
    FsMailEntityProcessor p = new FsMailEntityProcessor();
    Date since = minutesAgo(10);

    assertFalse("wrong extension", p.shouldAcceptFile(file("2026/08/13", "notes.txt", minutesAgo(1)), since));
    assertFalse("no date part", p.shouldAcceptFile(file("2026/08/13", "garbage.mail.gz", minutesAgo(1)), since));
    assertFalse("unparseable date", p.shouldAcceptFile(file("2026/08/13", "u_notadate_h.mail.gz", minutesAgo(1)), since));
    assertFalse("directory", p.shouldAcceptFile(tmp.getRoot(), since));
  }

  /**
   * Directories are no longer pruned by the date in their path, so a whole day folder that is old
   * by name still gets walked. Fixing shouldAcceptFile alone would achieve nothing without this.
   */
  @Test
  public void walkDescendsIntoOldDayFoldersAndSelectsByMtime() throws Exception {
    FsMailEntityProcessor p = new FsMailEntityProcessor();
    File backfilled = file("2026/07/28", "nku_20260728T152056_e5818.mail.gz", minutesAgo(1));
    File alreadyIndexed = file("2026/07/28", "nku_20260728T160000_11111.mail.gz", minutesAgo(90));
    File recent = file("2026/08/13", "dsi_20260813T101915_8ba5c.mail.gz", minutesAgo(2));
    file("2025/01/02", "old_20250102T080000_22222.mail.gz", minutesAgo(60 * 24 * 400));

    List<String> found = new ArrayList<String>();
    p.getFolderFiles(tmp.getRoot(), minutesAgo(10), found);

    assertEquals(2, found.size());
    assertTrue(found.contains(backfilled.getAbsolutePath()));
    assertTrue(found.contains(recent.getAbsolutePath()));
    assertFalse(found.contains(alreadyIndexed.getAbsolutePath()));
  }

  @Test
  public void walkWithNullSinceCollectsEveryMailFile() throws Exception {
    FsMailEntityProcessor p = new FsMailEntityProcessor();
    file("2026/07/28", "nku_20260728T152056_e5818.mail.gz", minutesAgo(1));
    file("2025/01/02", "old_20250102T080000_22222.mail.gz", minutesAgo(60 * 24 * 400));
    file("2026/08/13", "notes.txt", minutesAgo(1));

    List<String> found = new ArrayList<String>();
    p.getFolderFiles(tmp.getRoot(), null, found);

    assertEquals(2, found.size());
  }

  /** Creates dataDir/&lt;path&gt;/&lt;name&gt; with the given last modified time. */
  private File file(String path, String name, Date lastModified) throws IOException {
    File dir = new File(tmp.getRoot(), path);
    if (!dir.exists() && !dir.mkdirs()) {
      throw new IOException("Unable to create " + dir);
    }
    File f = new File(dir, name);
    if (!f.createNewFile()) {
      throw new IOException("Unable to create " + f);
    }
    if (!f.setLastModified(lastModified.getTime())) {
      throw new IOException("Unable to set mtime on " + f);
    }
    return f;
  }

  private static Date minutesAgo(int minutes) {
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.MINUTE, -minutes);
    return cal.getTime();
  }

}
