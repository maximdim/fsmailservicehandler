package org.apache.solr.handler.dataimport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.Session;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FsMailEntityProcessorTest {

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

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

  /**
   * A signed message carries a pkcs7 part Tika refuses to parse. That used to throw out of
   * addPartToDocument and cost us the entire message - sender, subject, body and all. Only the
   * unreadable part should be lost. Observed live: 3 of 3 sampled S/MIME messages were on disk
   * and absent from Solr.
   */
  @Test
  public void unreadablePartDoesNotDiscardTheMessage() throws Exception {
    Session session = Session.getDefaultInstance(new Properties(), null);
    MimeMessage msg = new MimeMessage(session);
    msg.setFrom(new InternetAddress("alice@example.com"));
    msg.setRecipients(Message.RecipientType.TO, "bob@example.com");
    msg.setSubject("signed hello");
    msg.setSentDate(new Date());

    MimeBodyPart body = new MimeBodyPart();
    body.setText("the readable body");

    MimeBodyPart signature = new MimeBodyPart() {
      @Override
      public InputStream getInputStream() throws IOException {
        throw new IOException("cannot parse detached pkcs7 signature (no signed data to parse)");
      }
    };
    signature.setContent("garbage", "text/plain");
    signature.setFileName("smime.p7s");

    MimeMultipart mp = new MimeMultipart("signed");
    mp.addBodyPart(body);
    mp.addBodyPart(signature);
    msg.setContent(mp);
    msg.saveChanges();

    Map<String, Object> row = new HashMap<String, Object>();
    assertTrue(new FsMailEntityProcessor().addPartToDocument(msg, row, true));

    assertEquals("alice@example.com", row.get("from_clean"));
    assertEquals("signed hello", row.get("subject"));
    assertTrue("readable part should survive", contentOf(row).contains("the readable body"));
  }

  /** Same guarantee when the message itself is the unreadable part - envelope still indexes. */
  @Test
  public void unreadableBodyStillIndexesTheEnvelope() throws Exception {
    Session session = Session.getDefaultInstance(new Properties(), null);
    MimeMessage msg = new MimeMessage(session) {
      @Override
      public InputStream getInputStream() throws IOException {
        throw new IOException("unreadable");
      }
    };
    msg.setFrom(new InternetAddress("alice@example.com"));
    msg.setRecipients(Message.RecipientType.TO, "bob@example.com");
    msg.setSubject("broken body");
    msg.setText("never readable");
    msg.saveChanges();

    Map<String, Object> row = new HashMap<String, Object>();
    assertTrue(new FsMailEntityProcessor().addPartToDocument(msg, row, true));

    assertEquals("broken body", row.get("subject"));
    assertEquals("alice@example.com", row.get("from_clean"));
  }

  @SuppressWarnings("unchecked")
  private static String contentOf(Map<String, Object> row) {
    List<String> content = (List<String>) row.get("content");
    return content == null ? "" : content.toString();
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
