package org.apache.solr.handler.dataimport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
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

  @Test
  public void sinceIsTheLastIndexTimeMovedBackByTheRewind() throws Exception {
    Date since = FsMailEntityProcessor.parseSince("2026-08-13 13:45:49", 10);

    assertEquals(date(2026, 8, 13, 13, 35, 49), since);
  }

  @Test
  public void aZeroRewindLeavesTheLastIndexTimeAlone() throws Exception {
    Date since = FsMailEntityProcessor.parseSince("2026-08-13 13:45:49", 0);

    assertEquals(date(2026, 8, 13, 13, 45, 49), since);
  }

  /** No recorded last index time, or one we can't read - index everything rather than nothing. */
  @Test
  public void missingOrUnreadableLastIndexTimeSelectsEveryFile() throws Exception {
    assertNull("epoch placeholder", FsMailEntityProcessor.parseSince("1969-12-31 19:00:00", 10));
    assertNull("absent", FsMailEntityProcessor.parseSince(null, 10));
    assertNull("unparseable", FsMailEntityProcessor.parseSince("not a date", 10));
  }

  @Test
  public void rewindDefaultsToTenMinutesWhenNotConfigured() throws Exception {
    assertEquals(10, FsMailEntityProcessor.DEFAULT_SINCE_REWIND_MINUTES);
    assertEquals(10, FsMailEntityProcessor.parseRewindMinutes(null));
    assertEquals(10, FsMailEntityProcessor.parseRewindMinutes("  "));
  }

  @Test
  public void configuredRewindOverridesTheDefault() throws Exception {
    assertEquals(45, FsMailEntityProcessor.parseRewindMinutes("45"));
    assertEquals(0, FsMailEntityProcessor.parseRewindMinutes("0"));
    assertEquals(90, FsMailEntityProcessor.parseRewindMinutes(" 90 "));
  }

  /**
   * A negative rewind moves the cutoff forward and skips files nothing has indexed yet, and the
   * import would still report success. It has to fail loudly at startup.
   */
  @Test
  public void aRewindThatWouldSkipFilesFailsTheImport() throws Exception {
    assertRejected("-1");
    assertRejected("sixty");
    assertRejected("60m");
  }

  private static void assertRejected(String raw) {
    try {
      FsMailEntityProcessor.parseRewindMinutes(raw);
      fail("should have rejected sinceRewindMinutes=[" + raw + "]");
    }
    catch (DataImportHandlerException expected) {
      assertTrue(expected.getMessage(), expected.getMessage().contains("sinceRewindMinutes"));
    }
  }

  private static Date date(int year, int month, int day, int hour, int minute, int second) {
    Calendar cal = Calendar.getInstance();
    cal.set(year, month - 1, day, hour, minute, second);
    cal.set(Calendar.MILLISECOND, 0);
    return cal.getTime();
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

  /**
   * The point of dropping Tika: an attachment is recorded by name and its bytes are never read,
   * so nothing in a hostile file can reach a parser.
   */
  @Test
  public void attachmentsAreNamedButNeverOpened() throws Exception {
    Session session = Session.getDefaultInstance(new Properties(), null);
    MimeMessage msg = new MimeMessage(session);
    msg.setFrom(new InternetAddress("alice@example.com"));
    msg.setRecipients(Message.RecipientType.TO, "bob@example.com");
    msg.setSubject("cv attached");
    MimeBodyPart body = new MimeBodyPart();
    body.setText("please find my resume attached");
    MimeMultipart mp = new MimeMultipart();
    mp.addBodyPart(body);
    for (String name : new String[] {"resume.pdf", "evil.xml", "bomb.zip"}) {
      MimeBodyPart attachment = new MimeBodyPart() {
        @Override
        public InputStream getInputStream() {
          throw new AssertionError("attachment " + name + " was opened");
        }
      };
      attachment.setContent("<!DOCTYPE x [<!ENTITY e SYSTEM \"file:///etc/passwd\">]><x>&e;</x>", "application/octet-stream");
      attachment.setFileName(name);
      mp.addBodyPart(attachment);
    }
    msg.setContent(mp);
    msg.saveChanges();

    Map<String, Object> row = new HashMap<String, Object>();
    assertTrue(new FsMailEntityProcessor().addPartToDocument(msg, row, true));

    assertEquals(Arrays.asList("resume.pdf", "evil.xml", "bomb.zip"), row.get("attachmentNames"));
    assertEquals(Arrays.asList("please find my resume attached"), row.get("content"));
    assertFalse("the unused attachment field is no longer filled", row.containsKey("attachment"));
  }

  @Test
  public void anAttachmentWithoutAFileNameAddsNoName() throws Exception {
    Map<String, Object> row = index(
        "Content-Type: multipart/mixed; boundary=b\r\n\r\n"
        + "--b\r\nContent-Type: text/plain\r\n\r\nbody\r\n"
        + "--b\r\nContent-Type: application/pdf\r\nContent-Disposition: attachment\r\n\r\n%PDF-1.4\r\n"
        + "--b--\r\n");

    assertNull(row.get("attachmentNames"));
    assertEquals(Arrays.asList("body"), row.get("content"));
  }

  @Test
  public void aDetachedSignatureIsNotListedAsAnAttachment() throws Exception {
    Map<String, Object> row = index(
        "Content-Type: multipart/signed; protocol=\"application/pkcs7-signature\"; boundary=b\r\n\r\n"
        + "--b\r\nContent-Type: text/plain\r\n\r\nsigned body\r\n"
        + "--b\r\nContent-Type: application/pkcs7-signature; name=smime.p7s\r\n"
        + "Content-Disposition: attachment; filename=smime.p7s\r\nContent-Transfer-Encoding: base64\r\n\r\nMIAGCSqG\r\n"
        + "--b--\r\n");

    assertEquals(Arrays.asList("signed body"), row.get("content"));
    assertNull(row.get("attachmentNames"));
  }

  /** Inline images (signature logos) and the odd inline PDF gave Tika nothing; they are skipped unread. */
  @Test
  public void nonTextInlinePartsAreSkipped() throws Exception {
    Map<String, Object> row = index(
        "Content-Type: multipart/related; boundary=b\r\n\r\n"
        + "--b\r\nContent-Type: text/plain\r\n\r\nsee logo\r\n"
        + "--b\r\nContent-Type: image/png\r\nContent-Disposition: inline; filename=image001.png\r\n"
        + "Content-Transfer-Encoding: base64\r\n\r\niVBORw0KGgo=\r\n"
        + "--b\r\nContent-Type: application/pdf\r\n\r\n%PDF-1.4\r\n"
        + "--b--\r\n");

    assertEquals(Arrays.asList("see logo"), row.get("content"));
    assertNull("inline parts were never listed as attachments", row.get("attachmentNames"));
  }

  /**
   * Every text type is read as plain text, as Tika's text parser read it - calendar invites and
   * bounce reports were searchable - but XML is never parsed, so no entity resolves.
   */
  @Test
  public void otherTextTypesAndReportsAreReadAsPlainText() throws Exception {
    Map<String, Object> row = index(
        "Content-Type: multipart/mixed; boundary=b\r\n\r\n"
        + "--b\r\nContent-Type: text/plain\r\n\r\nthe body\r\n"
        + "--b\r\nContent-Type: text/xml\r\n\r\n<!DOCTYPE x [<!ENTITY e SYSTEM \"file:///etc/passwd\">]><x>&e;</x>\r\n"
        + "--b\r\nContent-Type: text/calendar; method=REQUEST\r\n\r\nSUMMARY:Interview with Bob\r\n"
        + "--b\r\nContent-Type: message/delivery-status\r\n\r\nStatus: 5.1.1\r\n"
        + "--b\r\nContent-Type: application/json\r\n\r\n{\"not\": \"text\"}\r\n"
        + "--b--\r\n");

    String content = contentOf(row);
    assertTrue(content, content.contains("the body"));
    assertTrue(content, content.contains("&e;"));
    assertFalse(content, content.contains("root:"));
    assertTrue(content, content.contains("SUMMARY:Interview with Bob"));
    assertTrue(content, content.contains("Status: 5.1.1"));
    assertFalse(content, content.contains("not"));
  }

  @Test
  public void htmlBodyBecomesTextWithoutMarkupScriptOrTitle() throws Exception {
    Map<String, Object> row = index(
        "Content-Type: text/html; charset=utf-8\r\n\r\n"
        + "<html><head><title>Newsletter</title><style>p { color: red }</style></head><body>"
        + "<script>var tracking = 1;</script>"
        + "<p>Dear&nbsp;Bob,</p><div>Interview at <b>10:00</b> &amp; lunch</div>Line one<br>Line two"
        + "<ul><li>first</li><li>second</li></ul><p>para</p><br><br><p>after a gap</p></body></html>\r\n");

    // &nbsp; comes out as a plain space, which is what a search wants
    assertEquals(Arrays.asList("Dear Bob,\nInterview at 10:00 & lunch\nLine one\nLine two\nfirst\nsecond\npara\n\nafter a gap"),
        row.get("content"));
  }

  @Test
  public void declaredCharsetIsHonouredThroughTransferEncodings() throws Exception {
    // "Caf\u00e9 Zo\u00eb" in ISO-8859-1, quoted-printable, and in windows-1252 base64
    assertEquals(Arrays.asList("Caf\u00e9 Zo\u00eb"), index(
        "Content-Type: text/plain; charset=iso-8859-1\r\nContent-Transfer-Encoding: quoted-printable\r\n\r\n"
        + "Caf=E9 Zo=EB").get("content"));
    assertEquals(Arrays.asList("Caf\u00e9 Zo\u00eb"), index(
        "Content-Type: text/plain; charset=windows-1252\r\nContent-Transfer-Encoding: base64\r\n\r\n"
        + "Q2Fm6SBab+s=\r\n").get("content"));
    assertEquals(Arrays.asList("Caf\u00e9"), index(
        "Content-Type: text/html; charset=\"ISO-8859-1\"\r\nContent-Transfer-Encoding: quoted-printable\r\n\r\n"
        + "<p>Caf=E9</p>").get("content"));
  }

  /** No charset, an unknown one, or "us-ascii" over 8-bit bytes: UTF-8 if valid, else windows-1252. */
  @Test
  public void undeclaredOrWrongCharsetIsGuessed() throws Exception {
    byte[] utf8 = "Caf\u00e9 \u20ac5".getBytes(StandardCharsets.UTF_8);
    byte[] latin = "Caf\u00e9 \u20ac5".getBytes("windows-1252");

    assertEquals("Caf\u00e9 \u20ac5", FsMailEntityProcessor.decode(utf8, null));
    assertEquals("Caf\u00e9 \u20ac5", FsMailEntityProcessor.decode(latin, null));
    assertEquals("Caf\u00e9 \u20ac5", FsMailEntityProcessor.decode(latin, "x-unknown"));
    assertEquals("Caf\u00e9 \u20ac5", FsMailEntityProcessor.decode(latin, "us-ascii"));
    assertEquals("Caf\u00e9 \u20ac5", FsMailEntityProcessor.decode(utf8, "us-ascii"));
    assertEquals("plain", FsMailEntityProcessor.decode("plain".getBytes(StandardCharsets.US_ASCII), "us-ascii"));
  }

  /** Unchanged behaviour: only the first alternative is read, so a body is not indexed twice. */
  @Test
  public void onlyTheFirstAlternativeIsIndexed() throws Exception {
    Map<String, Object> row = index(
        "Content-Type: multipart/alternative; boundary=b\r\n\r\n"
        + "--b\r\nContent-Type: text/plain\r\n\r\nplain version\r\n"
        + "--b\r\nContent-Type: text/html\r\n\r\n<p>html version</p>\r\n"
        + "--b--\r\n");

    assertEquals(Arrays.asList("plain version"), row.get("content"));
  }

  /** Unchanged behaviour: a forwarded message's body and attachment names count as this one's. */
  @Test
  public void aForwardedMessageIsIndexedWithItsAttachmentNames() throws Exception {
    Map<String, Object> row = index(
        "Content-Type: multipart/mixed; boundary=outer\r\n\r\n"
        + "--outer\r\nContent-Type: text/plain\r\n\r\nFYI\r\n"
        + "--outer\r\nContent-Type: message/rfc822\r\n\r\n"
        + "From: carol@example.com\r\nSubject: original\r\n"
        + "Content-Type: multipart/mixed; boundary=inner\r\n\r\n"
        + "--inner\r\nContent-Type: text/plain\r\n\r\nthe original text\r\n"
        + "--inner\r\nContent-Type: application/pdf; name=offer.pdf\r\n"
        + "Content-Disposition: attachment; filename=offer.pdf\r\n\r\n%PDF-1.4\r\n"
        + "--inner--\r\n"
        + "\r\n--outer--\r\n");

    assertEquals(Arrays.asList("FYI", "the original text"), row.get("content"));
    assertEquals(Arrays.asList("offer.pdf"), row.get("attachmentNames"));
  }

  @Test
  public void textIsCappedWhereTikaCappedIt() throws Exception {
    StringBuilder big = new StringBuilder();
    while (big.length() < FsMailEntityProcessor.MAX_TEXT_CHARS + 5000) {
      big.append("lorem ipsum ");
    }
    Map<String, Object> row = index("Content-Type: text/plain\r\n\r\n" + big);

    assertEquals(FsMailEntityProcessor.MAX_TEXT_CHARS, contentOf(row).length() - 2);   // List.toString's brackets
  }

  @Test
  public void readingStopsAtTheByteCap() throws Exception {
    byte[] data = new byte[100];
    assertEquals(40, FsMailEntityProcessor.readCapped(new java.io.ByteArrayInputStream(data), 40).length);
    assertEquals(100, FsMailEntityProcessor.readCapped(new java.io.ByteArrayInputStream(data), 4096).length);
  }

  /** Parses a raw message (headers given after a fixed envelope) the way the import reads files. */
  private static Map<String, Object> index(String mimeHeadersAndBody) throws Exception {
    String raw = "From: alice@example.com\r\nTo: bob@example.com\r\nSubject: test\r\n"
        + "Date: Wed, 18 Nov 2026 10:00:00 -0500\r\nMIME-Version: 1.0\r\n" + mimeHeadersAndBody;
    Session session = Session.getDefaultInstance(new Properties(), null);
    MimeMessage msg = new MimeMessage(session,
        new java.io.ByteArrayInputStream(raw.getBytes(StandardCharsets.ISO_8859_1)));
    Map<String, Object> row = new HashMap<String, Object>();
    assertTrue(new FsMailEntityProcessor().addPartToDocument(msg, row, true));
    return row;
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
