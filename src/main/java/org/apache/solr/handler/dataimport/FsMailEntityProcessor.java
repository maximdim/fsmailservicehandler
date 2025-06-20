/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.handler.dataimport;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import javax.mail.Address;
import javax.mail.Flags;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Part;
import javax.mail.Session;
import javax.mail.internet.AddressException;
import javax.mail.internet.ContentType;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.tika.Tika;
import org.apache.tika.metadata.HttpHeaders;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaMetadataKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link EntityProcessor} instance which can index emails along with their attachments 
 * from mail messages stored in local FS.
 * 
 * This code borrows many parts from existing Solr's {@link MailEntityProcessor}. Too bad
 * MailEntityProcessor is not written with extension in mind - otherwise only authentication
 * mechanism and multi-user support could be added
 * 
 * @author Dmitri Maximovich maxim@maximdim.com
 *
 */
public class FsMailEntityProcessor extends EntityProcessorBase {
  static final Logger LOG = LoggerFactory.getLogger(FsMailEntityProcessor.class);

  // Fields To Index
  // single valued
  private static final String USER_ID = "userId";
  private static final String MESSAGE_ID = "messageId";
  private static final String SUBJECT = "subject";
  private static final String FROM = "from";
  private static final String FROM_CLEAN = "from_clean";
  private static final String SENT_DATE = "sentDate";
  private static final String RECEIVED_DATE = "receivedDate";
  private static final String XMAILER = "xMailer";
  // first 'To' address - need it for sorting. Sort is impossible for multivalued fields
  private static final String TO = "to";
  private static final String TO_CLEAN = "to_clean";
  // hash of the important fields
  private static final String HASH = "hash";
  // multi valued
  private static final String TO_CC_BCC = "allTo";
  private static final String TO_CC_BCC_CLEAN = "allTo_clean";
  private static final String FLAGS = "flags";
  private static final String CONTENT = "content";
  private static final String ATTACHMENT = "attachment";
  private static final String ATTACHMENT_NAMES = "attachmentNames";
  // flag values
  private static final String FLAG_ANSWERED = "answered";
  private static final String FLAG_DELETED = "deleted";
  private static final String FLAG_DRAFT = "draft";
  private static final String FLAG_FLAGGED = "flagged";
  private static final String FLAG_RECENT = "recent";
  private static final String FLAG_SEEN = "seen";

  private final Tika tika = new Tika();
  private final Session session = Session.getDefaultInstance(new Properties(), null);

  private File dataDir;
  private List<String> ignoreFrom;

  private Iterator<String> fileNames;

  @Override
  public void init(Context context) {
    super.init(context);

    // see https://javaee.github.io/javamail/FAQ#castmultipart
    Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());

    this.dataDir = new File(getStringFromContext("dataDir", null));
    this.ignoreFrom = Arrays.asList(getStringFromContext("ignoreFrom", "qwe123").split(","));
    
    LOG.info("datadir: "+this.dataDir);
    LOG.info("ignoreFrom: "+this.ignoreFrom);

    // We don't distinguish between full and delta import here. Always need to be triggered
    // as FULL dump but we would look at date from 'dataimport.properties' to determine last index time
    // and only process delta changes
    LOG.info("Current process: "+context.currentProcess());
    Date since = getSince(context);
    LOG.info("Using since: "+since);
    
    List<String> files = new ArrayList<String>();
    getFolderFiles(dataDir, since, files);
    LOG.info("Files to process: "+files.size());

    this.fileNames = files.iterator();
  }

  private Date getSince(Context c) {
    // perhaps there is a better way to get last index time?
    String sinceStr = context.replaceTokens("${dataimporter.last_index_time}");
    if (!sinceStr.contains("1969")) { // if there are no last delta time (e.g. file removed) date in 1969 is returned
      SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      try {
        Date date = df.parse(sinceStr);
        // it seems that last updated time is saved at the end of the run so there is a window
        // when some new files could be written which would fall into the crack so we would
        // move this time back. Not particularly nice hack
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.HOUR, -2);
        LOG.info("Since date " + date + "->" + cal.getTime());
        return cal.getTime();
      } 
      catch (ParseException e) {
        LOG.error("Failed to parse date: ["+sinceStr+"]");
      }
    }
    return null;
  }
  
  @Override
  public Map<String, Object> nextRow() {
    while(this.fileNames.hasNext()) {
      File file = new File(this.fileNames.next());
      //LOG.info("Processing "+file.getAbsolutePath());
      FileInfo fi = null;
      try {
        fi = new FileInfo(file);
      } 
      catch (InvalidFileException e) {
        LOG.error("We should be able to parse this FileInfo here: "+e.getMessage()+": "+file.getName());
        continue;
      }
      
      Message message = readMessage(session, file);
      if (message == null) {
        continue;
      }
      Map<String, Object> row = getDocumentFromMail(message);
      if (row == null) {
        continue;
      }
      // use file name as id - it's guaranteed to be unique
      row.put(MESSAGE_ID, fi.id);
      row.put(USER_ID, fi.user);
      LOG.info("Processed "+file.getAbsolutePath());
      return row;
    }
    return null;
  }

  private Message readMessage(Session session, File f) {
    if (f.getName().toLowerCase().endsWith(".gz")) { // gzipped file
      try (GZIPInputStream is = new GZIPInputStream(new FileInputStream(f))) { // already buffered
        return new MimeMessage(session, is);
      }
      catch (Exception e) {
        LOG.error("Error indexing message from "+f.getAbsolutePath()+": "+e.getMessage(), e);
        return null;
      }
    }
    else { // regular file
      try (BufferedInputStream is = new BufferedInputStream(new FileInputStream(f))) {
        return new MimeMessage(session, is);
      }
      catch (Exception e) {
        LOG.error("Error indexing message from "+f.getAbsolutePath()+": "+e.getMessage(), e);
        return null;
      }
    }
  }
  
  private Map<String, Object> getDocumentFromMail(Message mail) {
    Map<String, Object> row = new HashMap<String, Object>();
    try {
      if(addPartToDocument(mail, row, true)) {
        return row;
      }
      return null;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  @SuppressWarnings("unchecked")
  public boolean addPartToDocument(Part part, Map<String, Object> row, boolean outerMost) throws Exception {
    if (outerMost && part instanceof Message) {
      if (!addEnvelopToDocument(part, row)) {
        return false;
      }
      // store hash - used to filter out non-unique messages on retrieval
      row.put(HASH, DigestUtils.md5Hex(row.get(FROM_CLEAN) + "|" + row.get(TO_CLEAN) +"|"+ row.get(SENT_DATE) + "|" + row.get(SUBJECT)));
    }

    String ct = part.getContentType();
    ContentType ctype = new ContentType(ct);
    if (part.isMimeType("multipart/*")) {
      //LOG.info("MimeType: "+ct + ": " + part.getContent().getClass().getName());
      Multipart mp = (Multipart) part.getContent();
      int count = mp.getCount();
      if (part.isMimeType("multipart/alternative")) {
        count = 1;
      }
      for (int i = 0; i < count; i++) {
        addPartToDocument(mp.getBodyPart(i), row, false);
      }
    } 
    else if (part.isMimeType("message/rfc822")) {
      addPartToDocument((Part) part.getContent(), row, false);
    } 
    else {
      String disp = part.getDisposition();
      @SuppressWarnings("resource") // Tika will close stream
      InputStream is = part.getInputStream();
      String fileName = part.getFileName();
      Metadata md = new Metadata();
      md.set(HttpHeaders.CONTENT_TYPE, ctype.getBaseType().toLowerCase(Locale.ROOT));
      md.set(TikaMetadataKeys.RESOURCE_NAME_KEY, fileName);
      String content = this.tika.parseToString(is, md);
      if (disp != null && disp.equalsIgnoreCase(Part.ATTACHMENT)) {
        if (row.get(ATTACHMENT) == null) {
          row.put(ATTACHMENT, new ArrayList<String>());
        }
        List<String> contents = (List<String>) row.get(ATTACHMENT);
        contents.add(content);
        row.put(ATTACHMENT, contents);
        if (row.get(ATTACHMENT_NAMES) == null) {
          row.put(ATTACHMENT_NAMES, new ArrayList<String>());
        }
        List<String> names = (List<String>) row.get(ATTACHMENT_NAMES);
        names.add(fileName);
        row.put(ATTACHMENT_NAMES, names);
      } 
      else {
        if (row.get(CONTENT) == null) {
          row.put(CONTENT, new ArrayList<String>());
        }
        List<String> contents = (List<String>) row.get(CONTENT);
        contents.add(content);
        row.put(CONTENT, contents);
      }
    }
    return true;
  }

  private boolean addEnvelopToDocument(Part part, Map<String, Object> row) throws MessagingException {
    MimeMessage mail = (MimeMessage) part;
    Address[] adresses;
    if ((adresses = mail.getFrom()) != null && adresses.length > 0) {
      String from = adresses[0].toString();
      // check if we should ignore this sender
      for(String ignore: this.ignoreFrom) {
        if (from.toLowerCase().contains(ignore)) {
          LOG.info("Ignoring email from "+from);
          return false;
        }
      }
      row.put(FROM, from);
      row.put(FROM_CLEAN, cleanAddress(from));
    }
    else {
      return false;
    }

    List<String> to = new ArrayList<String>();
    if ((adresses = mail.getRecipients(Message.RecipientType.TO)) != null) {
      addAddressToList(adresses, to);
    }
    if ((adresses = mail.getRecipients(Message.RecipientType.CC)) != null) {
      addAddressToList(adresses, to);
    }
    if ((adresses = mail.getRecipients(Message.RecipientType.BCC)) != null) {
      addAddressToList(adresses, to);
    }
    if (!to.isEmpty()) {
      row.put(TO_CC_BCC, to);
      List<String> cleanAddresses = cleanAddresses(to);
      row.put(TO_CC_BCC_CLEAN, cleanAddresses);
      // save first TO address into separate field
      row.put(TO, to.get(0));
      if (!cleanAddresses.isEmpty()) {
        row.put(TO_CLEAN, cleanAddresses.get(0));
      }
    }

    row.put(MESSAGE_ID, mail.getMessageID());
    row.put(SUBJECT, mail.getSubject());

    {
      Date d = mail.getSentDate();
      if (d != null) {
        row.put(SENT_DATE, d);
      }
    }
    {
      Date d = mail.getReceivedDate();
      if (d != null) {
        row.put(RECEIVED_DATE, d);
      }
    }

    List<String> flags = new ArrayList<String>();
    for (Flags.Flag flag : mail.getFlags().getSystemFlags()) {
      if (flag == Flags.Flag.ANSWERED) {
        flags.add(FLAG_ANSWERED);
      } else if (flag == Flags.Flag.DELETED) {
        flags.add(FLAG_DELETED);
      } else if (flag == Flags.Flag.DRAFT) {
        flags.add(FLAG_DRAFT);
      } else if (flag == Flags.Flag.FLAGGED) {
        flags.add(FLAG_FLAGGED);
      } else if (flag == Flags.Flag.RECENT) {
        flags.add(FLAG_RECENT);
      } else if (flag == Flags.Flag.SEEN) {
        flags.add(FLAG_SEEN);
      }
    }
    flags.addAll(Arrays.asList(mail.getFlags().getUserFlags()));
    row.put(FLAGS, flags);

    String[] hdrs = mail.getHeader("X-Mailer");
    if (hdrs != null) {
      row.put(XMAILER, hdrs[0]);
    }
    return true;
  }

  private void addAddressToList(Address[] adresses, List<String> to) throws AddressException {
    for (Address address : adresses) {
      to.add(address.toString());
      InternetAddress ia = (InternetAddress) address;
      if (ia.isGroup()) {
        InternetAddress[] group = ia.getGroup(false);
        for (InternetAddress member : group) {
          to.add(member.toString());
        }
      }
    }
  }

  private static final String EMAIL_PATTERN = 
      "^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@"
      + "[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$";
  
  private static final Pattern emailPattern = Pattern.compile(EMAIL_PATTERN);
  
  /**
   * @return 'clean' email address or null if it doesn't look like email address
   */
  private static String cleanAddress(String a) {
    if (a == null || a.trim().isEmpty()) {
      return null;
    }
    int i = a.indexOf('<');
    int j = a.indexOf('>', i);
    if (i >= 0 && j > i) {
      String mail = a.substring(i+1, j);
      if (emailPattern.matcher(mail).matches()) {
        return mail.toLowerCase();
      }
    }
    else {
      return a.trim().toLowerCase();
    }
    return null;
  }
  
  private static List<String> cleanAddresses(List<String> aa) {
    List<String> result = new ArrayList<String>();
    for(String a: aa) {
      String email = cleanAddress(a);
      if (email != null) {
        result.add(email);
      }
    }
    return result;
  }

  private String getStringFromContext(String prop, String ifNull) {
    String v = ifNull;
    String val = context.getEntityAttribute(prop);
    if (val != null) {
      val = context.replaceTokens(val);
      v = val;
    }
    return v;
  }
  
  void getFolderFiles(File dir, final Date since, final List<String> fileNames) {
    // Fetch an list of file objects that pass the filter, however the
    // returned array is never populated; accept() always returns false.
    // Rather we make use of the fileNames list which is populated as
    // a side affect of the accept method.
    dir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File f) {
        if (f.isDirectory() && shouldAcceptDirectory(f, since)) {
          getFolderFiles(f, since, fileNames);
        } 
        else {
          if (shouldAcceptFile(f, since)) {
            fileNames.add(f.getAbsolutePath());
          }
        }
        return false;
      }

      private final SimpleDateFormat df = new SimpleDateFormat("yyyy"+File.separator+"MM"+File.separator+"dd");
      private final Pattern YEAR_PATTERN = Pattern.compile(".*/(20\\d{2}).*");

      // Optimization because path to data file has full date, e.g.:
      // foo.com/2013/05/11/user_20130611T092053.mail 
      private boolean shouldAcceptDirectory(File dir, Date since) {
        if (since == null) {
          return true;
        }
        String name = dir.getAbsolutePath();
        LOG.info("name: [" + name + "]");

        {
          // skip whole year folder if before since
          Matcher matcher = YEAR_PATTERN.matcher(name);
          if (matcher.find()) {
            int dirYear = Integer.parseInt(matcher.group(1));
            if (dirYear < 1900 + since.getYear()) {
              LOG.info("Skipping old year: "+dir);
              return false;
            }
          }
        }

        if (name.length() < 10) {
          return true;
        }

        try {
          Date dirDate = df.parse(name.substring(name.length() - 10));
          since = DateUtils.truncate(since, Calendar.DAY_OF_MONTH);
          boolean oldDir = dirDate.before(since);
          if (oldDir) {
            LOG.info("Skipping old directory: "+dir);
          }
          return !oldDir;
        }
        catch (ParseException e) {
          return true;
        }
      }
    });
  }
  
  boolean shouldAcceptFile(File f, Date since) {
    if (f.isDirectory()) {
      return false;
    }
    if (since == null) {
      return true;
    }
    
    try {
      FileInfo fi = new FileInfo(f);
      return fi.date.after(since);
    } 
    catch (InvalidFileException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }
  
  static class FileInfo {
    // gmailbackup creates files with following format: "user_yyyyMMdd'T'HHmmss.mail"
    private final static SimpleDateFormat DF = new SimpleDateFormat("yyyyMMdd'T'HHmmss"); 
    final String id;
    final String user;
    final Date date;
    final String hash;
    
    FileInfo(File f) throws InvalidFileException {
      String name = f.getName();
      if (!(name.endsWith(".mail") || name.endsWith(".mail.gz"))) {
        throw new InvalidFileException("File name not recognized ["+name+"]");
      }
      // remove extension
      name = name.substring(0, name.lastIndexOf('.'));
      this.id = name;
      // split by user and date
      String[] parts = name.split("_");
      if (parts.length != 3) {
        throw new InvalidFileException("Not parseable file name ["+name+"]");
      }
      this.user = parts[0];
      this.hash = parts[2];
      try {
        this.date = DF.parse(parts[1]);
      } 
      catch (ParseException e) {
        throw new InvalidFileException("Unable to parse date from file ["+name+"]");
      }
    }
  }
  
  static class InvalidFileException extends Exception {
    InvalidFileException(String message) {
      super(message);
    }

    private static final long serialVersionUID = 1L;
  }
  
}
