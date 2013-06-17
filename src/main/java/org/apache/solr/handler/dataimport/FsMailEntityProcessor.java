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
import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

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
    this.dataDir = new File(getStringFromContext("dataDir", null));
    this.ignoreFrom = Arrays.asList(getStringFromContext("ignoreFrom", "qwe123").split(","));
    
    LOG.info("datadir: "+this.dataDir);
    LOG.info("ignoreFrom: "+this.ignoreFrom);

    Date since = getSince(context);
    if (since != null) {
      LOG.info("Since: "+since);
    }
    
    List<String> files = new ArrayList<String>();
    getFolderFiles(dataDir, since, files);
    LOG.info("Files to process: "+files.size());

    this.fileNames = files.iterator();
  }

  private Date getSince(Context c) {
    // perhaps there is a better way to get last index time?
    String sinceStr = context.replaceTokens("${dataimporter.last_index_time}");
    if (!sinceStr.contains("1969")) { // if there are no last delat time (e.g. file removed) date in 1969 is returned
      SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      try {
        return df.parse(sinceStr);
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

  @SuppressWarnings("resource")
  private Message readMessage(Session session, File f) {
    BufferedInputStream is = null;
    try {
      is = new BufferedInputStream(new FileInputStream(f));
      return new MimeMessage(session, is);
    }
    catch (Exception e) {
      LOG.error("Error indexing message from "+f.getAbsolutePath()+": "+e.getMessage(), e);
      return null;
    }
    finally {
      close(is);
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

  public boolean addPartToDocument(Part part, Map<String, Object> row, boolean outerMost) throws Exception {
    if (part instanceof Message) {
      if (!addEnvelopToDocument(part, row)) {
        return false;
      }
    }

    String ct = part.getContentType();
    ContentType ctype = new ContentType(ct);
    if (part.isMimeType("multipart/*")) {
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
      row.put(TO_CC_BCC_CLEAN, cleanAddresses(to));
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
    if (a == null || a.trim().length() == 0) {
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
  
  private void close(Closeable c) {
    if (c != null) {
      try {
        c.close();
      }
      catch (IOException e) {
        LOG.warn("Error closing Closeable: "+e.getMessage(), e);
      }
    }
  }

  void getFolderFiles(File dir, final Date since, final List<String> fileNames) {
    // Fetch an list of file objects that pass the filter, however the
    // returned array is never populated; accept() always returns false.
    // Rather we make use of the fileNames list which is populated as
    // a side affect of the accept method.
    dir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File f) {
        if (f.isDirectory()) {
          // TODO: Could optimize here because path to data file has full date, e.g.:
          // foo.com/2013/05/11/user_20130611T092053.mail 
          getFolderFiles(f, since, fileNames);
        } 
        else {
          if (shouldAcceptFile(f, since)) {
            fileNames.add(f.getAbsolutePath());
          }
        }
        return false;
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
      LOG.error(e.getMessage());
      return false;
    }
  }
  
  static class FileInfo {
    final String id;
    final String user;
    final Date date;
    final String hash;
    
    FileInfo(File f) throws InvalidFileException {
      String name = f.getName();
      if (!name.endsWith(".mail")) {
        throw new InvalidFileException("File name not recognized ["+name+"]");
      }
      // gmailbackup creates files with following format: "user_yyyyMMdd'T'HHmmss.mail"
      SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd'T'HHmmss");
      
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
        this.date = df.parse(parts[1]);
      } 
      catch (ParseException e) {
        throw new InvalidFileException("Unable to parse date from file ["+name+"]");
      }
    }
  }
  
  static class InvalidFileException extends Exception {
    public InvalidFileException(String message) {
      super(message);
    }

    public InvalidFileException(String message, Throwable cause) {
      super(message, cause);
    }

    private static final long serialVersionUID = 1L;
  }
  
}
