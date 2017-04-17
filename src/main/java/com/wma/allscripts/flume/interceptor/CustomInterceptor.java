package com.wma.allscripts.flume.interceptor;

/**
 * Created by MShrestha on 12/5/2016.
 */
import java.lang.*;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;
import org.json.XML;

import java.util.UUID;
import java.util.regex.*;
import java.io.BufferedWriter;
import java.io.FileWriter;
import org.apache.log4j.Logger;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;


public class CustomInterceptor implements Interceptor {

    private String MessageTypeName;
    private String[] MessageTypeList;
    private String RootElementName;
    private String ContextElementName;
    private String BodyElementName;
    private String format;
    private String AuditPath;
    private String timeBucketWindow;
    private static final Logger LOG = Logger.getLogger(CustomInterceptor.class);
    /* Get actual class name to be printed on */
    //static Logger log = Logger.getLogger(CustomInterceptor.class.getName());


    private CustomInterceptor(String MessageTypeName, String MessageTypeList, String RootElementName, String ContextElementName,String BodyElementName,String format,String AuditPath, String timeBucketWindow){
        this.MessageTypeName = MessageTypeName;
        this.MessageTypeList = MessageTypeList.split(",");
        this.RootElementName = RootElementName;
        this.ContextElementName = ContextElementName;
        this.BodyElementName = BodyElementName;
        this.format = format;
        this.AuditPath = AuditPath;
        this.timeBucketWindow = timeBucketWindow;

    }

    @Override
    public void close()
    {

    }

    @Override
    public void initialize()
    {

    }

    @Override
    public Event intercept(Event event)
    {
        //byte[] eventBody = event.getBody();
        //Get original headers
        Map<String,String> origheaders = event.getHeaders();

        //Bucket number, which is every minute
        Long currentbucket = System.currentTimeMillis()/60000;
        String uuid = UUID.randomUUID().toString();
        String MessageType = "";
        try{

            Long timeWindow = Long.parseLong(timeBucketWindow);
            if(timeWindow > 0) {
                currentbucket = System.currentTimeMillis() / timeWindow;
            }
            String targetFolder = "Default";
            MessageType = origheaders.get(MessageTypeName);
            if(StringUtils.isNotEmpty(MessageType)){
                MessageType = MessageType.trim();
            }else {
                //Get message type from the incoming messages
                Pattern p = Pattern.compile("(<ahs_message_type>)(\\w+)(</ahs_message_type>)");
                Matcher m = p.matcher(new String(event.getBody(), "UTF-8"));
                //String MessageType = "";
                while (m.find()) {
                    MessageType = m.group(2);
                }
            }

            //convert all message type to lowercase for partitioning
            MessageType = MessageType.toLowerCase();

            //MessageType = origheaders.get(MessageTypeName);
            //Logger.info("MessageType:intercept:" + MessageType);
            /*for(String cMessageType : MessageTypeList){
                if(cMessageType.contains(MessageType)){
                   // Logger.info("CurrentMessageType:" + cMessageType);
                    targetFolder = cMessageType.substring(cMessageType.indexOf(':') + 1);
                }
            }*/

            //Add header data for event
            origheaders.put("Bucket",currentbucket.toString());
            origheaders.put("UUID", uuid);
            origheaders.put("MsgType", MessageType);
            origheaders.put("TargetFolder", MessageType);
            origheaders.put("Status", "new");

            //Logger.info("TargetFolder:" + targetFolder);
            String formattedMessage = "";

            //Create new xml data with new headers
            String xmlstring = getXMLFormat(origheaders,event);
            formattedMessage = xmlstring;

            //If the required output is JSON, convert it
            if(format !=null && format.equalsIgnoreCase("JSON")){
                JSONObject jobj = XML.toJSONObject(xmlstring,true);
                String jsonString = jobj.toString();
                formattedMessage = jsonString;
            }

            event.setBody(formattedMessage.getBytes());

        }
        catch(Exception ex){
            return event;
            throw new ChannelException("RxNucLog:Intercept  " + ex.getMessage());
        }

        LOG.info("Message Processed");
        try {
            //For logging and verification purpose, store incoming message data into local file system
            BufferedWriter bw = new BufferedWriter(new FileWriter(AuditPath + "/" + currentbucket.toString() + ".log", true));
            bw.write(MessageType);
            bw.newLine();
            bw.flush();
        }
        catch (IOException ioe) {
            ioe.printStackTrace();
        }

        return event;

    }

    private String getXMLFormat(Map<String,String>headers, Event event) throws IOException
    {
        StringBuilder headerbuild = new StringBuilder();
        headerbuild.append("<" + RootElementName + ">");
        headerbuild.append("<" + ContextElementName + ">");
        //headerbuild.append("<MsgType>" + MessageType + "</MsgType>");
        Iterator it = headers.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry pair = (Map.Entry)it.next();
            headerbuild.append("<" + pair.getKey() + ">" + pair.getValue() + "</" + pair.getKey() + ">");
            //Logger.info("Headers " + pair.getKey() + " - " + pair.getValue());
        }

        headerbuild.append("</" + ContextElementName + ">");
        headerbuild.append("<" + BodyElementName + ">");
        headerbuild.append(new String(event.getBody(),"UTF-8"));
        headerbuild.append("</" + BodyElementName + ">");
        headerbuild.append("</" + RootElementName + ">");
        return headerbuild.toString();
    }

    @Override
    public List<Event> intercept(List<Event> events)
    {
        for (Event event : events){

            intercept(event);
        }

        return events;
    }

    public static class Builder implements Interceptor.Builder
    {
        private String MessageTypeName;
        private String MessageTypeList;
        private String RootElementName;
        private String ContextElementName;
        private String BodyElementName;
        private String format;
        private String AuditPath;
        private String timeBucketWindow;
        @Override
        public void configure(Context context) {
            MessageTypeName = context.getString("MessageTypeName");
            MessageTypeList = context.getString("MessageTypeList");
            RootElementName = context.getString("RootElementName");
            ContextElementName = context.getString("ContextElementName");
            BodyElementName = context.getString("BodyElementName");
            AuditPath = context.getString("AuditPath");
            format = context.getString("format");
            timeBucketWindow = context.getString("timeBucketWindow");
            // TODO Auto-generated method stub
        }

        @Override
        public Interceptor build() {
            return new CustomInterceptor(MessageTypeName,MessageTypeList,RootElementName,ContextElementName,BodyElementName,format,AuditPath,timeBucketWindow);
        }
    }
}