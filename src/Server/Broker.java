package Server;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class Broker extends HttpServlet{

    private static int sessionID = 0;
    ConcurrentHashMap<String, ArrayList<String>> topicToEvent=new ConcurrentHashMap<String,ArrayList<String>> ();
    ConcurrentHashMap<Integer, ConcurrentHashMap<String, Integer>> idToOffset=new ConcurrentHashMap<Integer,ConcurrentHashMap<String,Integer>>();
    
    //constructor
    public Broker(){
        this.sessionID = 0;
    }
    
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("text/plain");
        PrintWriter printWriter  = response.getWriter();
        //First time
        if(request.getParameter("sessionID")==null || request.getParameter("sessionID").isEmpty()) {
            sessionID++;
            printWriter.println(Integer.toString(sessionID));
            idToOffset.put(sessionID,new ConcurrentHashMap<String,Integer>());
            System.out.println("New session ID = " + sessionID);
        } else {
            //already have session ID and topic
            int id=Integer.parseInt(request.getParameter("sessionID"));
            String topic=request.getParameter("topic");
            String event=null;
            try {
                event = getEvent(id,topic);
                if(getEvent(id,topic)!=null)
                    System.out.println("Consumed event. SessionID = "+id+" Offset = "+idToOffset.get(id).get(topic)+" Topic = "+topic+" Event = "+event);
                else
                    System.out.println("Error consumer request.");
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            printWriter.println(event);
            printWriter.close();
        }   

    }

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String topic = request.getParameter("topic");
        String event = request.getParameter("event");
        if(topic!=null && event!=null) {
            this.addEvent(event,topic);
            System.out.println("Event added. Topic = "+ topic+" Event = " + event);
        } else {
            System.out.println("Error adding event");
        }
    }

    private void addEvent(String event, String topic){

        //add event to existing topic
        if(topicToEvent.containsKey(topic)){
            ArrayList<String> topicQueue=(ArrayList<String>) Collections.synchronizedCollection(topicToEvent.get(topic));
            topicQueue.add(event);
        } else {
            //create a new list
            ArrayList<String> topicQueue=(ArrayList<String>) Collections.synchronizedCollection(new ArrayList<String>());
            topicQueue.add(event);
            topicToEvent.put(topic, topicQueue);
        }
    }
    
    private String getEvent(int id, String topic) throws InterruptedException{
        int offset=0;
        ConcurrentHashMap<String, Integer> map=null;
        if(idToOffset.containsKey(id)){
            map= idToOffset.get(id);
            if(map.containsKey(topic)) {offset=map.get(topic);map.put(topic, offset+1);}
            else map.put(topic, 1);
        } else return null;

        ArrayList<String> q=(ArrayList<String>) Collections.synchronizedCollection(topicToEvent.get(topic));

        String event;
        if(q==null||offset>=q.size()) {
            Thread.sleep(1000);
            if(q!=null&&offset<q.size()){
               //Producer sends a new event to consumer
                event=(String) q.get(offset);
                idToOffset.put(id, map);
            }
            else event=null;
        } else {
            event=(String) q.get(offset);
            idToOffset.put(id, map);
        }
        return event;
    }
}
