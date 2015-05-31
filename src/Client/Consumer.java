package Client;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class Consumer implements Runnable {

    private final String port = "4430";
    private final String baseUrl = "https://hq.company.com:" + port;
    private final String url = baseUrl + "/HyperQueue/Broker";
    private String topic = new String("");
    private String event = new String("");
    private String sessionID = new String("");
    private String query = new String("");

    // constructor
    public Consumer(String topic) {
        this.topic = topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
        setQuery();
    }

    public void setSessionID(String sessionID) {
        this.sessionID = sessionID;
        setQuery();
    }

    private void setQuery() {
        this.query = "Topic: " + this.topic + ". Session ID: " + this.sessionID;
    }
    
    private String getQuery() {
        return "Topic: " + this.topic + ". Session ID: " + this.sessionID;
    }

    @Override
    public void run() {
        if (sessionID.isEmpty() || sessionID == null) {
            getSessionID();
        } else {
            event = getEvent(this.topic);
        }
    }

    public void getSessionID() {
        HttpURLConnection con = null;
        InputStream in = null;
        try {
            con = (HttpURLConnection) (new URL(url)).openConnection();
            con.setRequestMethod("GET");
            con.setDoInput(true);
            con.connect();

            StringBuffer buffer = new StringBuffer();
            in = con.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String line = null;
            while ((line = br.readLine()) != null){
                buffer.append(line);
            }

            System.out.println("Session ID is " + buffer.toString());
            this.setSessionID(buffer.toString());
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            try {
                in.close();
                con.disconnect();
            } catch (Throwable t) {
            }
        }
    }

    // HTTP GET, read response from server
    public String getEvent(String topic) {
        HttpURLConnection con = null;
        InputStream in = null;
        
        try {
            con = (HttpURLConnection) (new URL(url + getQuery())).openConnection();
            con.setRequestMethod("GET");
            con.setDoInput(true);
            con.connect();

            StringBuffer buffer = new StringBuffer();
            in = con.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String line = null;
            while ((line = br.readLine()) != null){
                buffer.append(line);
            }

            return buffer.toString();
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            try {
                in.close();
                con.disconnect();
            } catch (Throwable t) {
            }
        }
        return null;
    }

}
