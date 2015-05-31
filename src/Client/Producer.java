package Client;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class Producer implements Runnable {

    private final String port = "4430";
    private final String charset = "UTF-8";
    private final String baseUrl = "https://hq.company.com:" + port;
    private final String url = baseUrl+"/HyperQueue/Broker";
    private String topic = new String("");
    private String event = new String("");
    private String query = new String("");

    // constructor
    public Producer(String topic, String event) {
        this.topic = topic;
        this.event = event;
        setQuery();
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub
        postMessage(this.event);
    }

    public void postMessage(String event) {
        HttpURLConnection con = null;
        OutputStream output = null;
        InputStream input = null;
        try {
            con = (HttpURLConnection) (new URL(url)).openConnection();
            con.setRequestMethod("POST");
            con.setDoOutput(true);
            con.connect();
            output = con.getOutputStream();
            output.write(event.getBytes(charset));
            input = con.getInputStream();
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            try {
                input.close();
                output.close();
                con.disconnect();
            } catch (Throwable t) {
            }
        }
    }

    public void setTopic(String topic) {
        this.topic = topic;
        setQuery();
    }

    public void setEvent(String event) {
        this.event = event;
        setQuery();
    }
    
    private void setQuery(){
        this.query = "Topic: " + this.topic + ". Event: " + this.event;
    }
}
