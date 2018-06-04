package dto;


import java.io.Serializable;
import java.sql.Timestamp;


public class RequestDto implements Serializable{
    private String url;
    private String ip;
    private String type;
    private Timestamp unix_time;
    private int view;
    private int click;

    public RequestDto() {
    }

    public RequestDto(String ip, String type) {
        this.ip = ip;
        this.type = type;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getType() {
        return type;
    }

    //Work around to prevent doing multiple aggregation on stream
    public void setType(String type) {
        this.type = type;
        if(type.equals("click")){
            this.click = 1;
        } else {
            this.view = 1;
        }
    }

    public Timestamp getUnix_time() {
        return unix_time;
    }

    public void setUnix_time(Timestamp unix_time) {
        this.unix_time = unix_time;
    }

    public int getView() {
        return view;
    }

    public void setView(int view) {
        this.view = view;
    }

    public int getClick() {
        return click;
    }

    public void setClick(int click) {
        this.click = click;
    }
}
