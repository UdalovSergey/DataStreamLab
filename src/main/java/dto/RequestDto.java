package dto;

import java.io.Serializable;

public class RequestDto implements Serializable{
    private String url;
    private String ip;
    private String type;
    private String unix_time;

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

    public void setType(String type) {
        this.type = type;
    }

    public String getUnix_time() {
        return unix_time;
    }

    public void setUnix_time(String unix_time) {
        this.unix_time = unix_time;
    }
}
