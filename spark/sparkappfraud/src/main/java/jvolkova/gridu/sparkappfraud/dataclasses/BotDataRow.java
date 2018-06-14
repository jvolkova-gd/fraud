package main.java.jvolkova.gridu.sparkappfraud.dataclasses;

import java.io.Serializable;

public class BotDataRow implements Serializable {

    private String ip;
    public BotDataRow(){}
    public BotDataRow(String ip) {
        this.ip = ip;
    }
    public String getIp() { return ip; }
    public void setIp(String ip) { this.ip = ip; }
}
