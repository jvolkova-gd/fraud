package main.java.jvolkova.gridu.sparkappfraud.dataclasses;

import java.io.Serializable;

public class CtrDataRow implements Serializable {
    private String ip;
    private Double ctr;

    public CtrDataRow(){}

    public CtrDataRow(String ip, Double ctr){
        this.ip = ip;
        this.ctr = ctr;
    }

    public String getIp() { return ip; }
    public void setIp(String ip) { this.ip = ip; }

    public Double getCtr() { return ctr; }
    public void setCtr(Double ctr) { this.ctr = ctr; }
}