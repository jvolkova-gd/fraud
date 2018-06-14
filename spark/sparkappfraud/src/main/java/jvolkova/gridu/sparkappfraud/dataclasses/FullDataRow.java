package main.java.jvolkova.gridu.sparkappfraud.dataclasses;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class FullDataRow implements Serializable {
    private String ip;
    private Double ctr;
    private Double clicks;
    private Double views;
    private Set<String> categoriesIds = new HashSet<String>();
    private Boolean bot = false;

    FullDataRow(){}

    public FullDataRow(Boolean bot){
        this.bot = bot;
    }
    public FullDataRow(String ip, Double clicks, Double views, String categoriesIds){
        this.ip = ip;
        this.clicks = clicks;
        this.views = views;
        this.categoriesIds.add(categoriesIds);
    }

    public FullDataRow(String ip, Double clicks, Double views, Set<String> categoriesIds){
        this.ip = ip;
        this.clicks = clicks;
        this.views = views;
        this.categoriesIds = categoriesIds;
    }

    public FullDataRow(String ip, Double clicks, Double views, Set<String> categoriesIds, Double ctr){
        this.ip = ip;
        this.clicks = clicks;
        this.views = views;
        this.categoriesIds = categoriesIds;
        this.ctr = ctr;
    }

    public String getIp() { return ip; }
    public void setIp(String ip) { this.ip = ip; }

    public Boolean getBot() { return bot; }
    public void setBot(Boolean bot) { this.bot = bot; }

    public Double getClicks() { return clicks; }
    public void setClicks(Double clicks) { this.clicks = clicks; }

    public Double getViews() { return clicks; }
    public void setViews(Double views) { this.views = views; }

    public Double getCtr() { return ctr; }
    public void setCtr(Double ctr) { this.ctr = ctr; }

    public Set<String> getCategoriesIds() { return categoriesIds; }
    public void setCategoriesIds(String categoriesIds) {
        this.categoriesIds.add(categoriesIds); }
    public Set<String> joinCategoriesIds(Set<String> categoriesIds) {
        this.categoriesIds.addAll(categoriesIds);
        return categoriesIds;
    }
}