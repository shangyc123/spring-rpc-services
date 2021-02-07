package com.zr.springdataelasticsearchservice.model;


import java.io.Serializable;

public class ContentModel implements Serializable {
    private String img;
    private String title;
    private String price;

    public String getImg() {
        return img;
    }

    public void setImg(String img) {
        this.img = img;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getPrice() {
        return price;
    }

    public void setPrice(String price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "ContentModel{" +
                "img='" + img + '\'' +
                ", title='" + title + '\'' +
                ", price='" + price + '\'' +
                '}';
    }
}
