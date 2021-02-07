package com.zr.springdataelasticsearchservice.model;

public class UserModel {
    private String name;
    private int age;
    private String desc;
    private String level;
    private String experience;
    private String entry_time;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public String getExperience() {
        return experience;
    }

    public void setExperience(String experience) {
        this.experience = experience;
    }

    public String getEntry_time() {
        return entry_time;
    }

    public void setEntry_time(String entry_time) {
        this.entry_time = entry_time;
    }

    @Override
    public String toString() {
        return "UserModel{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", desc='" + desc + '\'' +
                ", level='" + level + '\'' +
                ", experience='" + experience + '\'' +
                ", entry_time='" + entry_time + '\'' +
                '}';
    }
}
