package com.zr.springdataelasticsearchservice.dto;

public class ResultBean {

    private final static String SUCCESS = "success";
    private final static String FAIL = "fail";
    private String msg;

    public ResultBean(String success, String msg) {
        msg = this.msg;
    }

    public ResultBean(String success) {
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public static ResultBean success(){
        return new ResultBean(SUCCESS);
    }

    public static ResultBean success(String msg){
        return new ResultBean(SUCCESS, msg);
    }

    public static ResultBean fail(){
        return new ResultBean(FAIL);
    }

    public static ResultBean fail(String msg){
        return new ResultBean(FAIL, msg);
    }


}
