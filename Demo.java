package com.ascend.connect.sqlserver;

/**
 * Created by Manoj.dalai on 10/21/2016.
 */
public class Demo {
    public static void main(String ... args){
        String pk = "user1:user2";

        int size = pk.split(":").length;
        System.out.println("# - "+size);
    }
}
