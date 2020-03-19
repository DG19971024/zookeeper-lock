package com.example.zookeeper_lock.lock;

/**
 * @ClassName SecondKill
 * @Deacription TODO
 * @Author dinggang
 * @Date 2020/3/16
 * @Version 1.0
 * @Modefied what？
 **/
public class SecondKill {
    private int number = 10000;

    public void decrease() {
        if (number>0) {
            Thread.yield();
            number--;
            System.out.println(number);
        }
    }
}
