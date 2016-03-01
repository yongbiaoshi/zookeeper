package com.tsingda.zk.demo;

import java.io.IOException;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * Hello world!
 *
 */
public class ZKApp {
    /**
     * @param args
     * @throws IOException
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        RetryPolicy retryPolicy = new RetryUntilElapsed(10000, 1000);
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("localhost:2180,localhost:2181,localhost:2182").sessionTimeoutMs(10000)
                .retryPolicy(retryPolicy).namespace("payment-sys").build();
        
        try {
            client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/order/recharge");
        } catch (Exception e1) {
            e1.printStackTrace();
            System.out.println("create exception");
        }
        
        ZKWatcher watcher = new ZKWatcher();
        ZooKeeper zooKeeper = new ZooKeeper("localhost:2180", 6000, new ZKWatcher());
        System.out.println("begin state = " + zooKeeper.getState());
        Stat stat = new Stat();
        byte[] data = zooKeeper.getData("/zk-demo", watcher, stat);
        System.out.println(new String(data));
        System.out.println(stat.getAversion());
        System.out.println(stat.getVersion());
        stat = zooKeeper.setData("/zk-demo", "测试中文数据".getBytes(), stat.getVersion());
        System.out.println(stat.getVersion());
        try {
            watcher.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.out.println("Zookeeper session established.");
        }
        System.out.println("end state=" + zooKeeper.getState());
    }
}
