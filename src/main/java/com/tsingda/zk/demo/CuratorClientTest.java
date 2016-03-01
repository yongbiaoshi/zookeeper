package com.tsingda.zk.demo;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CuratorClientTest {
    private static final Logger logger = LoggerFactory.getLogger(CuratorClientTest.class);

    private CuratorFramework client = null;

    public CuratorClientTest() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(2000, 5);
        client = CuratorFrameworkFactory.builder().connectString("localhost:2180,localhost:2181,localhost:2182")
                .sessionTimeoutMs(10000).retryPolicy(retryPolicy).namespace("zk-demo").build();
        client.start();
    }

    public void closeClient() {
        if (client != null) {
            client.close();
        }
    }

    public void createNode(String path, byte[] data) throws Exception {
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).withACL(Ids.OPEN_ACL_UNSAFE)
                .forPath(path, data);
    }

    public void deleteNode(String path) throws Exception {
        BackgroundCallback callback = new BackgroundCallback() {
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                logger.info("删除结果：{}", event.getResultCode());
                logger.info("删除Node事件回调：eventPath={}, eventData={}, eventType={}, eventCode={}", event.getPath(),
                        event.getData(), event.getType(), event.getResultCode());
            }
        };
        client.delete().guaranteed().deletingChildrenIfNeeded().inBackground(callback).forPath(path);
    }

    /**
     * 读取ZNode数据
     *@param path Node path
     *@param stat Node stat，可以传null
     *@return
     *@throws Exception
     */
    public byte[] readNode(String path, Stat stat) throws Exception {
        if (stat == null) {
            stat = new Stat();
        }
        byte[] data = client.getData().storingStatIn(stat).forPath(path);
        return data;
    }

    public static void main(String[] args) {
        String path = "/payment-sys/cash";
        CuratorClientTest test = null;
        try {
            test = new CuratorClientTest();
            // test.deleteNode(path);
            // test.createNode(path, "测试数据内容".getBytes());
            Stat stat = new Stat();
            byte[] data = test.readNode(path, stat);
            logger.info("返回数据：{}", new String(data));
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("操作失败：", e);
        } finally {
            test.closeClient();
        }
    }

}
