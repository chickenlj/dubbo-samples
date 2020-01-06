package org.apache.dubbo.samples.perf.registry;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.springframework.util.CollectionUtils;

import java.net.URLEncoder;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class ZookeeperNotificationMock {
    private static String zookeeperHost = System.getProperty("zookeeper.address", "11.164.235.9");
    private static String ROOT_PATH = "/dubbo/org.apache.dubbo.demo.DemoService/providers/";
    private static ExecutorService executorService = Executors.newFixedThreadPool(100);
    private static String[] nodePathes;
    private static CuratorFramework client;

    public static void main(String[] args) throws Exception {
        initClient();
        if (args.length == 0) {
            deleteProviders();
        } else {
            ROOT_PATH = "/dubbo/" + args[1] + "/providers/";
            initProviders(args[0] + "/" + args[1], args[1]);
            System.out.println("init finished, please input any character to continue...");
            System.in.read();
            mockProvidersChange(Integer.parseInt(args[2]));
        }
    }

    public static void initClient() throws Exception {
        client = CuratorFrameworkFactory.newClient(zookeeperHost + ":2181", 60 * 1000, 60 * 1000,
                new ExponentialBackoffRetry(1000, 3));
        client.start();
    }

    public static void initProviders(String addressAndService, String interfaceName) throws Exception {
        nodePathes = new String[1000];
        for (int i = 0; i < 1000; i++) {
            String providerUrl = "dubbo://" + addressAndService + "?anyhost=true&application=demo-provider&bind.ip=30.5.125.122&bind.port=20880&deprecated=false&dubbo=2.0.2&dynamic=true&generic=false&interface=" +
                    interfaceName + "&methods=testVoid,sayHello&pid=19175&release=2.7.5-SNAPSHOT&side=provider&" +
                    "timestamp=" + System.currentTimeMillis();
            try {
                String path = ROOT_PATH + URLEncoder.encode(providerUrl, "utf-8");
                nodePathes[i] = path;
                if (client.checkExists().forPath(path) == null) {
                    client.create().creatingParentsIfNeeded().forPath(path);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        Thread.sleep(100);
    }

    public static void mockProvidersChange(int count) throws Exception {
        Random r = new Random();
        String[] changes = new String[100];
        for (int index = 0; index < count; index++) {
            CountDownLatch deleteLatch = new CountDownLatch(100);
            for (int i = 0; i < 100; i++) {
                String path = nodePathes[r.nextInt(1000)];
                changes[i] = path;
                executorService.submit(() -> {
                    try {
                        deleteNode(path);
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        deleteLatch.countDown();
                    }
                });
            }
            deleteLatch.await();

            Thread.sleep(100);

            CountDownLatch createLatch = new CountDownLatch(100);
            for (int i = 0; i < 100; i++) {
                try {
                    createNode(changes[i]);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    createLatch.countDown();
                }
            }
            createLatch.await();

            Thread.sleep(100);
        }
    }

    public static void deleteProviders() throws Exception {
        List<String> children = client.getChildren().forPath(ROOT_PATH.substring(0, ROOT_PATH.length() - 1));
        if (!CollectionUtils.isEmpty(children)) {
            children.forEach(c -> {
                try {
                    deleteNode(ROOT_PATH + c);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
    }

    private static void createNode(String path) throws Exception {
        client.create().withMode(CreateMode.EPHEMERAL).forPath(path);
        System.out.println("Creating " + path);
    }

    private static void deleteNode(String path) throws Exception {
        client.delete().forPath(path);
        System.out.println("Deleting " + path);
    }

}
