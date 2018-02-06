package cat.nyaa.nyaacore.database.redis;

import cat.nyaa.nyaacore.database.DatabaseUtils;
import cat.nyaa.nyaacore.database.KeyValueDB;
import org.junit.*;
import org.junit.rules.ExpectedException;
import redis.embedded.RedisServer;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RedisProviderTest {

    private static RedisServer redisServer;

    private static int port = 6379;

    @BeforeClass
    public static void setup() throws IOException {
        ServerSocket s = new ServerSocket(0);
        port = s.getLocalPort();
        s.close();
        redisServer = new RedisServer(port);
        redisServer.start();
        port = redisServer.ports().stream().findFirst().get();
        DatabaseUtils.registerProvider("redis", new LettuceRedisProvider());
    }

    @Test
    public void canConnect(){
        Map<String, Object> conf = new HashMap<>();
        conf.put("url", "redis://localhost:" + port +"/");
        KeyValueDB<String, String> db = DatabaseUtils.get("redis", null, conf);
        db.connect();
        db.close();
    }

    @Test
    public void canGetSet(){
        Map<String, Object> conf = new HashMap<>();
        conf.put("url", "redis://localhost:" + port +"/");
        KeyValueDB<String, String> db = DatabaseUtils.get("redis", null, conf);
        db.connect();
        db.clear();
        db.put("k", "v");
        Assert.assertEquals(db.get("k"), "v");
        db.close();
    }

    @Test
    public void canGetSetAsync(){
        Map<String, Object> conf = new HashMap<>();
        conf.put("url", "redis://localhost:" + port +"/");
        KeyValueDB<String, String> db = DatabaseUtils.get("redis", null, conf);
        db.connect();
        db.clear();
        db.putAsync("k", "v").thenAccept((n) -> Assert.assertEquals(db.getAsync("k").join(), "v")).thenAccept((n) -> db.close());
    }

    @Test
    public void canGetWithLoader(){
        Map<String, Object> conf = new HashMap<>();
        conf.put("url", "redis://localhost:" + port +"/");
        KeyValueDB<String, String> db = DatabaseUtils.get("redis", null, conf);
        db.connect();
        db.clear();
        Assert.assertEquals(db.get("k"), null);
        db.get("k", (k) -> k + k);
        Assert.assertEquals(db.get("k"), "kk");
        db.close();
    }

    @Test
    public void canNotReconnect() throws InterruptedException {
        Map<String, Object> conf = new HashMap<>();
        conf.put("url", "redis://localhost:" + port +"/");
        KeyValueDB<String, String> db = DatabaseUtils.get("redis", null, conf);
        db.connect();
        db.clear();
        db.put("k", "v");
        db.close();
        TimeUnit.SECONDS.sleep(2);
        db.connect();
        db.close();
    }

    @Test
    public void canReadPersistedValue() throws InterruptedException {
        Map<String, Object> conf = new HashMap<>();
        conf.put("url", "redis://localhost:" + port +"/");
        KeyValueDB<String, String> db = DatabaseUtils.get("redis", null, conf);
        db.connect();
        db.clear();
        db.put("k", "v");
        db.close();
        TimeUnit.SECONDS.sleep(2);
        db = DatabaseUtils.get("redis", null, conf);
        db.connect();
        Assert.assertEquals(db.get("k"), "v");
        db.close();
    }

    @Test
    public void canRwMap() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("url", "redis://localhost:" + port +"/");
        KeyValueDB<String, String> db = DatabaseUtils.get("redis", null, conf);
        db.connect();
        db.clear();
        Map<String, String> dbMap = db.asMap();
        dbMap.put("km", "vm");
        Assert.assertEquals(db.get("km"), "vm");
        db.put("kr", "vr");
        Assert.assertEquals(dbMap.get("kr"), "vr");
        Assert.assertTrue(dbMap.containsKey("kr"));
    }

    @Test
    public void canClear() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("host", "localhost");
        conf.put("port", port);
        conf.put("database", 0);
        conf.put("password", "");
        KeyValueDB<String, String> db = DatabaseUtils.get("redis", null, conf);
        db.connect();
        db.clear();
        db.put("k", "v");
        Map<String, String> dbMap = db.asMap();
        Assert.assertEquals(dbMap.size(), 1);
        db.put("k2", "v2");
        Assert.assertEquals(dbMap.size(), 2);
        db.clear();
        Assert.assertEquals(dbMap.size(), 0);
        db.close();
    }

    @AfterClass
    public static void down(){
        redisServer.stop();
    }
}
