package cat.nyaa.nyaacore.database.provider;

import cat.nyaa.nyaacore.database.DatabaseUtils;
import cat.nyaa.nyaacore.database.keyvalue.KeyValueDB;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.embedded.RedisServer;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class RedisProviderTest {

    private static RedisServer redisServer;

    private static int port = 6379;

    @BeforeClass
    public static void setup() throws IOException {
        ServerSocket s = new ServerSocket(0);
        port = s.getLocalPort();
        s.close();
        redisServer = RedisServer.builder().port(port).setting("maxheap 128M").setting("bind 127.0.0.1").build();
        redisServer.start();
        port = redisServer.ports().stream().findFirst().orElseThrow(IllegalStateException::new);
        DatabaseUtils.registerProvider("redis", new LettuceRedisProvider());
    }

    @Test
    public void canConnect() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("url", "redis://localhost:" + port + "/");
        @SuppressWarnings("unchecked") KeyValueDB<String, String> db = DatabaseUtils.get("redis", null, conf, KeyValueDB.class);
        db.close();
    }

    @Test
    public void canGetSet() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("url", "redis://localhost:" + port + "/");
        @SuppressWarnings("unchecked") LettuceRedisProvider.LettuceRedisDB<String, String> db = (LettuceRedisProvider.LettuceRedisDB<String, String>) DatabaseUtils.get("redis", null, conf, KeyValueDB.class);
        db.clear();
        db.put("k", "v");
        Assert.assertEquals(db.get("k"), "v");
        db.close();
    }

    @Test
    public void canGetSetAsync() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("url", "redis://localhost:" + port + "/");
        @SuppressWarnings("unchecked") LettuceRedisProvider.LettuceRedisDB<String, String> db = (LettuceRedisProvider.LettuceRedisDB<String, String>) DatabaseUtils.get("redis", null, conf, KeyValueDB.class);
        db.clear();
        db.putAsync("k", "v").thenAccept((n) -> Assert.assertEquals(db.getAsync("k").join(), "v")).thenAccept((n) -> db.close());
    }

    @Test
    public void canGetWithLoader() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("url", "redis://localhost:" + port + "/");
        @SuppressWarnings("unchecked") LettuceRedisProvider.LettuceRedisDB<String, String> db = (LettuceRedisProvider.LettuceRedisDB<String, String>) DatabaseUtils.get("redis", null, conf, KeyValueDB.class);
        db.clear();
        Assert.assertNull(db.get("k"));
        db.get("k", (k) -> k + k);
        Assert.assertEquals(db.get("k"), "kk");
        db.close();
    }

    @Test
    public void canReconnect() throws InterruptedException {
        Map<String, Object> conf = new HashMap<>();
        conf.put("url", "redis://localhost:" + port + "/");
        @SuppressWarnings("unchecked") LettuceRedisProvider.LettuceRedisDB<String, String> db = (LettuceRedisProvider.LettuceRedisDB<String, String>) DatabaseUtils.get("redis", null, conf, KeyValueDB.class);
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
        conf.put("url", "redis://localhost:" + port + "/");
        @SuppressWarnings("unchecked") LettuceRedisProvider.LettuceRedisDB<String, String> db = (LettuceRedisProvider.LettuceRedisDB<String, String>) DatabaseUtils.get("redis", null, conf, KeyValueDB.class);
        db.clear();
        db.put("k", "v");
        db.close();
        TimeUnit.SECONDS.sleep(2);
        //noinspection unchecked
        db = (LettuceRedisProvider.LettuceRedisDB<String, String>) DatabaseUtils.get("redis", null, conf, KeyValueDB.class);
        Assert.assertEquals(db.get("k"), "v");
        db.close();
    }

    @Test
    public void canReadPersistedValueWithPrefix() throws InterruptedException {
        Map<String, Object> conf = new HashMap<>();
        conf.put("url", "redis://localhost:" + port + "/");
        conf.put("prefix", "nyaacat:redis:test4:");
        @SuppressWarnings("unchecked") LettuceRedisProvider.LettuceRedisDB<String, String> db = (LettuceRedisProvider.LettuceRedisDB<String, String>) DatabaseUtils.get("redis", null, conf, KeyValueDB.class);
        db.clear();
        db.put("k", "v");
        db.close();
        TimeUnit.SECONDS.sleep(2);
        //noinspection unchecked
        db = (LettuceRedisProvider.LettuceRedisDB<String, String>) DatabaseUtils.get("redis", null, conf, KeyValueDB.class);
        Assert.assertEquals(db.get("k"), "v");
        db.close();
    }

    @Test
    public void canRwMap() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("url", "redis://localhost:" + port + "/");
        @SuppressWarnings("unchecked") LettuceRedisProvider.LettuceRedisDB<String, String> db = (LettuceRedisProvider.LettuceRedisDB<String, String>) DatabaseUtils.get("redis", null, conf, KeyValueDB.class);
        db.clear();
        Map<String, String> dbMap = db.asMap();
        dbMap.put("km", "vm");
        Assert.assertEquals(db.get("km"), "vm");
        db.put("kr", "vr");
        Assert.assertEquals(dbMap.get("kr"), "vr");
        Assert.assertTrue(dbMap.containsKey("kr"));
    }

    @Test
    public void canRwInteger() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("url", "redis://localhost:" + port + "/");
        conf.put("key", Integer.class.getName());
        conf.put("value", Integer.class.getName());
        @SuppressWarnings("unchecked") LettuceRedisProvider.LettuceRedisDB<Integer, Integer> db = (LettuceRedisProvider.LettuceRedisDB<Integer, Integer>) DatabaseUtils.get("redis", null, conf, KeyValueDB.class);
        db.clear();
        db.put(1, 2);
        Assert.assertEquals(2, (int) db.get(1));
    }

    @Test
    public void canRwIntegerString() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("url", "redis://localhost:" + port + "/");
        conf.put("key", Integer.class.getName());
        @SuppressWarnings("unchecked") LettuceRedisProvider.LettuceRedisDB<Integer, String> db = (LettuceRedisProvider.LettuceRedisDB<Integer, String>) DatabaseUtils.get("redis", null, conf, KeyValueDB.class);
        db.clear();
        db.put(1, "Str");
        Assert.assertEquals("Str", db.get(1));
    }

    @Test
    public void canRwIntegerUtf8String() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("url", "redis://localhost:" + port + "/");
        conf.put("key", Integer.class.getName());
        @SuppressWarnings("unchecked") LettuceRedisProvider.LettuceRedisDB<Integer, String> db = (LettuceRedisProvider.LettuceRedisDB<Integer, String>) DatabaseUtils.get("redis", null, conf, KeyValueDB.class);
        db.clear();
        db.put(1, "中文");
        Assert.assertEquals("中文", db.get(1));
    }

    @Test
    public void canRwLongStringWithPrefix() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("url", "redis://localhost:" + port + "/");
        conf.put("key", Long.class.getName());
        conf.put("prefix", "nyaacat:redis:test3:");
        @SuppressWarnings("unchecked") LettuceRedisProvider.LettuceRedisDB<Long, String> db = (LettuceRedisProvider.LettuceRedisDB<Long, String>) DatabaseUtils.get("redis", null, conf, KeyValueDB.class);
        db.clear();
        db.put(1L, "Str");
        Assert.assertEquals("Str", db.get(1L));
    }

    @Test
    public void canRemoveAndClear() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("host", "localhost");
        conf.put("port", port);
        conf.put("database", 0);
        conf.put("password", "");
        @SuppressWarnings("unchecked") LettuceRedisProvider.LettuceRedisDB<String, String> db = (LettuceRedisProvider.LettuceRedisDB<String, String>) DatabaseUtils.get("redis", null, conf, KeyValueDB.class);
        db.clear();
        db.put("k", "v");
        Map<String, String> dbMap = db.asMap();
        Assert.assertEquals(1, dbMap.size());
        db.put("k2", "v2");
        Assert.assertEquals(2, db.size());
        db.clear();
        Assert.assertEquals(0, dbMap.size());
        db.put("k2", "v2");
        Assert.assertEquals(1, dbMap.size());
        Assert.assertEquals("v2", db.remove("k2"));
        db.remove("not exist");
        Assert.assertEquals(0, dbMap.size());
        db.removeAsync("yet another not exist");
        Assert.assertEquals(0, dbMap.size());
        db.put("k3", "v3");
        db.put("k4", "v4");
        Assert.assertEquals(2, db.size());
        dbMap.remove("k3");
        Assert.assertEquals(1, db.size());
        db.removeAsync("k4").thenAccept(v -> Assert.assertEquals("v4", v)).join();
        Assert.assertEquals(0, db.size());
        db.close();
    }

    @Test
    public void canGetSizeAndClearWithPrefix() throws InterruptedException, ExecutionException {
        Map<String, Object> conf = new HashMap<>();
        conf.put("host", "localhost");
        conf.put("port", port);
        conf.put("prefix", "nyaacat:redis:test:");
        conf.put("database", 0);
        conf.put("password", "");
        @SuppressWarnings("unchecked") LettuceRedisProvider.LettuceRedisDB<String, String> db = (LettuceRedisProvider.LettuceRedisDB<String, String>) DatabaseUtils.get("redis", null, conf, KeyValueDB.class);
        Map<String, Object> conf2 = new HashMap<>();
        conf2.put("url", "redis://localhost:" + port + "/");
        conf2.put("key", Integer.class.getName());
        conf2.put("prefix", "nyaacat:redis:test2:");
        @SuppressWarnings("unchecked") LettuceRedisProvider.LettuceRedisDB<Integer, String> db2 = (LettuceRedisProvider.LettuceRedisDB<Integer, String>) DatabaseUtils.get("redis", null, conf2, KeyValueDB.class);
        db.clear();
        db2.clear();

        db.put("k", "v");
        Map<String, String> dbMap = db.asMap();
        Assert.assertEquals(1, dbMap.size());
        db2.put(1, "2s");
        db.put("k2", "v2");
        Assert.assertEquals(2, db.size());
        Assert.assertEquals(1, db2.size());
        Assert.assertEquals(2, dbMap.size());
        db.clear();
        Assert.assertEquals(0, db.size());
        db.close();
        Assert.assertEquals(1, db2.size());
        db2.clearAsync().get();
        Assert.assertEquals(0, db2.size());
        db2.close();
    }

    @AfterClass
    public static void down() {
        redisServer.stop();
    }
}
