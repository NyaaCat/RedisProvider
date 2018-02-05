package cat.nyaa.nyaacore.database.redis;

import cat.nyaa.nyaacore.database.Database;
import cat.nyaa.nyaacore.database.DatabaseProvider;
import cat.nyaa.nyaacore.database.KeyValueDB;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.api.async.RedisAsyncCommands;
import com.lambdaworks.redis.api.sync.RedisCommands;
import org.apache.commons.lang.Validate;
import org.bukkit.plugin.Plugin;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

public class LettuceRedisProvider implements DatabaseProvider {
    @Override
    public Database get(Plugin plugin, Map<String, Object> map) {
        Object url = map.get("url");
        RedisClient client;
        if (url != null){
            client = RedisClient.create((String) map.get("url"));
        } else {
            client = RedisClient.create(RedisURI.builder()
                                       .withHost((String) map.get("host"))
                                       .withPort((int) map.get("port"))
                                       .withPassword((String) map.get("password"))
                                       .withDatabase((int) map.get("database"))
                                       .build());
        }
        return new LettuceRedisDB(client);
    }

    public class LettuceRedisDB implements KeyValueDB<String, String>{

        private final RedisClient client;
        private StatefulRedisConnection<String, String> connection;
        private RedisCommands<String, String> sync = null;
        private RedisAsyncCommands<String, String> async = null;

        LettuceRedisDB(RedisClient client) {
            this.client = client;
        }

        @Override
        public String get(String key) {
            return sync.get(key);
        }

        @Override
        public CompletableFuture<String> getAsync(String key) {
            return async.get(key).toCompletableFuture();
        }

        @Override
        public String get(String key, Function<? super String, ? extends String> loader) {
            String result = sync.get(key);
            if(result == null){
                result = loader.apply(key);
                sync.set(key, result);
            }
            return result;
        }

        @Override
        public CompletableFuture<String> getAsync(String key, Function<? super String, ? extends String> mappingFunction) {
            return async.get(key).thenApply(s -> s == null ? mappingFunction.apply(key) : s).toCompletableFuture();
        }

        @Override
        public String put(String k, String v) {
            return sync.getset(k, v);
        }

        @Override
        public CompletableFuture<String> putAsync(String key, String value) {
            return async.getset(key, value).toCompletableFuture();
        }

        @Override
        public String remove(String key) {
            String val = sync.get(key);
            sync.del(key);
            return val;
        }

        @Override
        public CompletableFuture<String> removeAsync(String key) {
            return async.get(key).thenApply((s) -> {async.del(key); return s;}).toCompletableFuture();
        }

        @Override
        public Collection<String> getAll(String key) {
            return Collections.singleton(sync.get(key));
        }

        @Override
        public CompletableFuture<Collection<String>> getAllAsync(String key) {
            return async.get(key).thenApply(s -> (Collection<String>)Collections.singleton(s)).toCompletableFuture();
        }

        @Override
        public Map<String, String> asMap() {
            return new Map<String, String>() {
                @Override
                public int size() {
                    return sync.dbsize().intValue();
                }

                @Override
                public boolean isEmpty() {
                    return size() > 0;
                }

                @Override
                public boolean containsKey(Object key) {
                    return sync.exists((String) key) != 0;
                }

                @Override
                public boolean containsValue(Object value) {
                    return values().stream().anyMatch(value::equals);
                }

                @Override
                public String get(Object key) {
                    return sync.get((String) key);
                }

                @Override
                public String put(String key, String value) {
                    return sync.set(key, value);
                }

                @Override
                public String remove(Object key) {
                    String val = sync.get((String) key);
                    sync.del((String) key);
                    return val;
                }

                @Override
                public void putAll(Map<? extends String, ? extends String> m) {
                    Validate.isTrue(sync.multi().equals("OK"));
                    m.forEach((key, value) -> sync.set(key, value));
                    Validate.isTrue(!sync.exec().wasRolledBack());
                }

                @Override
                public void clear() {
                    sync.flushdb();
                }

                @Override
                public Set<String> keySet() {
                    return new HashSet<>(sync.keys("*"));
                }

                @Override
                public Collection<String> values() {
                    return sync.keys("*").stream().map(sync::get).collect(Collectors.toSet());
                }

                @Override
                public Set<Entry<String, String>> entrySet() {
                    return sync.keys("*").stream().map(k -> new AbstractMap.SimpleEntry<>(k, sync.get(k))).collect(Collectors.toSet());
                }
            };
        }

        @Override
        public void clear() {
            sync.flushdb();
        }

        @Override
        public CompletableFuture<Void> clearAsync() {
            return async.flushdb().thenAccept(s -> {}).toCompletableFuture();
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends Database> T connect() {
            connection = client.connect();
            sync = connection.sync();
            async = connection.async();
            return (T) this;
        }

        @Override
        public void close() {
            async.save();
            sync.save();
            connection.close();
            sync = null;
            async = null;
            client.shutdown();
        }
    }
}
