package cat.nyaa.nyaacore.database.redis;

import cat.nyaa.nyaacore.database.Database;
import cat.nyaa.nyaacore.database.DatabaseProvider;
import cat.nyaa.nyaacore.database.KeyValueDB;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.api.async.RedisAsyncCommands;
import com.lambdaworks.redis.api.sync.RedisCommands;
import com.lambdaworks.redis.codec.RedisCodec;
import com.lambdaworks.redis.codec.StringCodec;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.Validate;
import org.bukkit.plugin.Plugin;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.logging.Level;

@SuppressWarnings("unchecked")
public class LettuceRedisProvider implements DatabaseProvider {

    @Override
    public KeyValueDB get(Plugin plugin, Map<String, Object> map) {
        Object url = map.get("url");
        RedisURI uri;
        if (url != null) {
            uri = RedisURI.create((String) map.get("url"));
        } else {
            uri = RedisURI.builder()
                          .withHost((String) map.get("host"))
                          .withPort((int) map.get("port"))
                          .withPassword((String) map.get("password"))
                          .withDatabase((int) map.get("database"))
                          .build();
        }
        try {
            Class<?> k = map.get("key") == null ? String.class : Class.forName((String) map.get("key"));
            Class<?> v = map.get("value") == null ? String.class : Class.forName((String) map.get("value"));
            RedisCodec codec;
            if (k.equals(String.class) && v.equals(String.class)) {
                codec = new StringCodec();
            } else {
                Function<Object, ByteBuffer> ek = getEncoder(k);
                Function<ByteBuffer, Object> dk = getDecoder(k);
                Function<Object, ByteBuffer> ev = getEncoder(v);
                Function<ByteBuffer, Object> dv = getDecoder(v);
                codec = new Codec(dk, dv, ek, ev);
            }
            return new LettuceRedisDB(codec, plugin, uri);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private Function<ByteBuffer, Object> getDecoder(Class<?> k) {
        if (k.isEnum()) {
/*            return (bb) -> {
                Long high = bb.getLong();
                Long low = bb.getLong();
                return new UUID(high, low);
            };*/
            return (bb) -> Enum.valueOf((Class<? extends Enum>) k, StandardCharsets.UTF_8.decode(bb).toString());
        } else if(k == UUID.class) {
            return (bb) -> UUID.fromString(StandardCharsets.UTF_8.decode(bb).toString());
        } else if(k == Long.class) {
            return ByteBuffer::getLong;
        } else if(k == Integer.class) {
            return ByteBuffer::getInt;
        } else if(k == String.class) {
            return (bb) -> StandardCharsets.UTF_8.decode(bb).toString();
        } else {
            throw new NotImplementedException();
        }
    }

    private Function<Object, ByteBuffer> getEncoder(Class<?> k) {
        if (k.isEnum()) {
            return (o) -> StandardCharsets.UTF_8.encode(((Enum) o).name());
        } else if(k == UUID.class) {
/*            return (o) -> {
                UUID uuid = (UUID) o;
                ByteBuffer bb = ByteBuffer.wrap(new byte[Long.BYTES * 2]);
                bb.putLong(uuid.getMostSignificantBits());
                bb.putLong(uuid.getLeastSignificantBits());
                return bb;
            };*/
            return (o) -> {
                UUID uuid = (UUID) o;
                return StandardCharsets.UTF_8.encode(uuid.toString());
            };
        } else if(k == Long.class){
            return (o) -> {
                Long n = (Long) o;
                ByteBuffer bb = ByteBuffer.wrap(new byte[Long.BYTES]);
                bb.putLong(n);
                return bb;
            };
        } else if(k == Integer.class){
            return (o) -> {
                Integer n = (Integer) o;
                ByteBuffer bb = ByteBuffer.wrap(new byte[Integer.BYTES]);
                bb.putInt(n);
                return bb;
            };
        } else if(k == Double.class){
            return (o) -> {
                Double n = (Double) o;
                ByteBuffer bb = ByteBuffer.wrap(new byte[Double.BYTES]);
                bb.putDouble(n);
                return bb;
            };
        } else if(k == String.class){
            return (o) -> {
                String n = (String) o;
                return ByteBuffer.wrap(n.getBytes());
            };
        } else {
            throw new NotImplementedException();
        }
    }

    public class LettuceRedisDB<K, V> implements KeyValueDB<K, V> {
        private final RedisCodec<K, V> codec;
        private final Plugin plugin;
        private final RedisURI uri;
        private RedisClient client;
        private StatefulRedisConnection<K, V> connection;
        private RedisCommands<K, V> sync = null;
        private RedisAsyncCommands<K, V> async = null;

        LettuceRedisDB(RedisCodec<K, V> codec, Plugin plugin, RedisURI uri) {
            this.codec = codec;
            this.plugin = plugin;
            this.uri = uri;
        }

        @Override
        public V get(K key) {
            return sync.get(key);
        }

        @Override
        public CompletableFuture<V> getAsync(K key) {
            return async.get(key).toCompletableFuture();
        }

        @Override
        public V get(K key, Function<? super K, ? extends V> loader) {
            V result = sync.get(key);
            if (result == null) {
                result = loader.apply(key);
                sync.set(key, result);
            }
            return result;
        }

        @Override
        public CompletableFuture<V> getAsync(K key, Function<? super K, ? extends V> mappingFunction) {
            return async.get(key).thenApply(s -> s == null ? mappingFunction.apply(key) : s).toCompletableFuture();
        }

        @Override
        public V put(K k, V v) {
            return sync.getset(k, v);
        }

        @Override
        public CompletableFuture<V> putAsync(K key, V value) {
            return async.getset(key, value).toCompletableFuture();
        }

        @Override
        public V remove(K key) {
            V val = sync.get(key);
            sync.del(key);
            return val;
        }

        @Override
        public CompletableFuture<V> removeAsync(K key) {
            return async.get(key).thenApply((s) -> {
                async.del(key);
                return s;
            }).toCompletableFuture();
        }

        @Override
        public Collection<V> getAll(K key) {
            return Collections.singleton(sync.get(key));
        }

        @Override
        public CompletableFuture<Collection<V>> getAllAsync(K key) {
            return async.get(key).thenApply(s -> (Collection<V>) Collections.singleton(s)).toCompletableFuture();
        }

        @Override
        public Map<K, V> asMap() {
            return new Map<K, V>() {
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
                    return sync.exists((K) key) != 0;
                }

                @Override
                public boolean containsValue(Object value) {
                    throw new NotImplementedException();
                }

                @Override
                public V get(Object key) {
                    return sync.get((K) key);
                }

                @Override
                public V put(K key, V value) {
                    return sync.getset(key, value);
                }

                @Override
                public V remove(Object key) {
                    V val = sync.get((K) key);
                    sync.del((K) key);
                    return val;
                }

                @Override
                public void putAll(Map<? extends K, ? extends V> m) {
                    Validate.isTrue(sync.multi().equals("OK"));
                    m.forEach((key, value) -> sync.set(key, value));
                    Validate.isTrue(!sync.exec().wasRolledBack());
                }

                @Override
                public void clear() {
                    sync.flushdb();
                }

                @Override
                public Set<K> keySet() {
                    throw new NotImplementedException();
                }

                @Override
                public Collection<V> values() {
                    throw new NotImplementedException();
                }

                @Override
                public Set<Entry<K, V>> entrySet() {
                    throw new NotImplementedException();
                }
            };
        }

        @Override
        public void clear() {
            sync.flushdb();
        }

        @Override
        public CompletableFuture<Void> clearAsync() {
            return async.flushdb().thenAccept(s -> {
            }).toCompletableFuture();
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends Database> T connect() {
            if (plugin != null) {
                plugin.getLogger().log(Level.INFO, "Connecting redis server " + uri.toString());
            }
            client = RedisClient.create(uri);
            connection = client.connect(codec);
            sync = connection.sync();
            async = connection.async();
            return (T) this;
        }

        @Override
        public void close() {
            if (plugin != null) {
                plugin.getLogger().log(Level.INFO, "Disconnecting redis server " + uri.toString());
            }
            async.save();
            sync.save();
            connection.close();
            sync = null;
            async = null;
            connection = null;
            client.shutdown();
            client = null;
        }
    }

    public class Codec<K, V> implements RedisCodec<K, V> {

        private final Function<ByteBuffer, K> dk;
        private final Function<ByteBuffer, V> dv;
        private final Function<K, ByteBuffer> ek;
        private final Function<V, ByteBuffer> ev;

        private Codec(Function<ByteBuffer, K> dk,
                      Function<ByteBuffer, V> dv,
                      Function<K, ByteBuffer> ek,
                      Function<V, ByteBuffer> ev) {
            this.dv = dv;
            this.dk = dk;
            this.ev = ev;
            this.ek = ek;
        }

        @Override
        public K decodeKey(ByteBuffer bytes) {
            return dk.apply(bytes);
        }

        @Override
        public V decodeValue(ByteBuffer bytes) {
            return dv.apply(bytes);
        }

        @Override
        public ByteBuffer encodeKey(K key) {
            return ek.apply(key);
        }

        @Override
        public ByteBuffer encodeValue(V value) {
            return ev.apply(value);
        }
    }
}
