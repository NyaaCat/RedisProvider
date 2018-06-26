package cat.nyaa.nyaacore.database.provider;

import cat.nyaa.nyaacore.database.Database;
import cat.nyaa.nyaacore.database.DatabaseProvider;
import cat.nyaa.nyaacore.database.KeyValueDB;
import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;
import com.google.common.util.concurrent.Futures;
import com.lambdaworks.redis.*;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.api.async.RedisAsyncCommands;
import com.lambdaworks.redis.api.sync.RedisCommands;
import com.lambdaworks.redis.codec.RedisCodec;
import com.lambdaworks.redis.codec.StringCodec;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.Validate;
import org.bukkit.plugin.Plugin;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.stream.Stream;

@SuppressWarnings("unchecked")
public class LettuceRedisProvider implements DatabaseProvider {

    @Override
    public KeyValueDB get(Plugin plugin, Map<String, Object> map) {
        String url = (String) map.get("url");
        RedisURI uri;
        if (url != null) {
            uri = RedisURI.create(url);
        } else {
            uri = RedisURI.builder()
                          .withHost((String) Objects.requireNonNull(map.get("host"), "'host' is required in redis provider"))
                          .withPort((int) Objects.requireNonNull(map.get("port"), "'port' is required in redis provider"))
                          .withPassword((String) Objects.requireNonNull(map.get("password"), "'password' is required in redis provider"))
                          .withDatabase((int) Objects.requireNonNull(map.get("database"), "'password' is required in redis provider"))
                          .build();
        }
        String prefix = (String) map.get("prefix");
        try {
            Class<?> k = map.get("key") == null ? String.class : Class.forName((String) map.get("key"));
            Class<?> v = map.get("value") == null ? String.class : Class.forName((String) map.get("value"));
            RedisCodec codec;
            if (k.equals(String.class) && v.equals(String.class) && prefix == null) {
                codec = new StringCodec();
            } else {
                Function<Object, ByteBuffer> ek = getEncoder(k);
                Function<ByteBuffer, Object> dk = getDecoder(k);
                Function<Object, ByteBuffer> ev = getEncoder(v);
                Function<ByteBuffer, Object> dv = getDecoder(v);
                codec = new Codec(dk, dv, ek, ev, prefix);
            }
            return new LettuceRedisDB(codec, plugin, uri, prefix);
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
        } else if (k == UUID.class) {
            return (bb) -> UUID.fromString(StandardCharsets.UTF_8.decode(bb).toString());
        } else if (k == Long.class) {
            return ByteBuffer::getLong;
        } else if (k == Integer.class) {
            return ByteBuffer::getInt;
        } else if (k == String.class) {
            return (bb) -> StandardCharsets.UTF_8.decode(bb).toString();
        } else {
            throw new NotImplementedException();
        }
    }

    private Function<Object, ByteBuffer> getEncoder(Class<?> k) {
        if (k.isEnum()) {
            return (o) -> StandardCharsets.UTF_8.encode(((Enum) o).name());
        } else if (k == UUID.class) {
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
        } else if (k == Long.class) {
            return (o) -> {
                Long n = (Long) o;
                ByteBuffer bb = ByteBuffer.allocate(Long.BYTES);
                bb.putLong(n).rewind();
                return bb;
            };
        } else if (k == Integer.class) {
            return (o) -> {
                Integer n = (Integer) o;
                ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES);
                bb.putInt(n).rewind();
                return bb;
            };
        } else if (k == Double.class) {
            return (o) -> {
                Double n = (Double) o;
                ByteBuffer bb = ByteBuffer.allocate(Double.BYTES);
                bb.putDouble(n).rewind();
                return bb;
            };
        } else if (k == String.class) {
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
        private final String prefix;
        private RedisClient client;
        private StatefulRedisConnection<K, V> connection;
        private RedisCommands<K, V> sync = null;
        private RedisAsyncCommands<K, V> async = null;

        LettuceRedisDB(RedisCodec<K, V> codec, Plugin plugin, RedisURI uri, String prefix) {
            this.codec = codec;
            this.plugin = plugin;
            this.uri = uri;
            this.prefix = prefix;
        }

        @Override
        public int size() {
            if (prefix == null) {
                return sync.dbsize().intValue();
            }
            ScanArgs s = new ScanArgs().match(prefix + "*");
            KeyScanCursor<K> c = sync.scan(s);
            List<K> keys = new ArrayList<>(c.getKeys());
            while (!c.isFinished()) {
                c = sync.scan(c, s);
                keys.addAll(c.getKeys());
            }
            return keys.size();
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
        public boolean containsKey(K key){
            return sync.exists((K) key) != 0;
        }

        @Override
        public Map<K, V> asMap() {
            return new Map<K, V>() {
                @Override
                public int size() {
                    return LettuceRedisDB.this.size();
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
                    return LettuceRedisDB.this.get((K) key);
                }

                @Override
                public V put(K key, V value) {
                    return LettuceRedisDB.this.put(key, value);
                }

                @Override
                public V remove(Object key) {
                    return LettuceRedisDB.this.remove((K) key);
                }

                @Override
                public void putAll(Map<? extends K, ? extends V> m) {
                    Validate.isTrue(sync.multi().equals("OK"));
                    m.forEach((key, value) -> sync.set(key, value));
                    Validate.isTrue(!sync.exec().wasRolledBack());
                }

                @Override
                public void clear() {
                    LettuceRedisDB.this.clear();
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
            if (prefix == null) {
                sync.flushdb();
                return;
            }
            ScanArgs s = new ScanArgs().match(prefix + "*");
            KeyScanCursor<K> c = sync.scan(s);
            List<RedisFuture<Long>> awaits = new ArrayList<>();
            if(!c.getKeys().isEmpty()){
                awaits.add(async.del((K[]) c.getKeys().toArray()));
            }
            while (!c.isFinished()) {
                c = sync.scan(c, s);
                if(!c.getKeys().isEmpty()){
                    awaits.add(async.del((K[]) c.getKeys().toArray()));
                }
            }
            Validate.isTrue(LettuceFutures.awaitAll(10, TimeUnit.SECONDS, awaits.toArray(new Future<?>[0])));
        }

        @Override
        public CompletableFuture<Void> clearAsync() {
            if (prefix == null) {
                return async.flushdb().thenAccept(s -> {
                }).toCompletableFuture();
            }
            ScanArgs s = new ScanArgs().match(prefix + "*");
            KeyScanCursor<K> c = sync.scan(s);
            List<RedisFuture<Long>> awaits = new ArrayList<>();
            if(!c.getKeys().isEmpty()){
                awaits.add(async.del((K[]) c.getKeys().toArray()));
            }
            while (!c.isFinished()) {
                c = sync.scan(c, s);
                if(!c.getKeys().isEmpty()){
                    awaits.add(async.del((K[]) c.getKeys().toArray()));
                }
            }
            return CompletableFuture.allOf(awaits.toArray(new CompletableFuture<?>[0]));
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

        @Override
        protected void finalize(){
            if(connection != null){
                close();
            }
        }
    }

    public class Codec<K, V> implements RedisCodec<K, V> {

        private final Function<ByteBuffer, K> dk;
        private final Function<ByteBuffer, V> dv;
        private final Function<K, ByteBuffer> ek;
        private final Function<V, ByteBuffer> ev;
        private final byte[] prefixBytes;

        private Codec(Function<ByteBuffer, K> dk,
                      Function<ByteBuffer, V> dv,
                      Function<K, ByteBuffer> ek,
                      Function<V, ByteBuffer> ev,
                      String prefix) {
            this.dv = dv;
            this.dk = dk;
            this.ev = ev;
            this.ek = ek;
            if (prefix == null) {
                prefixBytes = null;
            } else {
                prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);
            }
        }

        @Override
        public K decodeKey(ByteBuffer bytes) {
            bytes.position(prefixBytes.length);
            try {
                return dk.apply(bytes);
            } catch (BufferUnderflowException e){
                bytes.rewind();
                byte[] contents = new byte[bytes.remaining()];
                bytes.put(contents);
                throw new RuntimeException("key with prefix " + new String(prefixBytes) + "is not decodeable: " + Arrays.toString(contents), e);
            }
        }

        @Override
        public V decodeValue(ByteBuffer bytes) {
            return dv.apply(bytes);
        }

        @Override
        public ByteBuffer encodeKey(K key) {
            ByteBuffer o = ek.apply(key);
            if (prefixBytes == null) {
                return o;
            }
            return ByteBuffer.wrap(Bytes.concat(prefixBytes, o.array()));
        }

        @Override
        public ByteBuffer encodeValue(V value) {
            return ev.apply(value);
        }
    }
}