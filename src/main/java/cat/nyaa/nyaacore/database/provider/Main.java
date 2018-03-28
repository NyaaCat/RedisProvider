package cat.nyaa.nyaacore.database.provider;

import cat.nyaa.nyaacore.database.DatabaseUtils;
import org.bukkit.Bukkit;
import org.bukkit.plugin.java.JavaPlugin;

import java.util.logging.Level;

public class Main extends JavaPlugin {
    @Override
    public void onEnable() {
        if(!DatabaseUtils.hasProvider("redis")){
            Bukkit.getLogger().log(Level.INFO, "Registering LettuceRedisProvider");
            DatabaseUtils.registerProvider("redis", new LettuceRedisProvider());
        }
    }

    @Override
    public void onDisable() {
        if(DatabaseUtils.hasProvider("redis")){
            Bukkit.getLogger().log(Level.INFO, "Unregistering LettuceRedisProvider");
            DatabaseUtils.unregisterProvider("redis");
        }
    }

    @Override
    public void onLoad() {
        Bukkit.getLogger().log(Level.INFO, "Registering LettuceRedisProvider");
        DatabaseUtils.registerProvider("redis", new LettuceRedisProvider());
    }
}
