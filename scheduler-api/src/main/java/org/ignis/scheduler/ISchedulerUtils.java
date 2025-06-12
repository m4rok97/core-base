package org.ignis.scheduler;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.ignis.properties.IProperties;
import org.ignis.scheduler.model.IContainerInfo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

public final class ISchedulerUtils {

    private ISchedulerUtils() {
    }

    public static String genId() {
        ByteBuffer bb = ByteBuffer.allocate(16);
        var uuid = UUID.randomUUID();
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        var b64 = Base64.getEncoder().withoutPadding().encodeToString(bb.array());
        return b64.replace("+","0").replace("/", "1");
    }

    private static final Pattern NAME_CHECK = Pattern.compile("[^a-zA-Z0-9_]");

    public static String name(String rawName) {
        var matcher = NAME_CHECK.matcher(rawName);
        return matcher.replaceAll("");
    }

    public static <T> String yaml(T obj){
        try {
            var yamlMapper = new YAMLMapper();
            var baos = new ByteArrayOutputStream();
            yamlMapper.writeValue(baos, obj);
            return baos.toString();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static <T> String yaml(T obj, String key){
        try {
            var yamlMapper = new YAMLMapper();
            Object fields = yamlMapper.convertValue(obj, Map.class);
            for (var name: IProperties.split(key)){
                if(fields == null){
                    break;
                }
                if (fields instanceof Map m){
                    fields = m.get(name);
                }else{
                    break;
                }
            }
            if(fields == null){
                return "";
            }
            return yaml(fields);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static String encode(IContainerInfo request) {
        try {
            var yamlMapper = new YAMLMapper();
            var baos = new ByteArrayOutputStream();
            yamlMapper.writeValue(baos, request);
            return Base64.getEncoder().encodeToString(baos.toByteArray());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static IContainerInfo decode(String request) {
        try {
            var yamlMapper = new YAMLMapper();
            var bais = new ByteArrayInputStream(Base64.getDecoder().decode(request));
            return yamlMapper.readValue(bais, IContainerInfo.class);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }


}
