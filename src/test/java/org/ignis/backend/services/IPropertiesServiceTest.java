/*
 * Copyright (C) 2018 
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.ignis.backend.services;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public class IPropertiesServiceTest {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IPropertiesServiceTest.class);

    @BeforeAll
    public static void info() {
        LOGGER.info("----|----|----|----IPropertiesServiceTest----|----|----|----");
    }

    @Test
    public void newInstance() throws Exception {
        LOGGER.info("----|----newInstance----|----");
        IAttributes attributes = new IAttributes();
        IPropertiesServiceImpl service = new IPropertiesServiceImpl(attributes);

        long id = service.newInstance();

        Assert.assertNotNull(attributes.getProperties(id));
    }

    @Test
    public void newInstance2() throws Exception {
        LOGGER.info("----|----newInstance2----|----");
        IAttributes attributes = new IAttributes();
        IPropertiesServiceImpl service = new IPropertiesServiceImpl(attributes);

        long id1 = service.newInstance();
        attributes.getProperties(id1).setProperty("key", "value");
        long id2 = service.newInstance2(id1);
        attributes.getProperties(id1).setProperty("key", "value2");

        Assert.assertEquals("value", attributes.getProperties(id2).getProperty("key"));
    }

    @Test
    public void setProperty() throws Exception {
        LOGGER.info("----|----setProperty----|----");
        IAttributes attributes = new IAttributes();
        IPropertiesServiceImpl service = new IPropertiesServiceImpl(attributes);

        long id1 = service.newInstance();

        Assert.assertEquals("", service.setProperty(id1, "key", "valueOld"));
        Assert.assertEquals("valueOld", service.setProperty(id1, "key", "value"));

        Assert.assertEquals("value", attributes.getProperties(id1).getProperty("key"));

        service.setProperty(id1, "value", "1");
        Assert.assertEquals("1", attributes.getProperties(id1).getProperty(""));

        service.setProperty(id1, "key.value", "2");
        Assert.assertEquals("2", attributes.getProperties(id1).getProperty("key"));

        service.setProperty(id1, "value.key", "3");
        Assert.assertEquals("3", attributes.getProperties(id1).getProperty("key"));

        service.setProperty(id1, "key.value.value", "4");
        Assert.assertEquals("4", attributes.getProperties(id1).getProperty("key"));
    }

    @Test
    public void getProperty() throws Exception {
        LOGGER.info("----|----getProperty----|----");
        IAttributes attributes = new IAttributes();
        IPropertiesServiceImpl service = new IPropertiesServiceImpl(attributes);

        long id1 = service.newInstance();
        attributes.getProperties(id1).setProperty("key", "value");

        Assert.assertEquals("value", service.getProperty(id1, "key"));
        Assert.assertEquals("", service.getProperty(id1, "key2"));
    }

    @Test
    public void isProperty() throws Exception {
        LOGGER.info("----|----isProperty----|----");
        IAttributes attributes = new IAttributes();
        IPropertiesServiceImpl service = new IPropertiesServiceImpl(attributes);

        long id1 = service.newInstance();
        attributes.getProperties(id1).setProperty("key", "value");

        Assert.assertTrue(service.contains(id1, "key"));
        Assert.assertFalse(service.contains(id1, "key2"));
    }

    @Test
    public void toMap() throws Exception {
        LOGGER.info("----|----toMap----|----");
        IAttributes attributes = new IAttributes();
        IPropertiesServiceImpl service = new IPropertiesServiceImpl(attributes);

        long id1 = service.newInstance();
        attributes.getProperties(id1).setProperty("key", "value");
        attributes.getProperties(id1).setProperty("key2", "value2");
        attributes.getProperties(id1).setProperty("key3", "value3");
        attributes.getProperties(id1).setProperty("key4", "value4");

        Map<String, String> map = service.toMap(id1, false);

        Assert.assertEquals(4, map.size());
        Assert.assertEquals("value", map.get("key"));
        Assert.assertEquals("value2", map.get("key2"));
        Assert.assertEquals("value3", map.get("key3"));
        Assert.assertEquals("value4", map.get("key4"));
    }

    @Test
    public void fromMap() throws Exception {
        LOGGER.info("----|----fromMap----|----");
        IAttributes attributes = new IAttributes();
        IPropertiesServiceImpl service = new IPropertiesServiceImpl(attributes);

        Map<String, String> map = new HashMap<>();
        map.put("key", "value");
        map.put("key2", "value2");
        map.put("key3", "value3");
        map.put("key4", "value4");

        long id1 = service.newInstance();
        service.fromMap(id1, map);

        Assert.assertEquals("value", attributes.getProperties(id1).getProperty("key"));
        Assert.assertEquals("value2", attributes.getProperties(id1).getProperty("key2"));
        Assert.assertEquals("value3", attributes.getProperties(id1).getProperty("key3"));
        Assert.assertEquals("value4", attributes.getProperties(id1).getProperty("key4"));
    }

    @Test
    public void fromFileToFile() throws Exception {
        LOGGER.info("----|----fromFileToFile----|----");
        IAttributes attributes = new IAttributes();
        IPropertiesServiceImpl service = new IPropertiesServiceImpl(attributes);
        File tmp = File.createTempFile("test", null);

        //To file
        long id1 = service.newInstance();
        attributes.getProperties(id1).setProperty("key.keyB", "value1");
        attributes.getProperties(id1).setProperty("key.keyB.keyC", "value2");
        attributes.getProperties(id1).setProperty("key2.keyB", "value3");
        service.store(id1, tmp.getAbsolutePath());

        //From file
        long id2 = service.newInstance();
        service.load(id2, tmp.getAbsolutePath());

        for (String key : new String[]{"key.keyB", "key2.keyB", "key.keyB.keyC"}) {
            Assert.assertEquals(
                    attributes.getProperties(id1).getProperty(key),
                    attributes.getProperties(id2).getProperty(key)
            );
        }
    }

    @Test
    public void reset() throws Exception {
        LOGGER.info("----|----clear----|----");
        IAttributes attributes = new IAttributes();
        IPropertiesServiceImpl service = new IPropertiesServiceImpl(attributes);

        long id1 = service.newInstance();
        attributes.getProperties(id1).setProperty("key", "value");
        service.clear(id1);

        Assert.assertEquals("", service.getProperty(id1, "key"));
    }

}
