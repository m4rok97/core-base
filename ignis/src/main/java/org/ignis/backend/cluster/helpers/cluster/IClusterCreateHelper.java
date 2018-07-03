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
package org.ignis.backend.cluster.helpers.cluster;

import java.util.ArrayList;
import java.util.List;
import org.ignis.backend.allocator.IContainerStub;
import org.ignis.backend.cluster.ICluster;
import org.ignis.backend.cluster.IContainer;
import org.ignis.backend.cluster.tasks.ILock;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;
import org.ignis.backend.properties.IPropertiesKeys;
import org.ignis.backend.properties.IPropertiesParser;

/**
 *
 * @author César Pomar
 */
public class IClusterCreateHelper extends IClusterHelper {

    public IClusterCreateHelper(ICluster cluster, IProperties properties) {
        super(cluster, properties);
    }
    
    public List<IContainer> create(ILock lock) throws IgnisException {
        int instances = IPropertiesParser.getInteger(properties, IPropertiesKeys.EXECUTOR_INSTANCES);
        List<IContainer> result = new ArrayList<>();
        for (int i = 0; i < instances; i++) {
            IContainerStub stub = new IContainerStub(properties);
            result.add(new IContainer(stub, properties, lock));
        }
        return result;
    }

}
