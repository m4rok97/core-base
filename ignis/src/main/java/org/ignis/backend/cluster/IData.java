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
package org.ignis.backend.cluster;

import org.ignis.backend.cluster.helpers.data.IDataMapHelper;
import org.ignis.backend.cluster.helpers.data.IDataReduceHelper;
import org.ignis.backend.properties.IProperties;
import org.ignis.rpc.IFunction;

/**
 *
 * @author César Pomar
 */
public class IData {

    private final long id;
    private final IProperties properties;

    public IData(long id, IProperties properties) {
        this.id = id;
        this.properties = properties;
    }

    public long getId() {
        return id;
    }
    
    public void setKeep(int level){
        //TODO
    }

    public IData map(IFunction function) {
        return new IDataMapHelper(this, properties).map(function);
    }

    public IData streamingMap(IFunction function) {
        return new IDataMapHelper(this, properties).streamingMap(function);
    }

    public IData reduceByKey(IFunction function) {
        return new IDataReduceHelper(this, properties).reduceByKey(function);
    }
    
    public void saveAsFile(String path, boolean join){
        //TODO
    }

}
