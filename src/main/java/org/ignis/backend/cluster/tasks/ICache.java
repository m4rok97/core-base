/*
 * Copyright (C) 2019 César Pomar
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
package org.ignis.backend.cluster.tasks;

import org.ignis.backend.exception.IgnisException;

/**
 *
 * @author César Pomar
 */
public class ICache {

    public static enum Level {
        NO_CACHE(0),
        PRESERVE(1),
        MEMORY(2),
        RAW_MEMORY(3),
        DISK(4);
        
        private int level;
        
        private Level(int level){
            this.level=level;
        }
        
        public int getInt(){
            return level;
        }
        
        public static Level fromInt(int level) throws IgnisException{
            switch(level){
                    case 0: return NO_CACHE;
                    case 1: return PRESERVE;
                    case 2: return MEMORY;
                    case 3: return RAW_MEMORY;
                    case 4: return DISK;
                    default: throw new IgnisException("Level "+level+" not found");
            }
        }
    }

    private final long id;
    private Level actualLevel;
    private Level nextLevel;

    public ICache(long id) {
        this. id = id;
        actualLevel = Level.NO_CACHE;
        nextLevel = Level.NO_CACHE;
    }

    public long getId() {
        return id;
    }
    
    public Level getActualLevel() {
        return actualLevel;
    }

    public void setActualLevel(Level actualLevel) {
        if(actualLevel != null){
            this.actualLevel = actualLevel;
        }
    }

    public Level getNextLevel() {
        return nextLevel;
    }

    public void setNextLevel(Level nextLevel) {
        if(nextLevel != null){
            this.nextLevel = nextLevel;
        }
    }
    
    public boolean isCached(){
        return actualLevel != Level.NO_CACHE;
    }

}
