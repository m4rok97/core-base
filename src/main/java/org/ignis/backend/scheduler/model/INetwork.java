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
package org.ignis.backend.scheduler.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;

/**
 *
 * @author César Pomar
 */
@Getter
public class INetwork {

    public INetwork() {
        tcpMap = new HashMap<>();
        udpMap = new HashMap<>();
        tcpPorts = new ArrayList<>();
        udpPorts = new ArrayList<>();
    }

    private final Map<Integer, Integer> tcpMap;
    private final Map<Integer, Integer> udpMap;
    private final List<Integer> tcpPorts;
    private final List<Integer> udpPorts;
}
