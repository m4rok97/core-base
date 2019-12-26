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
package org.ignis.backend.exception;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import org.ignis.rpc.IDriverException;

/**
 *
 * @author César Pomar
 */
public class IDriverExceptionImpl extends IDriverException {

    public IDriverExceptionImpl(Exception ex) {
        this(ex.getMessage(), ex);
    }

    public IDriverExceptionImpl(String msg) {
        super(msg, stackToString());
    }

    public IDriverExceptionImpl(String msg, Throwable cause) {
        this(msg, exceptionToString(cause));
    }
    
    public IDriverExceptionImpl(String msg, String cause) {
        super(msg, stackToString() + "\nCaused by: " + cause);
    }

    private static String exceptionToString(Throwable ex) {
        StringWriter writer = new StringWriter();
        ex.printStackTrace(new PrintWriter(writer));
        return writer.toString();
    }

    private static String stackToString() {
        Throwable t = new Throwable().fillInStackTrace();
        t.setStackTrace(Arrays.copyOfRange(t.getStackTrace(), 2, t.getStackTrace().length));
        String stack = exceptionToString(t);
        return stack.substring(stack.indexOf('\n') + 1);
    }
}
