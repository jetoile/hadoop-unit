/*
 * The MIT License
 *
 * Copyright (c) 2012, Ninja Squad
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package fr.jetoile.hadoop.test.hive;

import com.ninja_squad.dbsetup.destination.Destination;
import com.ninja_squad.dbsetup.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * A destination which uses the {@link DriverManager} to get a connection
 * @author JB Nizet
 */
@Immutable
public final class HiveDriverManagerDestination implements Destination {

    private final String url;
    private final String user;
    private final String password;

    /**
     * Constructor
     * @param url the URL of the database
     * @param user the user used to get a connection
     * @param password the password used to get a connection
     */
    public HiveDriverManagerDestination(@Nonnull String url, String user, String password) {
        Preconditions.checkNotNull(url, "url may not be null");
        this.url = url;
        this.user = user;
        this.password = password;
    }

    @Override
    public Connection getConnection() throws SQLException {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return DriverManager.getConnection(url, user, password);
    }

    @Override
    public String toString() {
        return "DriverManagerDestination [url="
               + url
               + ", user="
               + user
               + ", password="
               + password
               + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + url.hashCode();
        result = prime * result + ((password == null) ? 0 : password.hashCode());
        result = prime * result + ((user == null) ? 0 : user.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof HiveDriverManagerDestination)) return false;

        HiveDriverManagerDestination that = (HiveDriverManagerDestination) o;

        if (url != null ? !url.equals(that.url) : that.url != null) return false;
        if (user != null ? !user.equals(that.user) : that.user != null) return false;
        return password != null ? password.equals(that.password) : that.password == null;

    }
}
