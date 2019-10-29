/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.jetoile.hadoopunit.component;

public class OozieConfig {

    //Oozie
    public static final String OOZIE_TMP_DIR_KEY = "oozie.tmp.dir";
    public static final String OOZIE_TEST_DIR_KEY = "oozie.test.dir";
    public static final String OOZIE_HOME_DIR_KEY = "oozie.home.dir";
    public static final String OOZIE_USERNAME_KEY = "oozie.username";
    public static final String OOZIE_GROUPNAME_KEY = "oozie.groupname";
    public static final String OOZIE_HDFS_SHARE_LIB_DIR_KEY = "oozie.hdfs.share.lib.dir";
    public static final String OOZIE_SHARE_LIB_CREATE_KEY = "oozie.share.lib.create";
    public static final String OOZIE_LOCAL_SHARE_LIB_CACHE_DIR_KEY = "oozie.local.share.lib.cache.dir";
    public static final String OOZIE_PURGE_LOCAL_SHARE_LIB_CACHE_KEY = "oozie.purge.local.share.lib.cache";
    public static final String OOZIE_PORT = "oozie.port";
    public static final String OOZIE_HOST = "oozie.host";
    public static final String OOZIE_SHARELIB_PATH_KEY = "oozie.sharelib.path";
    public static final String OOZIE_SHARELIB_NAME_KEY = "oozie.sharelib.name";
    public static final String SHARE_LIB_LOCAL_TEMP_PREFIX = "oozie_share_lib_tmp";
    public static final String SHARE_LIB_PREFIX = "lib_";
    public static final String OOZIE_SHARE_LIB_COMPONENT_KEY = "oozie.sharelib.component";

    public static final String OOZIE_CLIENT_HOST = "oozie.client.host";

    private OozieConfig() {}
}
