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
package fr.jetoile.hadoopunit.redis;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DefaultLogger;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.ProjectHelper;
import org.codehaus.plexus.archiver.ArchiverException;
import org.codehaus.plexus.archiver.tar.TarGZipUnArchiver;
import org.codehaus.plexus.logging.console.ConsoleLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static org.apache.commons.io.FileUtils.getFile;

public class RedisInstaller {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisInstaller.class);
    private static final String REDIS_PACKAGE_PREFIX = "redis-";
    private static final List<String> REDIS_EXECUTABLE_FILE = Arrays.asList("redis-server", "redis-sentinel");

    private static final Path REDIS_INSTALLATION_PATH = Paths.get(System.getProperty("user.home") + "/.redis");

    private final String downloadUrl;
    private final String version;
    private final boolean forceCleanupInstallationDirectory;
    private final String tmpDir;

    RedisInstaller(String version, String downloadUrl, boolean forceCleanupInstallationDirectory, String tmpDir) {
        this.downloadUrl = downloadUrl;
        this.version = version;
        this.forceCleanupInstallationDirectory = forceCleanupInstallationDirectory;
        this.tmpDir = tmpDir;
        REDIS_INSTALLATION_PATH.toFile().mkdirs();
    }

    File getExecutableFile() {
        return fileRelativeToInstallationDir("src", "redis-server");
    }

    private File fileRelativeToInstallationDir(String... path) {
        return getFile(getInstallationDirectory(), path);
    }

    private File getInstallationDirectory() {
        return getFile(REDIS_INSTALLATION_PATH.toFile(), REDIS_PACKAGE_PREFIX + version);
    }

    void install() throws IOException, InterruptedException {
        if (forceCleanupInstallationDirectory) {
            FileUtils.forceDelete(getInstallationDirectory());
        }
        installRedis();
        makeRedis();
        applyRedisPermissionRights();
    }


    private void installRedis() throws IOException {
        Path downloadedTo = download(new URL(downloadUrl + REDIS_PACKAGE_PREFIX + version + ".tar.gz"));
        install(downloadedTo);
    }

    private File getAntFile() throws IOException {
        InputStream in = RedisInstaller.class.getClassLoader().getResourceAsStream("build.xml");
        File fileOut = new File(tmpDir, "build.xml");

        LOGGER.info("Writing redis' build.xml to: " + fileOut.getAbsolutePath());

        OutputStream out = FileUtils.openOutputStream(fileOut);
        IOUtils.copy(in, out);
        in.close();
        out.close();

        return fileOut;
    }

    private void makeRedis() throws IOException, InterruptedException {
        LOGGER.info("> make");
        File makeFilePath = getInstallationDirectory();

        DefaultLogger consoleLogger = getConsoleLogger();

        Project project = new Project();
        File buildFile = getAntFile();
        project.setUserProperty("ant.file", buildFile.getAbsolutePath());
        project.addBuildListener(consoleLogger);

        try {
            project.fireBuildStarted();
            project.init();
            ProjectHelper projectHelper = ProjectHelper.getProjectHelper();
            project.addReference("ant.projectHelper", projectHelper);
            project.setProperty("redisDirectory", makeFilePath.getAbsolutePath());
            projectHelper.parse(project, buildFile);
            project.executeTarget("init");
            project.fireBuildFinished(null);
        } catch (BuildException buildException) {
            project.fireBuildFinished(buildException);
            throw new RuntimeException("!!! Unable to compile redis !!!", buildException);
        }
    }

    private DefaultLogger getConsoleLogger() {
        DefaultLogger consoleLogger = new DefaultLogger();
        consoleLogger.setErrorPrintStream(System.err);
        consoleLogger.setOutputPrintStream(System.out);
        consoleLogger.setMessageOutputLevel(Project.MSG_INFO);

        return consoleLogger;
    }

    private Path download(URL source) throws IOException {
        File target = new File(REDIS_INSTALLATION_PATH.toString(), source.getPath());
        if (!target.exists()) {
            LOGGER.info("Downloading : " + source + " to " + target + "...");
            FileUtils.copyURLToFile(source, target);
            LOGGER.info("Download complete");
        } else {
            LOGGER.info("Download skipped");
        }
        return target.toPath();
    }

    private void install(Path downloadedFile) throws IOException {
        LOGGER.info("Installing Redis into " + REDIS_INSTALLATION_PATH + "...");
        try {
            final TarGZipUnArchiver ua = new TarGZipUnArchiver();
            ua.setSourceFile(downloadedFile.toFile());
            ua.enableLogging(new ConsoleLogger(1, "console"));
            ua.setDestDirectory(REDIS_INSTALLATION_PATH.toFile());
            ua.extract();
            LOGGER.info("Done");
        } catch (ArchiverException e) {
            LOGGER.info("Failure : " + e);
            throw new RuntimeException("!!! Unable to download and untar redis !!!", e);
        }
    }

    private void applyRedisPermissionRights() throws IOException {
        File binDirectory = getFile(getInstallationDirectory(), "src");
        for (String fn : REDIS_EXECUTABLE_FILE) {
            File executableFile = new File(binDirectory, fn);
            LOGGER.info("Applying executable permissions on " + executableFile);
            executableFile.setExecutable(true);
        }
    }

}

