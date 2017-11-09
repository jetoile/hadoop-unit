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
package fr.jetoile.hadoopunit;

import fr.jetoile.hadoopunit.component.Bootstrap;
import fr.jetoile.hadoopunit.exception.NotFoundServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;
import static org.fusesource.jansi.Ansi.Color.GREEN;

public enum HadoopBootstrap {
    INSTANCE;

    final private static Logger LOGGER = LoggerFactory.getLogger(HadoopBootstrap.class);

    List<Bootstrap> componentsToStart = new ArrayList<>();
    List<Bootstrap> manualComponentsToStart = new ArrayList<>();
    List<Bootstrap> componentsToStop = new ArrayList<>();
    List<Bootstrap> manualComponentsToStop = new ArrayList<>();

    private ServiceLoader<Bootstrap> commandLoader = ServiceLoader.load(Bootstrap.class);
    private Map<String, Bootstrap> commands = new HashMap<>();


    HadoopBootstrap() {
        commands.clear();
        commandLoader.reload();

        Iterable<Bootstrap> iterable = commandLoader::iterator;
        commands = StreamSupport.stream(iterable.spliterator(), false)
                .collect(Collectors.toMap(Bootstrap::getName, Function.identity()));

        componentsToStart = commands.entrySet().stream()
                .filter(entry -> Component.isComponent(entry.getKey()))
                .sorted(Comparator.comparingInt(e -> Component.getOrdinal(e.getKey())))
                .map(Map.Entry::getValue)
                .collect(toList());

        componentsToStop = this.componentsToStart.stream().collect(toList());
        Collections.reverse(componentsToStop);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                LOGGER.info("All services are going to be stopped");
                stopAll();
            }
        });
    }

    public Bootstrap getService(Component c) throws NotFoundServiceException {
        if (commands.containsKey(c.name())) {
            return commands.get(c.name());
        } else {
            throw new NotFoundServiceException("unable to find service " + c.name());
        }

    }

    public void startAll() {
        if (manualComponentsToStart.isEmpty()) {
            internalStart(componentsToStart);
        } else {
            internalStart(manualComponentsToStart);
        }
    }


    public void stopAll() {
        if (manualComponentsToStop.isEmpty()) {
            internalStop(componentsToStop);
        } else {
            manualComponentsToStop = this.manualComponentsToStart.stream().collect(toList());
            Collections.reverse(manualComponentsToStop);
            internalStop(manualComponentsToStop);
        }
    }

    public HadoopBootstrap add(Component component) throws NotFoundServiceException {
        manualComponentsToStart.add(getService(component));
        return this;
    }

    private void internalStart(List<Bootstrap> componentsToStart) {
        componentsToStart.forEach(this::startService);

        HadoopUtils.INSTANCE.printBanner(System.out);
        componentsToStart.forEach(c -> HadoopUtils.printColorLine(System.out, GREEN, "\t\t - " + c.getName() + " " + c.getProperties()));
        System.out.println();
    }

    private void internalStop(List<Bootstrap> componentsToStop) {
        componentsToStop.forEach(this::stopService);
    }

    private void startService(Bootstrap c) {
        c.start();
    }

    private void stopService(Bootstrap c) {
        c.stop();
    }

}
