/*
 * Copyright (c) 2011 Khanh Tuong Maudoux <kmx.petals@gmail.com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. The name of the author may not be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package fr.jetoile.hadoopunit;

import com.google.common.collect.Lists;
import fr.jetoile.hadoopunit.component.Bootstrap;
import fr.jetoile.hadoopunit.exception.NotFoundServiceException;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;

public enum HadoopBootstrap {
    INSTANCE;

    final private static Logger LOGGER = LoggerFactory.getLogger(HadoopBootstrap.class);


    private Configuration configuration;
    List<Bootstrap> componentsToStart = new ArrayList<>();
    List<Bootstrap> manualComponentsToStart = new ArrayList<>();
    List<Bootstrap> componentsToStop = new ArrayList<>();
    List<Bootstrap> manualComponentsToStop = new ArrayList<>();

    private ServiceLoader<Bootstrap> commandLoader = ServiceLoader.load(Bootstrap.class);
    private Map<String, Bootstrap> commands = new HashMap<>();


    HadoopBootstrap() {
        commands.clear();
        commandLoader.reload();
        Iterator<Bootstrap> commandsIterator = commandLoader.iterator();

        while (commandsIterator.hasNext()) {
            Bootstrap command = commandsIterator.next();
            commands.put(command.getName(), command);
        }

        Arrays.asList(Component.values()).stream().forEach(c -> {
            if (commands.containsKey(c.name())) {
                componentsToStart.add(commands.get(c.name()));
            }
        });

        componentsToStop = Lists.newArrayList(this.componentsToStart);
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
            internalStop(manualComponentsToStop);
        }
    }

    public HadoopBootstrap start(Component component) throws NotFoundServiceException {
        manualComponentsToStart.add(getService(component));
        return this;
    }

    public HadoopBootstrap stop(Component component) throws NotFoundServiceException {
        manualComponentsToStop.add(getService(component));
        return this;
    }

    private void internalStart(List<Bootstrap> componentsToStart) {
        componentsToStart.stream().forEach(c -> {
            startService(c);
        });
        HadoopUtils.printBanner(System.out);
        componentsToStart.stream().forEach(c -> System.out.println("\t\t - " + c.getName() + " " + c.getProperties()));
        System.out.println();
    }

    private void internalStop(List<Bootstrap> componentsToStop) {
        componentsToStop.stream().forEach(c -> {
            stopService(c);
        });
    }

    private void startService(Bootstrap c) {
        c.start();
    }

    private void stopService(Bootstrap c) {
        c.stop();
    }

}
