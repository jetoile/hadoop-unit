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
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;
import org.jgrapht.traverse.DepthFirstIterator;
import org.jgrapht.traverse.GraphIterator;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.*;
import java.util.stream.Collectors;

import static org.mockito.Mockito.when;

public class DependenciesCalculatorTest {

    @Test
    public void test() {
        Graph<String, DefaultEdge> g = new SimpleGraph<>(DefaultEdge.class);

        String v1 = "v1";
        String v2 = "v2";
        String v3 = "v3";
        String v4 = "v4";

        // add the vertices
        g.addVertex(v1);
        g.addVertex(v2);
        g.addVertex(v3);
        g.addVertex(v4);
        g.addVertex(v4);
        g.addVertex(v4);

        // add edges to create a circuit
        g.addEdge(v1, v2);
        g.addEdge(v2, v3);
        g.addEdge(v3, v4);
        g.addEdge(v4, v1);

        System.out.println(g);
    }

    @Test
    public void test2() {
        ComponentMetadata hdfsMetadata = Mockito.mock(ComponentMetadata.class);
        when(hdfsMetadata.getDependencies()).thenReturn(Collections.emptyList());
        Bootstrap hdfs = Mockito.mock(Bootstrap.class);
        when(hdfs.getMetadata()).thenReturn(hdfsMetadata);
        when(hdfs.getName()).thenReturn("HDFS");

        ComponentMetadata zkMetadata = Mockito.mock(ComponentMetadata.class);
        when(zkMetadata.getDependencies()).thenReturn(Collections.emptyList());
        Bootstrap zk = Mockito.mock(Bootstrap.class);
        when(zk.getMetadata()).thenReturn(zkMetadata);
        when(zk.getName()).thenReturn("ZOOKEEPER");

        ComponentMetadata hbaseMetadata = Mockito.mock(ComponentMetadata.class);
        when(hbaseMetadata.getDependencies()).thenReturn(Arrays.asList("HDFS", "ZOOKEEPER"));
        Bootstrap hbase = Mockito.mock(Bootstrap.class);
        when(hbase.getMetadata()).thenReturn(hbaseMetadata);
        when(hbase.getName()).thenReturn("HBASE");

        ComponentMetadata kafkaMetadata = Mockito.mock(ComponentMetadata.class);
        when(kafkaMetadata.getDependencies()).thenReturn(Arrays.asList("ZOOKEEPER", "HBASE"));
        Bootstrap kafka = Mockito.mock(Bootstrap.class);
        when(kafka.getMetadata()).thenReturn(kafkaMetadata);
        when(kafka.getName()).thenReturn("KAFKA");

        Map<String, Bootstrap> commands = new HashMap<>();
        commands.put("HDFS", hdfs);
        commands.put("ZOOKEEPER", zk);
        commands.put("HBASE", hbase);
        commands.put("KAFKA", kafka);

        Graph<String, DefaultEdge> g = new DefaultDirectedGraph<>(DefaultEdge.class);

        commands.keySet().stream().forEach(
                c -> g.addVertex(c)
        );

        commands.entrySet().stream().forEach(e ->
                {
                    String key = e.getKey();
                    e.getValue().getMetadata().getDependencies().stream().forEach(d ->
                            g.addEdge(key, d));
                }

        );

        System.out.println(g);

        GraphIterator<String, DefaultEdge> iterator =
                new DepthFirstIterator<String, DefaultEdge>(g, "HBASE");
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }

        System.out.println("========");

        GraphIterator<String, DefaultEdge> iterator1 =
                new DepthFirstIterator<String, DefaultEdge>(g, "KAFKA");
        while (iterator1.hasNext()) {
            System.out.println(iterator1.next());
        }

        System.out.println("========");

        GraphIterator<String, DefaultEdge> iterator2 =
                new DepthFirstIterator<String, DefaultEdge>(g, "HDFS");
        while (iterator2.hasNext()) {
            System.out.println(iterator2.next());
        }

        System.out.println("================");

        List<String> parents = DependenciesCalculator.calculateParents(g, "KAFKA");
        parents.stream().forEach(System.out::println);

        Graph<String, DefaultEdge> dependenciesGraph = HadoopBootstrap.INSTANCE.generateGraph(commands);

        Map<String, List<String>> dependenciesMapByComponent = commands.values().stream().collect(Collectors.toMap(Bootstrap::getName, c -> DependenciesCalculator.calculateParents(dependenciesGraph, c.getName())));


        ArrayList<String> result = new ArrayList<>();
        DependenciesCalculator.buildDependencies(result, dependenciesMapByComponent, "HBASE");
        System.out.println("================");

        Map<String, List<String>> res1 = new HashMap<>();
        dependenciesMapByComponent.entrySet().stream().forEach(entry -> {
            List<String> dependencies = new ArrayList<>();
            DependenciesCalculator.buildDependencies(dependencies, dependenciesMapByComponent, entry.getKey());
            res1.put(entry.getKey(), dependencies);
        });



        result.stream().forEach(System.out::println);

        System.out.println("================");
        System.out.println("================");

        List<String> res = DependenciesCalculator.dryRunToDefineCorrectOrder(res1);
        res.stream().forEach(System.out::println);

    }


}