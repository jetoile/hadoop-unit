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

import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.DepthFirstIterator;
import org.jgrapht.traverse.GraphIterator;

import java.util.*;
import java.util.stream.Collectors;

public class DependenciesCalculator {

    public static Map<String, List<String>> findTransitiveDependenciesByComponent(Map<String, List<String>> dependenciesMapByComponent) {
        Map<String, List<String>> result = new HashMap<>();
        dependenciesMapByComponent.entrySet().stream().forEach(entry -> {
            List<String> dependencies = new ArrayList<>();
            buildDependencies(dependencies, dependenciesMapByComponent, entry.getKey());
            result.put(entry.getKey(), dependencies);
        });
        return result;
    }

    public static List<String> dryRunToDefineCorrectOrder(Map<String, List<String>> dependenciesMapByComponent) {
        List<String> componentsStarted = new ArrayList<>();

        dependenciesMapByComponent.keySet().forEach(c -> {
            List<String> dependencies = dependenciesMapByComponent.get(c);
            List<String> componentsNotStarted = dependencies.stream().filter(d -> !componentsStarted.contains(d)).collect(Collectors.toList());
            componentsStarted.addAll(componentsNotStarted);
            if (!componentsStarted.contains(c)) {
                componentsStarted.add(c);
            }
        });
        return componentsStarted;
    }

    public static List<String> calculateParents(Graph<String, DefaultEdge> graph, String currentNode) {
        List<String> result = new ArrayList<>();
        GraphIterator<String, DefaultEdge> it = new DepthFirstIterator<>(graph, currentNode);
        while (it.hasNext()) {
            result.add(it.next());
        }
        Collections.reverse(result);
        return result.subList(0, result.size() - 1);
    }

    static void buildDependencies(List<String> accumulator, Map<String, List<String>> dependenciesMap, String currentComponent) {
        if (dependenciesMap.containsKey(currentComponent)) {
            for (String dep : dependenciesMap.get(currentComponent)) {
                if (!accumulator.contains(dep)) {
                    buildDependencies(accumulator, dependenciesMap, dep);
                    accumulator.add(dep);
                }
            }
        }
    }
}
