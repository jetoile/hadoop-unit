package fr.jetoile.hadoopunit.component;

import fr.jetoile.hadoopunit.ComponentMetadata;

import java.util.Collections;
import java.util.List;

public class TestContainerMetadata extends ComponentMetadata {
    @Override
    public String getName() {
        return "TESTCONTAINER";
    }

    @Override
    public List<String> getDependencies() {
        return Collections.emptyList();
    }
}
