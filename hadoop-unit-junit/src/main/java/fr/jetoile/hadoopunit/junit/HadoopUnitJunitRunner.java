package fr.jetoile.hadoopunit.junit;

import org.junit.rules.TestRule;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

import java.util.List;

public class HadoopUnitJunitRunner extends BlockJUnit4ClassRunner {

    public HadoopUnitJunitRunner(Class<?> klass) throws InitializationError {
        super(klass);
    }

    /**
     * Override @ClassRule retrieval to prevent from running them inside of a container
     */
    @Override
    protected List<TestRule> classRules() {
        return super.classRules();
    }



    @Override
    public void run(RunNotifier notifier) {
        super.run(notifier);
    }
}
