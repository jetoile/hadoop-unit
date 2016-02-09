package fr.jetoile.hadoop.test.hive;

import com.ninja_squad.dbsetup.DbSetupRuntimeException;
import com.ninja_squad.dbsetup.bind.BinderConfiguration;
import com.ninja_squad.dbsetup.bind.DefaultBinderConfiguration;
import com.ninja_squad.dbsetup.destination.Destination;
import com.ninja_squad.dbsetup.operation.Operation;
import com.ninja_squad.dbsetup.util.Preconditions;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.SQLException;

public class HiveSetup {
    private final Destination destination;
    private final Operation operation;
    private final BinderConfiguration binderConfiguration;

    /**
     * Constructor which uses the {@link DefaultBinderConfiguration#INSTANCE default binder configuration}.
     * @param destination the destination of the sequence of database operations
     * @param operation the operation to execute (most of the time, an instance of
     * {@link com.ninja_squad.dbsetup.operation.CompositeOperation}
     */
    public HiveSetup(@Nonnull Destination destination, @Nonnull Operation operation) {
        this(destination, operation, DefaultBinderConfiguration.INSTANCE);
    }

    /**
     * Constructor allowing to use a custom {@link BinderConfiguration}.
     * @param destination the destination of the sequence of database operations
     * @param operation the operation to execute (most of the time, an instance of
     * {@link com.ninja_squad.dbsetup.operation.CompositeOperation}
     * @param binderConfiguration the binder configuration to use.
     */
    public HiveSetup(@Nonnull Destination destination,
                   @Nonnull Operation operation,
                   @Nonnull BinderConfiguration binderConfiguration) {
        Preconditions.checkNotNull(destination, "destination may not be null");
        Preconditions.checkNotNull(operation, "operation may not be null");
        Preconditions.checkNotNull(binderConfiguration, "binderConfiguration may not be null");

        this.destination = destination;
        this.operation = operation;
        this.binderConfiguration = binderConfiguration;
    }

    /**
     * Executes the sequence of operations. All the operations use the same connection, and are grouped
     * in a single transaction. The transaction is rolled back if any exception occurs.
     */
    public void launch() {
        try {
            Connection connection = destination.getConnection();
            try {
//                connection.setAutoCommit(false);
                operation.execute(connection, binderConfiguration);
//                connection.commit();
            }
            catch (SQLException e) {
//                connection.rollback();
                throw e;
            }
            catch (RuntimeException e) {
//                connection.rollback();
                throw e;
            }
            finally {
                connection.close();
            }
        }
        catch (SQLException e) {
            throw new DbSetupRuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return "HiveSetup [destination="
                + destination
                + ", operation="
                + operation
                + ", binderConfiguration="
                + binderConfiguration
                + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + binderConfiguration.hashCode();
        result = prime * result + destination.hashCode();
        result = prime * result + operation.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof HiveSetup)) return false;

        HiveSetup hiveSetup = (HiveSetup) o;

        if (destination != null ? !destination.equals(hiveSetup.destination) : hiveSetup.destination != null)
            return false;
        if (operation != null ? !operation.equals(hiveSetup.operation) : hiveSetup.operation != null) return false;
        return binderConfiguration != null ? binderConfiguration.equals(hiveSetup.binderConfiguration) : hiveSetup.binderConfiguration == null;

    }
}
