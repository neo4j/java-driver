package org.neo4j.docs.driver;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.util.TestNeo4j;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.neo4j.driver.v1.Values.parameters;

public class ExamplesIT
{
    @ClassRule
    public static TestNeo4j neo4j = new TestNeo4j();

    private int readInt(final String statement, final Value parameters)
    {
        try (Session session = neo4j.driver().session())
        {
            return session.writeTransaction((tx) -> tx.run(statement, parameters).single().get(0).asInt());
        }
    }

    private int readInt(final String statement)
    {
        return readInt(statement, parameters());
    }

    private void write(final String statement, final Value parameters)
    {
        try (Session session = neo4j.driver().session())
        {
            session.readTransaction((tx) ->
            {
                tx.run(statement, parameters);
                return null;
            });
        }
    }

    private void write(String statement)
    {
        write(statement, parameters());
    }

    private void clean()
    {
        write("MATCH (a) DETACH DELETE a");
    }

    private int personCount(String name)
    {
        return readInt("MATCH (a:Person {name: $name}) RETURN count(a)", parameters("name", name));
    }

    @Before
    public void setUp()
    {
        clean();
    }

    @Test
    public void testShouldRunAutocommitTransactionExample()
    {
        // Given
        AutocommitTransactionExample example = new AutocommitTransactionExample(neo4j.uri().toString(), TestNeo4j.USER, TestNeo4j.PASSWORD);

        // When
        example.addPerson("Alice");

        // Then
        assert personCount("Alice") > 0;
    }

    @Test
    public void testShouldRunBasicAuthExample()
    {
        // Given
        BasicAuthExample example = new BasicAuthExample(neo4j.uri().toString(), TestNeo4j.USER, TestNeo4j.PASSWORD);

        // Then
        assert example.canConnect();
    }

    @Test
    public void testShouldRunConfigConnectionTimeoutExample()
    {
        // Given
        ConfigConnectionTimeoutExample example = new ConfigConnectionTimeoutExample(neo4j.uri().toString(), TestNeo4j.USER, TestNeo4j.PASSWORD);

        // Then
        assertThat(example, instanceOf(ConfigConnectionTimeoutExample.class));
    }

    @Test
    public void testShouldRunConfigMaxRetryTimeExample()
    {
        // Given
        ConfigMaxRetryTimeExample example = new ConfigMaxRetryTimeExample(neo4j.uri().toString(), TestNeo4j.USER, TestNeo4j.PASSWORD);

        // Then
        assertThat(example, instanceOf(ConfigMaxRetryTimeExample.class));
    }

    @Test
    public void testShouldRunConfigTrustExample()
    {
        // Given
        ConfigTrustExample example = new ConfigTrustExample(neo4j.uri().toString(), TestNeo4j.USER, TestNeo4j.PASSWORD);

        // Then
        assertThat(example, instanceOf(ConfigTrustExample.class));
    }

    @Test
    public void testShouldRunConfigUnencryptedExample()
    {
        // Given
        ConfigUnencryptedExample example = new ConfigUnencryptedExample(neo4j.uri().toString(), TestNeo4j.USER, TestNeo4j.PASSWORD);

        // Then
        assertThat(example, instanceOf(ConfigUnencryptedExample.class));
    }

    @Test
    public void testShouldRunCypherErrorExample()
    {
        // Given
        CypherErrorExample example = new CypherErrorExample(neo4j.uri().toString(), TestNeo4j.USER, TestNeo4j.PASSWORD);

        // When
        int employeeNumber = example.getEmployeeNumber("Alice");

        // Then
        assertThat(employeeNumber, equalTo(-1));
    }

    @Test
    public void testShouldRunDriverLifecycleExample()
    {
        // Given
        DriverLifecycleExample example = new DriverLifecycleExample(neo4j.uri().toString(), TestNeo4j.USER, TestNeo4j.PASSWORD);

        // Then
        assertThat(example, instanceOf(DriverLifecycleExample.class));
    }

    @Test
    public void testShouldRunHelloWorld()
    {
        // Given
        HelloWorld greeter = new HelloWorld(neo4j.uri().toString(), TestNeo4j.USER, TestNeo4j.PASSWORD);

        // When
        greeter.printGreeting("hello, world");

        // Then
        // TODO
    }

    @Test
    public void testShouldRunReadWriteTransactionExample()
    {
        // Given
        ReadWriteTransactionExample example = new ReadWriteTransactionExample(neo4j.uri().toString(), TestNeo4j.USER, TestNeo4j.PASSWORD);

        // When
        long nodeID = example.addPerson("Alice");

        // Then
        assertThat(nodeID, greaterThanOrEqualTo(0L));
    }

    @Test
    public void testShouldRunResultConsumeExample()
    {
        // Given
        write("CREATE (a:Person {name: 'Alice'})");
        write("CREATE (a:Person {name: 'Bob'})");
        ResultConsumeExample example = new ResultConsumeExample(neo4j.uri().toString(), TestNeo4j.USER, TestNeo4j.PASSWORD);

        // When
        List<String> names = example.getPeople();

        // Then
        assertThat(names, equalTo(asList("Alice", "Bob")));
    }

    @Test
    public void testShouldRunResultRetainExample()
    {
        // Given
        write("CREATE (a:Person {name: 'Alice'})");
        write("CREATE (a:Person {name: 'Bob'})");
        ResultRetainExample example = new ResultRetainExample(neo4j.uri().toString(), TestNeo4j.USER, TestNeo4j.PASSWORD);

        // When
        example.addEmployees("Acme");

        // Then
        int employeeCount = readInt("MATCH (emp:Person)-[WORKS_FOR]->(com:Company) WHERE com.name = 'Acme' RETURN count(emp)");
        assertThat(employeeCount, equalTo(2));
    }

    @Test
    public void testShouldRunServiceUnavailableExample() throws IOException
    {
        // Given
        ServiceUnavailableExample example = new ServiceUnavailableExample(neo4j.uri().toString(), TestNeo4j.USER, TestNeo4j.PASSWORD);

        try
        {
            // When
            neo4j.stop();

            // Then
            assertThat(example.addItem(), equalTo(false));
        }
        finally
        {
            neo4j.start();
        }
    }

    @Test
    public void testShouldRunSessionExample()
    {
        // Given
        SessionExample example = new SessionExample(neo4j.uri().toString(), TestNeo4j.USER, TestNeo4j.PASSWORD);

        // When
        example.doWork();

        // Then
        assertThat(example, instanceOf(SessionExample.class));
    }

    @Test
    public void testShouldRunTransactionFunctionExample()
    {
        // Given
        TransactionFunctionExample example = new TransactionFunctionExample(neo4j.uri().toString(), TestNeo4j.USER, TestNeo4j.PASSWORD);

        // When
        example.addPerson("Alice");

        // Then
        assert personCount("Alice") > 0;
    }

}
