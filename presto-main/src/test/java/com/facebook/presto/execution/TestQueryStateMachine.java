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
package com.facebook.presto.execution;

import com.facebook.airlift.testing.TestingTicker;
import com.facebook.presto.Session;
import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.memory.VersionedMemoryPoolId;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.security.AccessControlManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.resourceGroups.QueryType;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import static com.facebook.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.execution.QueryState.FINISHING;
import static com.facebook.presto.execution.QueryState.PLANNING;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.execution.QueryState.STARTING;
import static com.facebook.presto.execution.QueryState.WAITING_FOR_RESOURCES;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.USER_CANCELED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestQueryStateMachine
{
    private static final String QUERY = "sql";
    private static final URI LOCATION = URI.create("fake://fake-query");
    private static final SQLException FAILED_CAUSE = new SQLException("FAILED");
    private static final List<Input> INPUTS = ImmutableList.of(new Input(new ConnectorId("connector"), "schema", "table", Optional.empty(), ImmutableList.of(new Column("a", "varchar")), Optional.empty()));
    private static final Optional<Output> OUTPUT = Optional.empty();
    private static final List<String> OUTPUT_FIELD_NAMES = ImmutableList.of("a", "b", "c");
    private static final List<Type> OUTPUT_FIELD_TYPES = ImmutableList.of(BIGINT, BIGINT, BIGINT);
    private static final String UPDATE_TYPE = "update type";
    private static final VersionedMemoryPoolId MEMORY_POOL = new VersionedMemoryPoolId(new MemoryPoolId("pool"), 42);
    private static final Map<String, String> SET_SESSION_PROPERTIES = ImmutableMap.<String, String>builder()
            .put("fruit", "apple")
            .put("drink", "coffee")
            .build();
    private static final List<String> RESET_SESSION_PROPERTIES = ImmutableList.of("candy");
    private static final Optional<QueryType> QUERY_TYPE = Optional.of(QueryType.SELECT);

    private final ExecutorService executor = newCachedThreadPool();

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testBasicStateChanges()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();
        assertState(stateMachine, QUEUED);

        assertTrue(stateMachine.transitionToPlanning());
        assertState(stateMachine, PLANNING);

        assertTrue(stateMachine.transitionToStarting());
        assertState(stateMachine, STARTING);

        assertTrue(stateMachine.transitionToRunning());
        assertState(stateMachine, RUNNING);

        assertTrue(stateMachine.transitionToFinishing());
        tryGetFutureValue(stateMachine.getStateChange(FINISHING), 2, SECONDS);
        assertState(stateMachine, FINISHED);
    }

    @Test
    public void testStateChangesWithResourceWaiting()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();
        assertState(stateMachine, QUEUED);

        assertTrue(stateMachine.transitionToWaitingForResources());
        assertState(stateMachine, WAITING_FOR_RESOURCES);

        assertTrue(stateMachine.transitionToPlanning());
        assertState(stateMachine, PLANNING);

        assertTrue(stateMachine.transitionToStarting());
        assertState(stateMachine, STARTING);

        assertTrue(stateMachine.transitionToRunning());
        assertState(stateMachine, RUNNING);

        assertTrue(stateMachine.transitionToFinishing());
        tryGetFutureValue(stateMachine.getStateChange(FINISHING), 2, SECONDS);
        assertState(stateMachine, FINISHED);
    }

    @Test
    public void testQueued()
    {
        // all time before the first state transition is accounted to queueing
        assertAllTimeSpentInQueueing(QUEUED, queryStateMachine -> {});
        assertAllTimeSpentInQueueing(WAITING_FOR_RESOURCES, QueryStateMachine::transitionToWaitingForResources);
        assertAllTimeSpentInQueueing(PLANNING, QueryStateMachine::transitionToPlanning);
        assertAllTimeSpentInQueueing(STARTING, QueryStateMachine::transitionToStarting);
        assertAllTimeSpentInQueueing(RUNNING, QueryStateMachine::transitionToRunning);

        assertAllTimeSpentInQueueing(FINISHED, stateMachine -> {
            stateMachine.transitionToFinishing();
            tryGetFutureValue(stateMachine.getStateChange(FINISHING), 2, SECONDS);
        });

        assertAllTimeSpentInQueueing(FAILED, stateMachine -> stateMachine.transitionToFailed(FAILED_CAUSE));
    }

    private void assertAllTimeSpentInQueueing(QueryState expectedState, Consumer<QueryStateMachine> stateTransition)
    {
        TestingTicker ticker = new TestingTicker();
        QueryStateMachine stateMachine = createQueryStateMachineWithTicker(ticker);
        ticker.increment(7, MILLISECONDS);

        stateTransition.accept(stateMachine);
        assertEquals(stateMachine.getQueryState(), expectedState);

        QueryStats queryStats = stateMachine.getQueryInfo(Optional.empty()).getQueryStats();
        assertEquals(queryStats.getQueuedTime(), new Duration(7, MILLISECONDS));
        assertEquals(queryStats.getResourceWaitingTime(), new Duration(0, MILLISECONDS));
        assertEquals(queryStats.getTotalPlanningTime(), new Duration(0, MILLISECONDS));
        assertEquals(queryStats.getExecutionTime(), new Duration(0, MILLISECONDS));
        assertEquals(queryStats.getFinishingTime(), new Duration(0, MILLISECONDS));
    }

    @Test
    public void testPlanning()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();
        assertTrue(stateMachine.transitionToPlanning());
        assertState(stateMachine, PLANNING);

        assertFalse(stateMachine.transitionToPlanning());
        assertState(stateMachine, PLANNING);

        assertTrue(stateMachine.transitionToStarting());
        assertState(stateMachine, STARTING);

        stateMachine = createQueryStateMachine();
        stateMachine.transitionToPlanning();
        assertTrue(stateMachine.transitionToRunning());
        assertState(stateMachine, RUNNING);

        stateMachine = createQueryStateMachine();
        stateMachine.transitionToPlanning();
        assertTrue(stateMachine.transitionToFinishing());
        tryGetFutureValue(stateMachine.getStateChange(FINISHING), 2, SECONDS);
        assertState(stateMachine, FINISHED);

        stateMachine = createQueryStateMachine();
        stateMachine.transitionToPlanning();
        assertTrue(stateMachine.transitionToFailed(FAILED_CAUSE));
        assertState(stateMachine, FAILED, FAILED_CAUSE);
    }

    @Test
    public void testStarting()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();
        assertTrue(stateMachine.transitionToStarting());
        assertState(stateMachine, STARTING);

        assertFalse(stateMachine.transitionToPlanning());
        assertState(stateMachine, STARTING);

        assertFalse(stateMachine.transitionToStarting());
        assertState(stateMachine, STARTING);

        assertTrue(stateMachine.transitionToRunning());
        assertState(stateMachine, RUNNING);

        stateMachine = createQueryStateMachine();
        stateMachine.transitionToStarting();
        assertTrue(stateMachine.transitionToFinishing());
        tryGetFutureValue(stateMachine.getStateChange(FINISHING), 2, SECONDS);
        assertState(stateMachine, FINISHED);

        stateMachine = createQueryStateMachine();
        stateMachine.transitionToStarting();
        assertTrue(stateMachine.transitionToFailed(FAILED_CAUSE));
        assertState(stateMachine, FAILED, FAILED_CAUSE);
    }

    @Test
    public void testRunning()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();
        assertTrue(stateMachine.transitionToRunning());
        assertState(stateMachine, RUNNING);

        assertFalse(stateMachine.transitionToPlanning());
        assertState(stateMachine, RUNNING);

        assertFalse(stateMachine.transitionToStarting());
        assertState(stateMachine, RUNNING);

        assertFalse(stateMachine.transitionToRunning());
        assertState(stateMachine, RUNNING);

        assertTrue(stateMachine.transitionToFinishing());
        tryGetFutureValue(stateMachine.getStateChange(FINISHING), 2, SECONDS);
        assertState(stateMachine, FINISHED);

        stateMachine = createQueryStateMachine();
        stateMachine.transitionToRunning();
        assertTrue(stateMachine.transitionToFailed(FAILED_CAUSE));
        assertState(stateMachine, FAILED, FAILED_CAUSE);
    }

    @Test
    public void testFinished()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();
        assertTrue(stateMachine.transitionToFinishing());
        tryGetFutureValue(stateMachine.getStateChange(FINISHING), 2, SECONDS);
        assertFinalState(stateMachine, FINISHED);
    }

    @Test
    public void testFailed()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();
        assertTrue(stateMachine.transitionToFailed(FAILED_CAUSE));
        assertFinalState(stateMachine, FAILED, FAILED_CAUSE);
    }

    @Test
    public void testCanceled()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();
        assertTrue(stateMachine.transitionToCanceled());
        assertFinalState(stateMachine, FAILED, new PrestoException(USER_CANCELED, "canceled"));
    }

    @Test
    public void testPlanningTimeDuration()
    {
        TestingTicker mockTicker = new TestingTicker();
        QueryStateMachine stateMachine = createQueryStateMachineWithTicker(mockTicker);
        assertState(stateMachine, QUEUED);

        mockTicker.increment(50, MILLISECONDS);
        assertTrue(stateMachine.transitionToWaitingForResources());
        assertState(stateMachine, WAITING_FOR_RESOURCES);

        mockTicker.increment(100, MILLISECONDS);
        assertTrue(stateMachine.transitionToPlanning());
        assertState(stateMachine, PLANNING);

        mockTicker.increment(200, MILLISECONDS);
        assertTrue(stateMachine.transitionToStarting());
        assertState(stateMachine, STARTING);

        mockTicker.increment(300, MILLISECONDS);
        assertTrue(stateMachine.transitionToRunning());
        assertState(stateMachine, RUNNING);

        mockTicker.increment(400, MILLISECONDS);
        assertTrue(stateMachine.transitionToFinishing());
        tryGetFutureValue(stateMachine.getStateChange(FINISHING), 2, SECONDS);
        assertState(stateMachine, FINISHED);

        QueryStats queryStats = stateMachine.getQueryInfo(Optional.empty()).getQueryStats();
        assertEquals(queryStats.getElapsedTime().toMillis(), 1050);
        assertEquals(queryStats.getQueuedTime().toMillis(), 50);
        assertEquals(queryStats.getResourceWaitingTime().toMillis(), 100);
        assertEquals(queryStats.getTotalPlanningTime().toMillis(), 200);
        // there is no way to induce finishing time without a transaction and connector
        assertEquals(queryStats.getFinishingTime().toMillis(), 0);
        // query execution time is starts when query transitions to planning
        assertEquals(queryStats.getExecutionTime().toMillis(), 900);
    }

    @Test
    public void testUpdateMemoryUsage()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();

        stateMachine.updateMemoryUsage(5, 10, 1, 3);
        assertEquals(stateMachine.getPeakUserMemoryInBytes(), 5);
        assertEquals(stateMachine.getPeakTotalMemoryInBytes(), 10);
        assertEquals(stateMachine.getPeakTaskUserMemory(), 1);
        assertEquals(stateMachine.getPeakTaskTotalMemory(), 3);

        stateMachine.updateMemoryUsage(0, 0, 2, 2);
        assertEquals(stateMachine.getPeakUserMemoryInBytes(), 5);
        assertEquals(stateMachine.getPeakTotalMemoryInBytes(), 10);
        assertEquals(stateMachine.getPeakTaskUserMemory(), 2);
        assertEquals(stateMachine.getPeakTaskTotalMemory(), 3);

        stateMachine.updateMemoryUsage(1, 1, 1, 5);
        assertEquals(stateMachine.getPeakUserMemoryInBytes(), 6);
        assertEquals(stateMachine.getPeakTotalMemoryInBytes(), 11);
        assertEquals(stateMachine.getPeakTaskUserMemory(), 2);
        assertEquals(stateMachine.getPeakTaskTotalMemory(), 5);

        stateMachine.updateMemoryUsage(3, 3, 5, 2);
        assertEquals(stateMachine.getPeakUserMemoryInBytes(), 9);
        assertEquals(stateMachine.getPeakTotalMemoryInBytes(), 14);
        assertEquals(stateMachine.getPeakTaskUserMemory(), 5);
        assertEquals(stateMachine.getPeakTaskTotalMemory(), 5);
    }

    private static void assertFinalState(QueryStateMachine stateMachine, QueryState expectedState)
    {
        assertFinalState(stateMachine, expectedState, null);
    }

    private static void assertFinalState(QueryStateMachine stateMachine, QueryState expectedState, Exception expectedException)
    {
        assertTrue(expectedState.isDone());
        assertState(stateMachine, expectedState, expectedException);

        assertFalse(stateMachine.transitionToPlanning());
        assertState(stateMachine, expectedState, expectedException);

        assertFalse(stateMachine.transitionToStarting());
        assertState(stateMachine, expectedState, expectedException);

        assertFalse(stateMachine.transitionToRunning());
        assertState(stateMachine, expectedState, expectedException);

        assertFalse(stateMachine.transitionToFinishing());
        assertState(stateMachine, expectedState, expectedException);

        assertFalse(stateMachine.transitionToFailed(FAILED_CAUSE));
        assertState(stateMachine, expectedState, expectedException);

        // attempt to fail with another exception, which will fail
        assertFalse(stateMachine.transitionToFailed(new IOException("failure after finish")));
        assertState(stateMachine, expectedState, expectedException);
    }

    private static void assertState(QueryStateMachine stateMachine, QueryState expectedState)
    {
        assertState(stateMachine, expectedState, null);
    }

    private static void assertState(QueryStateMachine stateMachine, QueryState expectedState, Exception expectedException)
    {
        assertEquals(stateMachine.getQueryId(), TEST_SESSION.getQueryId());
        assertEqualSessionsWithoutTransactionId(stateMachine.getSession(), TEST_SESSION);
        assertSame(stateMachine.getMemoryPool(), MEMORY_POOL);
        assertEquals(stateMachine.getSetSessionProperties(), SET_SESSION_PROPERTIES);
        assertEquals(stateMachine.getResetSessionProperties(), RESET_SESSION_PROPERTIES);

        QueryInfo queryInfo = stateMachine.getQueryInfo(Optional.empty());
        assertEquals(queryInfo.getQueryId(), TEST_SESSION.getQueryId());
        assertEquals(queryInfo.getSelf(), LOCATION);
        assertFalse(queryInfo.getOutputStage().isPresent());
        assertEquals(queryInfo.getQuery(), QUERY);
        assertEquals(queryInfo.getInputs(), INPUTS);
        assertEquals(queryInfo.getOutput(), OUTPUT);
        assertEquals(queryInfo.getFieldNames(), OUTPUT_FIELD_NAMES);
        assertEquals(queryInfo.getUpdateType(), UPDATE_TYPE);
        assertEquals(queryInfo.getMemoryPool(), MEMORY_POOL.getId());
        assertEquals(queryInfo.getQueryType(), QUERY_TYPE);

        QueryStats queryStats = queryInfo.getQueryStats();
        assertNotNull(queryStats.getElapsedTime());
        assertNotNull(queryStats.getQueuedTime());
        assertNotNull(queryStats.getResourceWaitingTime());
        assertNotNull(queryStats.getExecutionTime());
        assertNotNull(queryStats.getTotalPlanningTime());
        assertNotNull(queryStats.getFinishingTime());

        assertNotNull(queryStats.getCreateTime());
        if (queryInfo.getState() == QUEUED || queryInfo.getState() == WAITING_FOR_RESOURCES) {
            assertNull(queryStats.getExecutionStartTime());
        }
        else {
            assertNotNull(queryStats.getExecutionStartTime());
        }
        if (queryInfo.getState().isDone()) {
            assertNotNull(queryStats.getEndTime());
        }
        else {
            assertNull(queryStats.getEndTime());
        }

        assertEquals(stateMachine.getQueryState(), expectedState);
        assertEquals(queryInfo.getState(), expectedState);
        assertEquals(stateMachine.isDone(), expectedState.isDone());

        if (expectedState == FAILED) {
            assertNotNull(queryInfo.getFailureInfo());
            FailureInfo failure = queryInfo.getFailureInfo().toFailureInfo();
            assertNotNull(failure);
            assertEquals(failure.getType(), expectedException.getClass().getName());
            if (expectedException instanceof PrestoException) {
                assertEquals(queryInfo.getErrorCode(), ((PrestoException) expectedException).getErrorCode());
            }
            else {
                assertEquals(queryInfo.getErrorCode(), GENERIC_INTERNAL_ERROR.toErrorCode());
            }
        }
        else {
            assertNull(queryInfo.getFailureInfo());
        }
    }

    private QueryStateMachine createQueryStateMachine()
    {
        return createQueryStateMachineWithTicker(Ticker.systemTicker());
    }

    private QueryStateMachine createQueryStateMachineWithTicker(Ticker ticker)
    {
        Metadata metadata = MetadataManager.createTestMetadataManager();
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControl accessControl = new AccessControlManager(transactionManager);
        QueryStateMachine stateMachine = QueryStateMachine.beginWithTicker(
                QUERY,
                TEST_SESSION,
                LOCATION,
                new ResourceGroupId("test"),
                QUERY_TYPE,
                false,
                transactionManager,
                accessControl,
                executor,
                ticker,
                metadata,
                WarningCollector.NOOP);
        stateMachine.setInputs(INPUTS);
        stateMachine.setOutput(OUTPUT);
        stateMachine.setColumns(OUTPUT_FIELD_NAMES, OUTPUT_FIELD_TYPES);
        stateMachine.setUpdateType(UPDATE_TYPE);
        stateMachine.setMemoryPool(MEMORY_POOL);
        for (Entry<String, String> entry : SET_SESSION_PROPERTIES.entrySet()) {
            stateMachine.addSetSessionProperties(entry.getKey(), entry.getValue());
        }
        RESET_SESSION_PROPERTIES.forEach(stateMachine::addResetSessionProperties);
        return stateMachine;
    }

    private static void assertEqualSessionsWithoutTransactionId(Session actual, Session expected)
    {
        assertEquals(actual.getQueryId(), expected.getQueryId());
        assertEquals(actual.getIdentity(), expected.getIdentity());
        assertEquals(actual.getSource(), expected.getSource());
        assertEquals(actual.getCatalog(), expected.getCatalog());
        assertEquals(actual.getSchema(), expected.getSchema());
        assertEquals(actual.getTimeZoneKey(), expected.getTimeZoneKey());
        assertEquals(actual.getLocale(), expected.getLocale());
        assertEquals(actual.getRemoteUserAddress(), expected.getRemoteUserAddress());
        assertEquals(actual.getUserAgent(), expected.getUserAgent());
        assertEquals(actual.getStartTime(), expected.getStartTime());
        assertEquals(actual.getSystemProperties(), expected.getSystemProperties());
        assertEquals(actual.getConnectorProperties(), expected.getConnectorProperties());
    }
}
