package com.github.phantomthief.failover.impl;

import static com.github.phantomthief.failover.impl.PriorityGroupManagerTest.countOfEachGroup;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.github.phantomthief.failover.impl.PriorityFailoverBuilder.ResConfig;
import com.github.phantomthief.failover.impl.PriorityFailoverManager.UpdateResult;

/**
 * @author huangli
 * Created on 2020-02-02
 */
class PriorityFailoverManagerTest {

    private Object o0 = "o0";
    private Object o1 = "o1";
    private Object o2 = "o2";

    @Test
    public void testUpdate() {
        PriorityFailover<Object> failover = PriorityFailover.newBuilder()
                .addResource(o0, 100, 0, 0, 100)
                .addResource(o1, 100, 0, 0, 100)
                .build();
        PriorityFailoverManager<Object> manager = new PriorityFailoverManager<>(failover, null);

        Map<Object, ResConfig> addOrUpdate = new HashMap<>();
        addOrUpdate.put(o1, new ResConfig(
                10, 5, 1, -1/*illegal but will be ignore*/));
        addOrUpdate.put(o2, new ResConfig(10, 5, 1, 7));

        UpdateResult<Object> result = manager.update(addOrUpdate, singleton(o0));

        assertEquals(1, result.getAddedResources().size());
        assertEquals(1, result.getUpdatedResources().size());
        assertEquals(1, result.getRemovedResources().size());
        assertEquals(2, manager.getFailover().getResourcesMap().size());

        assertEquals(10, result.getAddedResources().get(o2).getMaxWeight());
        assertEquals(5, result.getAddedResources().get(o2).getMinWeight());
        assertEquals(1, result.getAddedResources().get(o2).getPriority());
        assertEquals(7, result.getAddedResources().get(o2).getInitWeight());
        assertEquals(10, result.getUpdatedResources().get(o1).getMaxWeight());
        assertEquals(5, result.getUpdatedResources().get(o1).getMinWeight());
        assertEquals(1, result.getUpdatedResources().get(o1).getPriority());
        assertEquals(10, result.getUpdatedResources().get(o1).getInitWeight());
        assertEquals(100, result.getRemovedResources().get(o0).getMaxWeight());
        assertEquals(0, result.getRemovedResources().get(o0).getMinWeight());
        assertEquals(0, result.getRemovedResources().get(o0).getPriority());
        assertEquals(100, result.getRemovedResources().get(o0).getInitWeight());

        assertEquals(10, manager.getFailover().getResourcesMap().get(o1).currentWeight);
        assertEquals(10, manager.getFailover().getResourcesMap().get(o1).maxWeight);
        assertEquals(5, manager.getFailover().getResourcesMap().get(o1).minWeight);
        assertEquals(1, manager.getFailover().getResourcesMap().get(o1).priority);

        assertEquals(7, manager.getFailover().getResourcesMap().get(o2).currentWeight);
        assertEquals(10, manager.getFailover().getResourcesMap().get(o2).maxWeight);
        assertEquals(5, manager.getFailover().getResourcesMap().get(o2).minWeight);
        assertEquals(1, manager.getFailover().getResourcesMap().get(o2).priority);
    }

    @Test
    public void testUpdateAll() {
        PriorityFailover<Object> failover = PriorityFailover.newBuilder()
                .addResource(o0, 100, 0, 0, 100)
                .addResource(o1, 100, 0, 0, 100)
                .build();
        PriorityFailoverManager<Object> manager = new PriorityFailoverManager<>(failover, null);
        Map<Object, ResConfig> addOrUpdate = new HashMap<>();
        addOrUpdate.put(o1, new ResConfig(
                10, 5, 1, -1/*illegal but will be ignore*/));
        addOrUpdate.put(o2, new ResConfig(10, 5, 1, 7));

        UpdateResult<Object> result = manager.updateAll(addOrUpdate);

        assertEquals(1, result.getAddedResources().size());
        assertEquals(1, result.getUpdatedResources().size());
        assertEquals(1, result.getRemovedResources().size());
        assertEquals(2, manager.getFailover().getResourcesMap().size());

        assertEquals(10, result.getAddedResources().get(o2).getMaxWeight());
        assertEquals(5, result.getAddedResources().get(o2).getMinWeight());
        assertEquals(1, result.getAddedResources().get(o2).getPriority());
        assertEquals(7, result.getAddedResources().get(o2).getInitWeight());
        assertEquals(10, result.getUpdatedResources().get(o1).getMaxWeight());
        assertEquals(5, result.getUpdatedResources().get(o1).getMinWeight());
        assertEquals(1, result.getUpdatedResources().get(o1).getPriority());
        assertEquals(10, result.getUpdatedResources().get(o1).getInitWeight());
        assertEquals(100, result.getRemovedResources().get(o0).getMaxWeight());
        assertEquals(0, result.getRemovedResources().get(o0).getMinWeight());
        assertEquals(0, result.getRemovedResources().get(o0).getPriority());
        assertEquals(100, result.getRemovedResources().get(o0).getInitWeight());

        assertEquals(10, manager.getFailover().getResourcesMap().get(o1).currentWeight);
        assertEquals(10, manager.getFailover().getResourcesMap().get(o1).maxWeight);
        assertEquals(5, manager.getFailover().getResourcesMap().get(o1).minWeight);
        assertEquals(1, manager.getFailover().getResourcesMap().get(o1).priority);


        assertEquals(7, manager.getFailover().getResourcesMap().get(o2).currentWeight);
        assertEquals(10, manager.getFailover().getResourcesMap().get(o2).maxWeight);
        assertEquals(5, manager.getFailover().getResourcesMap().get(o2).minWeight);
        assertEquals(1, manager.getFailover().getResourcesMap().get(o2).priority);
    }

    @Test
    public void testCurrentWeight() {
        PriorityFailover<Object> failover = PriorityFailover.newBuilder()
                .addResource(o0, 100, 0, 0, 100)
                .weightFunction(new SimpleWeightFunction<>(0.5, 1))
                .build();
        PriorityFailoverManager<Object> manager = new PriorityFailoverManager<>(failover, null);
        manager.getFailover().fail(manager.getFailover().getOneAvailable());

        manager.updateAll(singletonMap(o0, new ResConfig(100, 0, 0, 100)));
        assertEquals(50, manager.getFailover().getResourcesMap().get(o0).currentWeight);


        manager.updateAll(singletonMap(o0, new ResConfig(100, 60, 0, 100)));
        assertEquals(60, manager.getFailover().getResourcesMap().get(o0).currentWeight);

        manager.updateAll(singletonMap(o0, new ResConfig(10, 0, 0, 100)));
        assertEquals(6, manager.getFailover().getResourcesMap().get(o0).currentWeight);

        manager.getFailover().success(manager.getFailover().getOneAvailable());

        manager.updateAll(singletonMap(o0, new ResConfig(100, 0, 0, 100)));
        assertEquals(100, manager.getFailover().getResourcesMap().get(o0).currentWeight);

        manager.updateAll(singletonMap(o0, new ResConfig(10, 0, 0, 100)));
        assertEquals(10, manager.getFailover().getResourcesMap().get(o0).currentWeight);
    }

    @Test
    public void testAutoPriority() {
        PriorityFailoverManager<Object> manager = PriorityFailover.newBuilder()
                .enableAutoPriority(1)
                .addResource(o0)
                .addResource(o1)
                .buildManager();
        int[] resCountOfGroup = countOfEachGroup(2, manager.getGroupManager());
        Assertions.assertEquals(1, resCountOfGroup[0]);
        Assertions.assertEquals(1, resCountOfGroup[1]);


        manager.update(singletonMap(o2, new ResConfig()), singleton(o0));

        resCountOfGroup = countOfEachGroup(2, manager.getGroupManager());
        Assertions.assertEquals(1, resCountOfGroup[0]);
        Assertions.assertEquals(1, resCountOfGroup[1]);

        Map<Object, ResConfig> map = new HashMap<>();
        map.put(o2, new ResConfig());
        map.put(new Object(), new ResConfig());
        map.put(new Object(), new ResConfig());
        manager.updateAll(map);

        resCountOfGroup = countOfEachGroup(2, manager.getGroupManager());
        Assertions.assertEquals(1, resCountOfGroup[0]);
        Assertions.assertEquals(2, resCountOfGroup[1]);
    }
}
