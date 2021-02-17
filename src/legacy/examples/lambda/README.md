### The SQL Statement

```
SELECT MAX(c1), MIN(c2), c3
FROM aggregate_test_100
WHERE c2 < 99
GROUP BY c3
```

### Data

```
c1: [90, 100, 91, 101, 92, 102, 93, 103]
c2: [92.1, 93.2, 95.3, 96.4, 98.5, 99.6, 100.7, 101.8]
c3: ["a", "a", "a", "b", "b", "b", "c", "c"]
```

### Expected Output

```
+---------+---------+----+
| MAX(c1) | MIN(c2) | c3 |
+---------+---------+----+
| 101     | 96.4    | b  |
| 100     | 92.1    | a  |
+---------+---------+----+
```

### Plans Generated

1. MemoryExec (Merged with FilterExec in this example)
2. FilterExec (mem_filter_lambda.rs)
3. CoalesceBatchesExec (Can be ignored)
4. HashAggregateExec (hash_agg1_lambda.rs)
5. HashAggregateExec (hash_agg2_lambda.rs)
6. ProjectionExec (projection_lambda.rs)

### Plan Details

See plan_details
