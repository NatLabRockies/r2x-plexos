-- Get scenarios for a specific model along with their Read Order attribute values
--
-- In PLEXOS: Higher Read Order = Higher priority (data read later overwrites earlier data)
-- Scenarios with Read Order 0 (or NULL) have lowest priority
--
-- This query returns scenarios with their raw Read Order values.
--
-- Parameters: model_id (integer)
SELECT
    o.name as scenario,
    CAST(ad.value AS INTEGER) AS read_order
FROM
    t_membership m
JOIN
    t_object o ON o.object_id = m.child_object_id
JOIN
    t_class c ON c.class_id = o.class_id
LEFT JOIN
    t_attribute a ON a.class_id = o.class_id AND a.name = 'Read Order'
LEFT JOIN
    t_attribute_data ad ON ad.object_id = o.object_id AND ad.attribute_id = a.attribute_id
WHERE
    m.parent_object_id = ?
    AND c.name = 'Scenario'
ORDER BY
    CAST(ad.value AS INTEGER) NULLS LAST, o.name
