// Get minimum spanning tree of a patient including the specified edges
MATCH (p:Patient {USUBJID:'01-701-1192'})
CALL apoc.path.spanningTree(p, {relationshipFilter: "WAS_TREATED|ASSESSED_ENDPOINT|<MEASURED_IN_VISIT",
minLevel: 1})
YIELD path
RETURN path

// Get minimum spanning tree of vital signs of a patients and return parameter, values and visits in table format
MATCH (p:Patient {USUBJID:'01-701-1015'})
CALL apoc.path.spanningTree(p, {relationshipFilter: "MEASURED_VITALSIGN|<MEASURED_IN_VISIT",
minLevel: 1})
YIELD path
WITH NODES(path) AS nodes
UNWIND nodes AS n
WITH n
WHERE 'VitalSign' IN LABELS(n)
RETURN n.Parameter AS Parameter, n.Value AS Value, n.VISIT AS Visit
ORDER BY n.Parameter, n.VISIT