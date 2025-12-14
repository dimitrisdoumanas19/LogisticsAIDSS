// ============================================================================
// NEO4J KNOWLEDGE GRAPH - DATA IMPORT SCRIPT
// Military Logistics Decision Support System
// ============================================================================

// ============================================================================
// PART 1: CONSTRAINTS AND INDEXES
// ============================================================================

// --- Constraints for Node Labels ---

CREATE CONSTRAINT node_unique_id IF NOT EXISTS
FOR (n:Node) REQUIRE (n.node_id, n.scenario_id) IS UNIQUE;

CREATE CONSTRAINT route_unique_id IF NOT EXISTS
FOR (r:Route) REQUIRE (r.route_id, r.scenario_id) IS UNIQUE;

CREATE CONSTRAINT resource_unique_id IF NOT EXISTS
FOR (res:Resource) REQUIRE (res.resource_id, res.scenario_id) IS UNIQUE;

CREATE CONSTRAINT vehicle_unique_id IF NOT EXISTS
FOR (v:Vehicle) REQUIRE (v.vehicle_id, v.scenario_id) IS UNIQUE;

CREATE CONSTRAINT mission_unique_id IF NOT EXISTS
FOR (m:Mission) REQUIRE (m.mission_id, m.scenario_id) IS UNIQUE;

CREATE CONSTRAINT unit_unique_id IF NOT EXISTS
FOR (u:Unit) REQUIRE (u.unit_id, u.scenario_id) IS UNIQUE;

CREATE CONSTRAINT disruption_unique_id IF NOT EXISTS
FOR (d:Disruption) REQUIRE (d.disruption_id, d.scenario_id) IS UNIQUE;

CREATE CONSTRAINT event_unique_id IF NOT EXISTS
FOR (e:Event) REQUIRE (e.event_id, e.scenario_id) IS UNIQUE;

// --- Indexes for Performance ---

CREATE INDEX node_type_idx IF NOT EXISTS FOR (n:Node) ON (n.node_type);
CREATE INDEX node_status_idx IF NOT EXISTS FOR (n:Node) ON (n.operational_status);
CREATE INDEX route_status_idx IF NOT EXISTS FOR (r:Route) ON (r.route_status);
CREATE INDEX resource_type_idx IF NOT EXISTS FOR (res:Resource) ON (res.resource_type);
CREATE INDEX resource_criticality_idx IF NOT EXISTS FOR (res:Resource) ON (res.criticality_level);
CREATE INDEX vehicle_status_idx IF NOT EXISTS FOR (v:Vehicle) ON (v.operational_status);
CREATE INDEX mission_status_idx IF NOT EXISTS FOR (m:Mission) ON (m.status);
CREATE INDEX mission_priority_idx IF NOT EXISTS FOR (m:Mission) ON (m.priority);
CREATE INDEX disruption_severity_idx IF NOT EXISTS FOR (d:Disruption) ON (d.severity);
CREATE INDEX scenario_idx IF NOT EXISTS FOR (n:Node) ON (n.scenario_id);

// ============================================================================
// PART 2: IMPORT NODES
// ============================================================================

// --- Import Locations/Nodes ---

LOAD CSV WITH HEADERS FROM 'file:///locations.csv' AS row
MERGE (n:Node {node_id: row.node_id, scenario_id: 'S1'})
SET n.node_name = row.node_name,
    n.node_type = row.node_type,
    n.latitude = toFloat(row.latitude),
    n.longitude = toFloat(row.longitude),
    n.capacity_fuel = toInteger(row.capacity_fuel),
    n.capacity_ammo = toInteger(row.capacity_ammo),
    n.capacity_food = toInteger(row.capacity_food),
    n.capacity_water = toInteger(row.capacity_water),
    n.capacity_medical = toInteger(row.capacity_medical),
    n.operational_status = row.operational_status,
    n.protection_level = row.protection_level,
    n.personnel_count = toInteger(row.personnel_count);

// --- Import Routes ---

LOAD CSV WITH HEADERS FROM 'file:///routes.csv' AS row
MERGE (r:Route {route_id: row.route_id, scenario_id: 'S1'})
SET r.route_name = row.route_name,
    r.origin_node = row.origin_node,
    r.destination_node = row.destination_node,
    r.distance_km = toFloat(row.distance_km),
    r.normal_travel_time_hours = toFloat(row.normal_travel_time_hours),
    r.current_travel_time_hours = toFloat(row.current_travel_time_hours),
    r.route_status = row.route_status,
    r.road_quality = row.road_quality,
    r.threat_level = row.threat_level,
    r.max_vehicle_weight_tons = toFloat(row.max_vehicle_weight_tons),
    r.weather_impact = row.weather_impact;

// --- Import Resources ---

LOAD CSV WITH HEADERS FROM 'file:///resources.csv' AS row
MERGE (res:Resource {resource_id: row.resource_id, scenario_id: 'S1'})
SET res.resource_type = row.resource_type,
    res.node_id = row.node_id,
    res.current_quantity = toInteger(row.current_quantity),
    res.unit_measure = row.unit_measure,
    res.minimum_required = toInteger(row.minimum_required),
    res.consumption_rate_per_day = toInteger(row.consumption_rate_per_day),
    res.criticality_level = row.criticality_level,
    res.expiration_days = toInteger(row.expiration_days),
    res.resupply_priority = toInteger(row.resupply_priority);

// --- Import Vehicles ---

LOAD CSV WITH HEADERS FROM 'file:///vehicles.csv' AS row
MERGE (v:Vehicle {vehicle_id: row.vehicle_id, scenario_id: 'S1'})
SET v.vehicle_type = row.vehicle_type,
    v.current_location = row.current_location,
    v.capacity_kg = toInteger(row.capacity_kg),
    v.fuel_consumption_rate = toInteger(row.fuel_consumption_rate),
    v.operational_status = row.operational_status,
    v.maintenance_required = row.maintenance_required,
    v.crew_size = toInteger(row.crew_size),
    v.armor_level = row.armor_level,
    v.max_speed_kmh = toInteger(row.max_speed_kmh),
    v.assigned_mission = row.assigned_mission;

// --- Import Missions ---

LOAD CSV WITH HEADERS FROM 'file:///missions.csv' AS row
MERGE (m:Mission {mission_id: row.mission_id, scenario_id: 'S1'})
SET m.mission_type = row.mission_type,
    m.priority = row.priority,
    m.origin_node = row.origin_node,
    m.destination_node = row.destination_node,
    m.required_resources = row.required_resources,
    m.assigned_vehicles = row.assigned_vehicles,
    m.planned_route = row.planned_route,
    m.status = row.status,
    m.start_time = datetime(row.start_time),
    m.estimated_completion_time = datetime(row.estimated_completion_time),
    m.risk_level = row.risk_level,
    m.success_probability = toFloat(row.success_probability);

// --- Import Units ---

LOAD CSV WITH HEADERS FROM 'file:///units.csv' AS row
MERGE (u:Unit {unit_id: row.unit_id, scenario_id: 'S1'})
SET u.unit_name = row.unit_name,
    u.unit_type = row.unit_type,
    u.parent_unit = row.parent_unit,
    u.location_node = row.location_node,
    u.personnel_strength = toInteger(row.personnel_strength),
    u.combat_readiness = row.combat_readiness,
    u.equipment_status = row.equipment_status,
    u.fuel_dependency = row.fuel_dependency,
    u.ammo_dependency = row.ammo_dependency,
    u.command_level = row.command_level;

// --- Import Disruptions ---

LOAD CSV WITH HEADERS FROM 'file:///disruptions.csv' AS row
MERGE (d:Disruption {disruption_id: row.disruption_id, scenario_id: 'S1'})
SET d.disruption_type = row.disruption_type,
    d.affected_entity_type = row.affected_entity_type,
    d.affected_entity_id = row.affected_entity_id,
    d.severity = row.severity,
    d.time_occurred = datetime(row.time_occurred),
    d.estimated_repair_time_hours = toInteger(row.estimated_repair_time_hours),
    d.cause = row.cause,
    d.cascading_effects = row.cascading_effects,
    d.mitigation_status = row.mitigation_status;

// --- Import Events ---

LOAD CSV WITH HEADERS FROM 'file:///events.csv' AS row
MERGE (e:Event {event_id: row.event_id, scenario_id: 'S1'})
SET e.event_type = row.event_type,
    e.event_time = datetime(row.event_time),
    e.affected_entity_type = row.affected_entity_type,
    e.affected_entity_id = row.affected_entity_id,
    e.event_description = row.event_description,
    e.impact_severity = row.impact_severity,
    e.response_action = row.response_action,
    e.resolution_time = CASE 
        WHEN row.resolution_time IS NOT NULL AND row.resolution_time <> '' 
        THEN datetime(row.resolution_time) 
        ELSE null 
    END;

// ============================================================================
// PART 3: CREATE RELATIONSHIPS
// ============================================================================

// --- Create CONNECTS_TO relationships from routes ---
// Also create HAS_ROUTE for bidirectional efficient pathfinding

LOAD CSV WITH HEADERS FROM 'file:///routes.csv' AS row
MATCH (origin:Node {node_id: row.origin_node, scenario_id: 'S1'})
MATCH (dest:Node {node_id: row.destination_node, scenario_id: 'S1'})
MATCH (route:Route {route_id: row.route_id, scenario_id: 'S1'})
MERGE (origin)-[c:CONNECTS_TO {route_id: row.route_id, scenario_id: 'S1'}]->(dest)
SET c.distance_km = toFloat(row.distance_km),
    c.travel_time_hours = toFloat(row.current_travel_time_hours),
    c.route_status = row.route_status,
    c.road_quality = row.road_quality,
    c.threat_level = row.threat_level,
    c.weather_impact = row.weather_impact,
    c.max_vehicle_weight_tons = toFloat(row.max_vehicle_weight_tons)
MERGE (origin)-[:HAS_ROUTE {direction: 'origin', scenario_id: 'S1'}]->(route)
MERGE (dest)-[:HAS_ROUTE {direction: 'destination', scenario_id: 'S1'}]->(route);

// --- Create STORES relationships from resources ---

LOAD CSV WITH HEADERS FROM 'file:///resources.csv' AS row
MATCH (n:Node {node_id: row.node_id, scenario_id: 'S1'})
MATCH (res:Resource {resource_id: row.resource_id, scenario_id: 'S1'})
MERGE (n)-[s:STORES {scenario_id: 'S1'}]->(res)
SET s.quantity = toInteger(row.current_quantity),
    s.minimum_required = toInteger(row.minimum_required),
    s.criticality_level = row.criticality_level,
    s.consumption_rate_per_day = toInteger(row.consumption_rate_per_day),
    s.unit_measure = row.unit_measure;

// --- Create LOCATED_AT relationships from vehicles ---

LOAD CSV WITH HEADERS FROM 'file:///vehicles.csv' AS row
MATCH (v:Vehicle {vehicle_id: row.vehicle_id, scenario_id: 'S1'})
MATCH (n:Node {node_id: row.current_location, scenario_id: 'S1'})
MERGE (v)-[l:LOCATED_AT {scenario_id: 'S1'}]->(n);

// --- Create ASSIGNED_TO relationships from missions (parse assigned_vehicles "V1|V7") ---

LOAD CSV WITH HEADERS FROM 'file:///missions.csv' AS row
MATCH (m:Mission {mission_id: row.mission_id, scenario_id: 'S1'})
WITH m, row, split(row.assigned_vehicles, '|') AS vehicle_list
UNWIND vehicle_list AS vehicle_id_str
WITH m, trim(vehicle_id_str) AS vehicle_id
WHERE vehicle_id <> '' AND vehicle_id <> 'NONE'
MATCH (v:Vehicle {vehicle_id: vehicle_id, scenario_id: 'S1'})
MERGE (v)-[a:ASSIGNED_TO {scenario_id: 'S1'}]->(m)
SET a.priority = m.priority;

// --- Create ORIGINATES_FROM relationships ---

LOAD CSV WITH HEADERS FROM 'file:///missions.csv' AS row
MATCH (m:Mission {mission_id: row.mission_id, scenario_id: 'S1'})
MATCH (origin:Node {node_id: row.origin_node, scenario_id: 'S1'})
MERGE (m)-[o:ORIGINATES_FROM {scenario_id: 'S1'}]->(origin)
SET o.departure_time = datetime(row.start_time);

// --- Create DELIVERS_TO relationships ---

LOAD CSV WITH HEADERS FROM 'file:///missions.csv' AS row
MATCH (m:Mission {mission_id: row.mission_id, scenario_id: 'S1'})
MATCH (dest:Node {node_id: row.destination_node, scenario_id: 'S1'})
MERGE (m)-[d:DELIVERS_TO {scenario_id: 'S1'}]->(dest)
SET d.arrival_time = datetime(row.estimated_completion_time);

// --- Create USES_ROUTE relationships (parse planned_route "R2-R3") ---

LOAD CSV WITH HEADERS FROM 'file:///missions.csv' AS row
MATCH (m:Mission {mission_id: row.mission_id, scenario_id: 'S1'})
WITH m, row, split(row.planned_route, '-') AS route_list
WHERE row.planned_route <> 'NONE' AND row.planned_route IS NOT NULL AND row.planned_route <> ''
UNWIND range(0, size(route_list)-1) AS idx
WITH m, trim(route_list[idx]) AS route_id, idx+1 AS sequence
MATCH (r:Route {route_id: route_id, scenario_id: 'S1'})
MERGE (m)-[u:USES_ROUTE {scenario_id: 'S1', route_id: route_id}]->(r)
SET u.sequence = sequence,
    u.planned_time = r.current_travel_time_hours;

// --- Create REQUIRES relationships (parse required_resources "FUEL:2000|AMMUNITION:1500") ---

LOAD CSV WITH HEADERS FROM 'file:///missions.csv' AS row
MATCH (m:Mission {mission_id: row.mission_id, scenario_id: 'S1'})
WITH m, row, split(row.required_resources, '|') AS resource_pairs
WHERE row.required_resources <> 'NONE' AND row.required_resources IS NOT NULL
UNWIND resource_pairs AS pair
WITH m, split(pair, ':') AS parts
WHERE size(parts) = 2
WITH m, trim(parts[0]) AS resource_type, toInteger(trim(parts[1])) AS quantity
// Match to actual resource instances at origin node
MATCH (m)-[:ORIGINATES_FROM]->(origin:Node {scenario_id: 'S1'})
MATCH (origin)-[:STORES]->(res:Resource {resource_type: resource_type, scenario_id: 'S1'})
MERGE (m)-[req:REQUIRES {scenario_id: 'S1'}]->(res)
SET req.resource_type = resource_type,
    req.quantity = quantity,
    req.priority = m.priority;

// --- Create AFFECTS relationships from disruptions to Nodes ---

LOAD CSV WITH HEADERS FROM 'file:///disruptions.csv' AS row
WHERE row.affected_entity_type = 'NODE'
MATCH (d:Disruption {disruption_id: row.disruption_id, scenario_id: 'S1'})
MATCH (n:Node {node_id: row.affected_entity_id, scenario_id: 'S1'})
MERGE (d)-[a:AFFECTS {scenario_id: 'S1'}]->(n)
SET a.severity = row.severity,
    a.impact = 'DESTRUCTION';

// --- Create AFFECTS relationships from disruptions to Routes ---

LOAD CSV WITH HEADERS FROM 'file:///disruptions.csv' AS row
WHERE row.affected_entity_type = 'ROUTE'
MATCH (d:Disruption {disruption_id: row.disruption_id, scenario_id: 'S1'})
MATCH (r:Route {route_id: row.affected_entity_id, scenario_id: 'S1'})
MERGE (d)-[a:AFFECTS {scenario_id: 'S1'}]->(r)
SET a.severity = row.severity,
    a.impact_duration = toInteger(row.estimated_repair_time_hours);

// --- Create CASCADES_TO relationships (parse cascading_effects) ---

LOAD CSV WITH HEADERS FROM 'file:///disruptions.csv' AS row
WHERE row.cascading_effects IS NOT NULL AND row.cascading_effects <> ''
MATCH (d1:Disruption {disruption_id: row.disruption_id, scenario_id: 'S1'})
WITH d1, split(row.cascading_effects, '|') AS effect_tokens
UNWIND effect_tokens AS token
WITH d1, trim(token) AS effect_token
// Try to match if effect_token is a disruption_id
OPTIONAL MATCH (d2:Disruption {disruption_id: effect_token, scenario_id: 'S1'})
WITH d1, d2, effect_token
WHERE d2 IS NOT NULL
MERGE (d1)-[c:CASCADES_TO {scenario_id: 'S1'}]->(d2)
SET c.cascade_type = effect_token;

// --- Create AFFECTS relationships from events to Nodes ---

LOAD CSV WITH HEADERS FROM 'file:///events.csv' AS row
WHERE row.affected_entity_type = 'NODE'
MATCH (e:Event {event_id: row.event_id, scenario_id: 'S1'})
MATCH (n:Node {node_id: row.affected_entity_id, scenario_id: 'S1'})
MERGE (e)-[a:AFFECTS {scenario_id: 'S1'}]->(n)
SET a.severity = row.impact_severity;

// --- Create AFFECTS relationships from events to Routes ---

LOAD CSV WITH HEADERS FROM 'file:///events.csv' AS row
WHERE row.affected_entity_type = 'ROUTE'
MATCH (e:Event {event_id: row.event_id, scenario_id: 'S1'})
MATCH (r:Route {route_id: row.affected_entity_id, scenario_id: 'S1'})
MERGE (e)-[a:AFFECTS {scenario_id: 'S1'}]->(r)
SET a.severity = row.impact_severity;

// --- Create AFFECTS relationships from events to Missions ---

LOAD CSV WITH HEADERS FROM 'file:///events.csv' AS row
WHERE row.affected_entity_type = 'MISSION'
MATCH (e:Event {event_id: row.event_id, scenario_id: 'S1'})
MATCH (m:Mission {mission_id: row.affected_entity_id, scenario_id: 'S1'})
MERGE (e)-[a:AFFECTS {scenario_id: 'S1'}]->(m)
SET a.severity = row.impact_severity;

// --- Create AFFECTS relationships from events to Vehicles ---

LOAD CSV WITH HEADERS FROM 'file:///events.csv' AS row
WHERE row.affected_entity_type = 'VEHICLE'
MATCH (e:Event {event_id: row.event_id, scenario_id: 'S1'})
MATCH (v:Vehicle {vehicle_id: row.affected_entity_id, scenario_id: 'S1'})
MERGE (e)-[a:AFFECTS {scenario_id: 'S1'}]->(v)
SET a.severity = row.impact_severity;

// --- Create LOCATED_AT relationships from units ---

LOAD CSV WITH HEADERS FROM 'file:///units.csv' AS row
MATCH (u:Unit {unit_id: row.unit_id, scenario_id: 'S1'})
MATCH (n:Node {node_id: row.location_node, scenario_id: 'S1'})
MERGE (u)-[l:LOCATED_AT {scenario_id: 'S1'}]->(n)
SET l.assignment_type = 'OPERATIONAL';

// --- Create SUPPLIES relationships ---

LOAD CSV WITH HEADERS FROM 'file:///relationships.csv' AS row
WHERE row.relationship_type = 'SUPPLIES'
MATCH (source:Node {node_id: row.source_entity_id, scenario_id: 'S1'})
MATCH (target:Node {node_id: row.target_entity_id, scenario_id: 'S1'})
MERGE (source)-[s:SUPPLIES {scenario_id: 'S1'}]->(target)
WITH s, row, split(row.relationship_properties, '|') AS prop_pairs
WITH s, [pair IN prop_pairs | split(pair, ':')] AS parsed_pairs
WITH s, 
     [p IN parsed_pairs WHERE size(p) = 2 AND trim(p[0]) = 'resource_flow' | trim(p[1])][0] AS resource_flow,
     [p IN parsed_pairs WHERE size(p) = 2 AND trim(p[0]) = 'frequency' | trim(p[1])][0] AS frequency
SET s.resource_flow = resource_flow,
    s.frequency = frequency;

// --- Create DEPENDS_ON relationships (Node -> Route) ---

LOAD CSV WITH HEADERS FROM 'file:///relationships.csv' AS row
WHERE row.relationship_type = 'DEPENDS_ON' AND row.source_entity_type = 'NODE' AND row.target_entity_type = 'ROUTE'
MATCH (source:Node {node_id: row.source_entity_id, scenario_id: 'S1'})
MATCH (target:Route {route_id: row.target_entity_id, scenario_id: 'S1'})
MERGE (source)-[d:DEPENDS_ON {scenario_id: 'S1'}]->(target)
WITH d, row, split(row.relationship_properties, '|') AS prop_pairs
WITH d, [pair IN prop_pairs | split(pair, ':')] AS parsed_pairs
WITH d,
     [p IN parsed_pairs WHERE size(p) = 2 AND trim(p[0]) = 'dependency_level' | trim(p[1])][0] AS dependency_level
SET d.dependency_level = dependency_level;

// --- Create ALTERNATIVE_TO relationships (Route -> Route) ---

LOAD CSV WITH HEADERS FROM 'file:///relationships.csv' AS row
WHERE row.relationship_type = 'ALTERNATIVE_TO'
MATCH (source:Route {route_id: row.source_entity_id, scenario_id: 'S1'})
MATCH (target:Route {route_id: row.target_entity_id, scenario_id: 'S1'})
MERGE (source)-[alt:ALTERNATIVE_TO {scenario_id: 'S1'}]->(target)
WITH alt, row, split(row.relationship_properties, '|') AS prop_pairs
WITH alt, [pair IN prop_pairs | split(pair, ':')] AS parsed_pairs
WITH alt,
     [p IN parsed_pairs WHERE size(p) = 2 AND trim(p[0]) = 'cost_difference' | trim(p[1])][0] AS cost_difference,
     [p IN parsed_pairs WHERE size(p) = 2 AND trim(p[0]) = 'risk_difference' | trim(p[1])][0] AS risk_difference
SET alt.cost_difference = cost_difference,
    alt.risk_difference = risk_difference;

// --- Create REQUIRES relationships (Unit -> Resource) ---

LOAD CSV WITH HEADERS FROM 'file:///relationships.csv' AS row
WHERE row.relationship_type = 'REQUIRES' AND row.source_entity_type = 'UNIT'
MATCH (source:Unit {unit_id: row.source_entity_id, scenario_id: 'S1'})
MATCH (target:Resource {resource_id: row.target_entity_id, scenario_id: 'S1'})
MERGE (source)-[req:REQUIRES {scenario_id: 'S1'}]->(target)
WITH req, row, split(row.relationship_properties, '|') AS prop_pairs
WITH req, [pair IN prop_pairs | split(pair, ':')] AS parsed_pairs
WITH req,
     [p IN parsed_pairs WHERE size(p) = 2 AND trim(p[0]) = 'resource_type' | trim(p[1])][0] AS resource_type,
     [p IN parsed_pairs WHERE size(p) = 2 AND trim(p[0]) = 'urgency' | trim(p[1])][0] AS urgency
SET req.resource_type = resource_type,
    req.urgency = urgency;

// ============================================================================
// IMPORT COMPLETE
// ============================================================================

/*
USAGE INSTRUCTIONS:
-------------------
1. Rename your CSV files to:
   - locations.csv
   - routes.csv
   - resources.csv
   - vehicles.csv
   - missions.csv
   - units.csv
   - disruptions.csv
   - events.csv
   - relationships.csv

2. Place files in Neo4j import directory (typically /var/lib/neo4j/import/)

3. Run this script in Neo4j Browser or cypher-shell

4. Verify import:
   MATCH (n {scenario_id: 'S1'}) RETURN labels(n)[0] AS type, count(n) AS count;

5. To import a different scenario:
   - Replace CSV files with new scenario data
   - Change 'S1' to 'S2' (or desired ID) throughout this script
   - Run the script again

NOTE: All entities use scenario_id='S1' by default. Change this value
      when importing different scenarios to keep data separated.
*/
