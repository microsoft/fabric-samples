-- Read-only SQL analytics endpoint checks. Allow metadata synchronization first.
SELECT COUNT(*) AS HistoryRows, COUNT(DISTINCT incidentId) AS DistinctIncidents
FROM dbo.incidenthistory;

SELECT geocodeStatus, COUNT(*) AS IncidentCount
FROM dbo.incidenthistory
GROUP BY geocodeStatus;