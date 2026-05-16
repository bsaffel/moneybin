/* Reports schema — read-only curated presentation views.
   Owned by SQLMesh models in sqlmesh/models/reports/. One model per CLI/MCP
   reports surface (per moneybin-cli.md v2 + reports-recipe-library.md).
   Consumers read these views; never written to by services. */
CREATE SCHEMA IF NOT EXISTS reports;
