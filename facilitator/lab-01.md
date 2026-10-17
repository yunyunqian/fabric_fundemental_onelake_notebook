# Facilitator guide: Lab 1

[Workshop home](../README.md) |
[Participant guide](../labs/01-lakehouse-and-delta/README.md)

## Goal

Participants should understand the journey from a local CSV to a file in OneLake
and then to a Delta table. Keep the first lab browser-based and avoid introducing
Spark setup, pipelines, or notebook code.

## Rehearsal checklist

- [ ] Confirm participant accounts can access the assigned workspace.
- [ ] Confirm active Fabric capacity/trial and lakehouse creation permissions.
- [ ] Test the sample download on the network used for the workshop.
- [ ] Rehearse the entire participant guide in Fabric, including the SQL query.
- [ ] Check the actual table row count; the verified sample has 295 data rows.
- [ ] Confirm current portal labels for upload and **Load to tables**.
- [ ] Decide whether participants use separate workspaces or uniquely named
      lakehouses in a shared workspace.
- [ ] Agree on retention and cleanup; do not delete items needed by later labs.
- [ ] Prepare a separate demo lakehouse so participants never overwrite demo data.

## Suggested delivery

| Time | Activity | Facilitation cue |
| --- | --- | --- |
| 0-5 min | Explain lakehouse, Files, Tables, and Delta | "Uploading a file and creating a table are different operations." |
| 5-10 min | Download CSV and create lakehouse | Pause until everyone has the original CSV and a lakehouse. |
| 10-15 min | Upload to Files | Ask whether the uploaded CSV is already a Delta table. |
| 15-25 min | Load to Tables and preview | Point out that the original CSV remains in Files. |
| 25-30 min | Validate with SQL | Show the full count rather than relying on the preview. |
| 30-35 min | Recap and questions | Ask participants to explain CSV versus Delta in their own words. |

## Key talking points

- A lakehouse is a Fabric item backed by OneLake, not a file format.
- A CSV is a useful landing format, but uploading it does not give it Delta
  transaction semantics.
- Loading creates Parquet data with a Delta transaction log.
- The transaction log tracks committed table changes. Reliable transactions and
  version history are Delta capabilities; this lab does not demonstrate updates,
  rollback, or time travel.
- The SQL analytics endpoint lets participants query the same Delta table;
  synchronization can lag after table creation.
- The no-code loader infers column types. Explicit type design, including
  monetary precision for `ListPrice`, is outside this introductory lab.

## Check understanding

1. "Where is the source CSV after upload?" Expected: the lakehouse's Files area
   in OneLake.
2. "What did Load to tables create?" Expected: a separate Delta table under
   Tables, with Parquet data and a transaction log.
3. "Does appending the same file twice replace the first load?" Expected: no,
   it adds rows and causes duplicates.
4. "How did you verify all rows were loaded?" Expected: a full `COUNT(*)`
   query, not just the preview.

## Recovery during the session

- **Permissions or capacity:** Stop and resolve with the workspace owner. Do not
  ask participants to change tenant settings or start a paid capacity.
- **Download blocked:** Use an organization-approved distribution method if
  permitted, or demonstrate from your demo lakehouse. A demonstration is not
  equivalent to participant completion.
- **Wrong header or separator:** Reload once to a new table with the correct
  settings. Avoid appending as a repair.
- **Table already exists:** Use a unique retry table name and update SQL to match.
- **SQL metadata lag:** Continue the conceptual recap while synchronization
  completes, then retry the query.

## Acceptance criteria before publishing

- [ ] A first-time participant can follow the guide without undocumented steps.
- [ ] CSV download, upload, table load, and SQL count have been tested in Fabric.
- [ ] Four expected columns and the source row count are confirmed.
- [ ] Shared-workspace naming and cleanup instructions fit the customer setup.
- [ ] Materials contain no credentials, tenant identifiers, or customer data.
