# Facilitator guide: Lab 2

[Workshop home](../README.md) |
[Participant guide](../labs/02-medallion-transformation/README.md)

## Goal and scope

Participants build a reusable PySpark notebook that moves unchanged Bronze
order CSVs into a validated Silver Delta table and a daily-product Gold
aggregate. Teach the purpose of each layer and verify the data, not just that
cells show success.

This is a notebook transformation lab, not a shortcut lab. It uses the order
dataset from the linked Microsoft Learning labs. It deliberately stops short of the reference
medallion lab's complete dimension-and-fact model.
Participants reuse their existing lakehouse, preserving its existing items and
adding separate Bronze order files and Silver/Gold tables.

## Rehearsal checklist

- [ ] Confirm active capacity, notebook execution, and lakehouse permissions.
- [ ] Test the orders.zip download and extraction on the workshop network.
- [ ] Reuse your demo lakehouse and preserve its existing items.
- [ ] Run every participant cell in Fabric from a fresh Spark session.
- [ ] Confirm `SourceFile` is populated and dates and decimals parse correctly.
- [ ] Confirm Silver validation passes and order-line keys are unique.
- [ ] Confirm the Gold aggregate reconciles with Silver.
- [ ] Run all cells a second time and check counts remain unchanged.
- [ ] Confirm notebook saving, sharing permissions, and optional chart controls.
- [ ] Agree which items participants should retain for later labs.

## Sample baselines

The public ZIP inspected during authoring contained:

| Output | Expected rows |
| --- | ---: |
| 2019.csv | 1,201 |
| 2020.csv | 2,733 |
| 2021.csv | 28,784 |
| Bronze total / validated Silver | 32,718 |
| Daily-product Gold | 14,386 |

The inspected source had nine fields per record, no header, no exact duplicate
records, and no conflicting order-line keys. Recheck these expectations if
the upstream data changes. Source inspection is not a substitute for running
the notebook in Fabric before delivery.

## Suggested delivery

| Minutes | Activity |
| --- | --- |
| 0-10 | Explain layers; open the existing lakehouse and upload Bronze. |
| 10-20 | Create the notebook, attach the lakehouse, and inspect DataFrames. |
| 20-40 | Clean, validate, deduplicate, and write Silver. |
| 40-55 | Build Gold and reconcile metrics. |
| 55-65 | Query and optionally chart the Gold data. |
| 65-75 | Rerun, save, and discuss reuse and sharing. |

## Key teaching points

- Bronze preserves evidence and supports reprocessing.
- The layers represent data readiness, not mandatory physical boundaries.
- A DataFrame is not a persisted table; `saveAsTable` writes the Delta output.
- Monetary values use decimals and preserve four source decimal places.
- Exact duplicates and conflicting business keys are different problems.
- Validation failures are intentional stop conditions, not errors to suppress.
- Gold has a stated grain and reconciles to Silver despite having fewer rows.
- Overwrite is appropriate here only because each participant uses their own
  existing lakehouse, writes to separate Lab 2 output tables, and performs a
  complete snapshot rebuild.
- Compare overwrite and append with the new Silver MERGE cell. Participants
  should replace the executable overwrite example, not run both on every rerun.
- Silver MERGE retains target rows absent from the source; Gold is rebuilt
  from persisted Silver. Reading all Bronze files is not incremental ingestion.
- The notebook does not make Silver and Gold writes one atomic transaction.
- Sharing notebook code does not automatically grant access to its data.

## Acceptance before delivery

- [ ] A new participant can complete the guide without hidden setup.
- [ ] Counts and monetary totals reconcile in Fabric.
- [ ] A second full run preserves business counts and totals.
- [ ] Failures provide useful diagnostics without silently dropping bad rows.
- [ ] No credentials, tenant identifiers, or customer data are in the materials.
