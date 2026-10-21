# Facilitator guide: Lab 2

[Workshop home](../README.md) |
[Participant guide](../labs/02-medallion-transformation/README.md)

## Goal and scope

Participants build a reusable PySpark notebook that moves unchanged Bronze
order CSVs into a validated Silver Delta table. Teach the purpose of each
layer and verify the data, not just that cells show success. Gold aggregation
continues in [Lab 4](../labs/04-medallion-gold/README.md), after Lab 3's
date-dimension pipeline.

This is a notebook transformation lab, not a shortcut lab. It uses the order
dataset from the linked Microsoft Learning labs. It deliberately stops short of the reference
medallion lab's complete dimension-and-fact model.
Participants reuse their existing lakehouse, preserving its existing items and
adding separate Bronze order files and a Silver table. Keep these items and
the notebook through Labs 3 and 4.

## Rehearsal checklist

- [ ] Confirm active capacity, notebook execution, and lakehouse permissions.
- [ ] Test the orders.zip download and extraction on the workshop network.
- [ ] Reuse your demo lakehouse and preserve its existing items.
- [ ] Run every participant cell in Fabric from a fresh Spark session.
- [ ] Confirm `SourceFile` is populated and dates and decimals parse correctly.
- [ ] Confirm Silver validation passes and order-line keys are unique.
- [ ] Run all cells a second time and check counts remain unchanged.
- [ ] Confirm notebook saving and the transition to Lab 3.
- [ ] Agree which items participants should retain for later labs.

## Sample baselines

The public ZIP inspected during authoring contained:

| Output | Expected rows |
| --- | ---: |
| 2019.csv | 1,201 |
| 2020.csv | 2,733 |
| 2021.csv | 28,784 |
| Bronze total / validated Silver | 32,718 |
| Silver after the two-row MERGE exercise | 32,719 |

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
| 40-50 | Compare write strategies and run the two-row MERGE exercise. |
| 50-55 | Save the notebook and transition to Lab 3. |

## Key teaching points

- Bronze preserves evidence and supports reprocessing.
- The layers represent data readiness, not mandatory physical boundaries.
- A DataFrame is not a persisted table; `saveAsTable` writes the Delta output.
- Monetary values use decimals and preserve four source decimal places.
- Exact duplicates and conflicting business keys are different problems.
- Validation failures are intentional stop conditions, not errors to suppress.
- Overwrite is appropriate here only because each participant uses their own
  existing lakehouse, writes to separate Lab 2 output tables, and performs a
  complete snapshot rebuild.
- Compare overwrite and append with the new Silver MERGE cell. Participants
  should replace the executable overwrite example, not run both on every rerun.
- Silver MERGE retains target rows absent from the source. Reading all Bronze
  files is not incremental ingestion.
- The two-row exercise adds one row on the first run and zero on subsequent
  runs. The synthetic row will be included in Lab 4's Gold totals.
- Preserve the notebook, Bronze files, and Silver table through Labs 3 and 4;
  see the [Lab 3 guide](lab-03.md) for the pipeline and the
  [Lab 4 guide](lab-04.md) for Gold aggregation and sharing.

## Acceptance before delivery

- [ ] A new participant can complete the guide without hidden setup.
- [ ] Counts and monetary totals reconcile in Fabric.
- [ ] A second full run preserves business counts and totals.
- [ ] Failures provide useful diagnostics without silently dropping bad rows.
- [ ] No credentials, tenant identifiers, or customer data are in the materials.
