# Lab 2: Build a medallion transformation notebook

[Workshop home](../../README.md)

**Estimated time:** 60-75 minutes  
**Experience required:** Basic lakehouse concepts and willingness to run introductory Python.

## What you will learn

- Organize data into Bronze, Silver, and Gold layers.
- Create a Fabric notebook and attach a default lakehouse.
- Read CSV files into a Spark DataFrame with an explicit schema.
- Clean and validate data before saving a Silver Delta table.
- Aggregate Silver data into a Gold table for analysis.
- Compare overwrite, append, and merge, and rerun transformations without duplicate records.

This workshop combines the notebook and DataFrame concepts from
[Analyze data with Apache Spark in Fabric](https://microsoftlearning.github.io/mslearn-fabric/Instructions/Labs/02-analyze-spark.html)
with the layered approach in
[Create a medallion architecture in a Microsoft Fabric lakehouse](https://microsoftlearning.github.io/mslearn-fabric/Instructions/Labs/03b-medallion-lakehouse.html).
The instructions and notebook cells below form a smaller, workshop-specific
workflow: one notebook and one Gold aggregate, rather than the reference lab's
full dimension-and-fact model.

## 1. Understand the medallion architecture

| Layer | Purpose | This lab's implementation |
| --- | --- | --- |
| Bronze | Preserve source data so it can be inspected and reprocessed. | Three unchanged CSV files under `Files/bronze/orders`. |
| Silver | Provide clean, typed, validated detail records. | Delta table `dbo.sales_silver`, one row per order line. |
| Gold | Shape trusted data for a business question. | Delta table `dbo.daily_product_sales_gold`, one row per date and product. |

```text
Files/bronze/orders/*.csv
          |
          | Read, clean, validate, deduplicate
          v
Tables/dbo/sales_silver
          |
          | Group by order date and product
          v
Tables/dbo/daily_product_sales_gold
```

Bronze, Silver, and Gold describe increasing data quality and business readiness,
not three mandatory file formats. This lab uses raw files for Bronze and Delta
tables for Silver and Gold. A single lakehouse keeps the exercise manageable;
larger solutions can separate layers into different lakehouses or workspaces.

## 2. Prepare your workspace and source files

Use your assigned workspace on active Fabric capacity or trial. You need
permission to write to your lakehouse, create a notebook, and run Spark.

1. Open and **reuse your existing lakehouse**. Do not create a new
   lakehouse for this lab. Keep its existing files, tables, and schema settings.
2. Download [orders.zip](https://github.com/MicrosoftLearning/dp-data/raw/main/orders.zip)
   and extract it locally. Expect `2019.csv`, `2020.csv`, and `2021.csv`.

   ![File Explorer showing the extracted 2019, 2020, and 2021 CSV files alongside the orders ZIP archive](../../screenshot/extract_locally.png)

3. Under **Files**, create a `bronze` folder, then an `orders` subfolder.

   ![Files menu showing the New subfolder option](../../screenshot/create_subfolder.png)

   ![New subfolder dialog with bronze entered as the folder name](../../screenshot/create_bronze.png)

   ![Bronze folder context menu showing the New subfolder option](../../screenshot/another_subfolder.png)

   ![New subfolder dialog with orders entered as the folder name](../../screenshot/create_orders.png)

4. Upload the three CSV files into **Files > bronze > orders**.
   Upload the extracted CSVs, not the ZIP file.

   ![Upload files pane targeting Files/bronze/orders with the 2019, 2020, and 2021 CSV files selected](../../screenshot/upload_orders.png)

5. Confirm all three filenames before continuing.

   ![Lakehouse Files/bronze/orders folder containing 2019.csv, 2020.csv, and 2021.csv](../../screenshot/confirm_csv.png)

These files contain sales orders. They have **no
header row**; the first line is a record. Product names can contain quoted
commas, so do not edit or resave the files in a spreadsheet.

**Checkpoint:** Your reused lakehouse contains exactly the three order CSV files
under `Files/bronze/orders`.

## 3. Create and configure the notebook

1. Return to your assigned **workspace** in the Fabric portal.

   ![Fabric workspace showing Workspaces navigation and the New item button highlighted](../../screenshot/return_workspace.png)

2. Select **New item**, search for **Notebook**, and select it to create a
   blank notebook.

   ![New item dialog with notebook entered in the search box and the Notebook item highlighted](../../screenshot/create_new_notebook.png)

3. In the **New Notebook** dialog, enter the following name, confirm the
   workspace location, and select **Create**. In a shared workspace, append
   your name to keep it unique.

   ```text
   lab02_medallion_transform
   ```

   ![New Notebook dialog with lab02_medallion_transform entered as the name](../../screenshot/rename_nnotebook.png)

4. In the notebook explorer's **Data items** tab, select **Add data items** >
   **From OneLake catalog**, then select and add the existing **Sales_Lakehouse**
   you created earlier, containing `Files/bronze/orders`. If you used a
   different lakehouse name, select that lakehouse instead.

   ![Notebook explorer showing Add data items and the From OneLake catalog option](../../screenshot/add_lakehouse.png)

   ![OneLake catalog with the existing Sales_Lakehouse selected](../../screenshot/select_existing_sales_lakehouse.png)

5. Confirm that lakehouse is set as the **default lakehouse**. If necessary,
   open its **...** menu and select **Set as default lakehouse**.
   If you change the default after starting Spark, restart the session.

   ![Notebook explorer showing Sales_Lakehouse attached with the bronze/orders folder and all three CSV files](../../screenshot/confirm_lakehouse.png)

6. Keep the default code language as **PySpark (Python)**.

   ![Notebook language menu with PySpark (Python) selected](../../screenshot/select_pyspark.png)

7. Select **Connect > New standard session** and wait for Spark to connect.
   Starting the session may take a few minutes.

   ![Notebook Connect menu showing New standard session and New high concurrency session](../../screenshot/connect_standard_session.png)

   **What is a high concurrency session?** It allows compatible notebooks or
   pipeline workloads belonging to the same user to share a running Spark
   session. Each workload has isolated execution state, so notebook variables
   do not overwrite one another. Sharing can reduce startup time for subsequent
   workloads and improve compute utilization. The workloads must use the same
   default lakehouse configuration and Spark compute settings; it is not a
   shared session for different workshop participants.

   Use a **standard session** for this lab: you are running one notebook and
   do not need session sharing. See
   [High concurrency mode in Fabric](https://learn.microsoft.com/en-us/fabric/data-engineering/high-concurrency-overview)
   for details.

8. Add a **Markdown** cell and copy the following content into it, then render
   the cell to display the workflow:

   ````markdown
   # Medallion transformation workflow

   Transform raw Bronze order files into a validated Silver Delta table,
   then aggregate daily product sales into a business-ready Gold Delta table.

   ```text
   Files/bronze/orders/*.csv
             |
             | Read, clean, validate, deduplicate
             v
   Tables/dbo/sales_silver
             |
             | Group by order date and product
             v
   Tables/dbo/daily_product_sales_gold
   ```
   ````

   ![Notebook cell containing the medallion workflow text with the Convert to Markdown button highlighted](../../screenshot/create_markdown.png)

   The rendered Markdown cell should look like this:

   ![Rendered Markdown cell showing the medallion transformation heading, description, and Bronze to Silver to Gold workflow diagram](../../screenshot/rendered_markdown.png)

9. Hover just below the bottom of the Markdown cell to reveal **+ Code** and
   **+ Markdown**. Select **+ Code** to insert a code cell. Add a separate code
   cell for each Python block below and run cells in order.

   ![Cell insertion controls showing + Code and + Markdown below the Markdown cell](../../screenshot/add_code_cell.png)

This notebook keeps configuration, transformations,
checks, and outputs together. Save it so you can rerun or share the workflow
with authorized colleagues.

### Parameter cell

1. Add a separate **PySpark (Python)** code cell at the top of the notebook,
   before the configuration and transformation cells.
2. Copy the following default parameter into it:

   ```python
   # Enable optional Silver previews when troubleshooting; keep False in production.
   debug = False
   ```

3. Open this cell's **...** menu and select **Toggle parameter cell**.
   Fabric marks it as the notebook's parameter cell.

   ![Notebook cell menu with the ellipsis and Toggle parameter cell option highlighted](../../screenshot/toggle_parameter_cell.png)

   Confirm that **Parameters** appears beside **PySpark (Python)** at the
   bottom of the cell:

   ![Notebook cell footer showing PySpark (Python) and the Parameters label](../../screenshot/confirm_parameters.png)

4. Run this cell before running the configuration cell. Use Python Boolean
   values `True` and `False`, not quoted strings.

A parameter cell is still a PySpark code cell; marking it as a parameter cell
does not change its language. It identifies default values that a supported
notebook invocation, such as a pipeline Notebook activity, can override.
For this interactive lab, edit `debug` here and use **Run all** to apply it.

### Configuration cell

Copy the following code into a code cell. To run it, select the **Run** button
to the left of the cell or use a keyboard shortcut:

- **Shift+Enter:** Run the current cell and move to the next cell, creating a
  new cell if there is no next cell.
- **Ctrl+Enter:** Run the current cell and stay in the current cell.

```python
# Import Spark helpers for transformations (F) and explicit data types (T).
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Read all Bronze CSVs and name the Silver and Gold output tables.
bronze_path = "Files/bronze/orders/*.csv"
# The *.csv wildcard matches every CSV file in the orders folder.
silver_table = "dbo.sales_silver"
gold_table = "dbo.daily_product_sales_gold"

# Use the nine business columns to identify exact duplicates, excluding metadata.
order_columns = [
    "SalesOrderNumber", "SalesOrderLineNumber", "OrderDate",
    "CustomerName", "Email", "Item", "Quantity", "UnitPrice", "Tax",
]

# Match the headerless CSV's column order and preserve four decimal places for money.
order_schema = T.StructType([
    T.StructField("SalesOrderNumber", T.StringType()),
    T.StructField("SalesOrderLineNumber", T.IntegerType()),
    T.StructField("OrderDate", T.DateType()),
    T.StructField("CustomerName", T.StringType()),
    T.StructField("Email", T.StringType()),
    T.StructField("Item", T.StringType()),
    T.StructField("Quantity", T.IntegerType()),
    T.StructField("UnitPrice", T.DecimalType(18, 4)),
    T.StructField("Tax", T.DecimalType(18, 4)),
])
```

After running the configuration cell, check the execution status beneath it.
A green check mark indicates success, and the status line shows how long the
cell took to complete, who ran it, and when. Your name, timestamp, and duration
will differ from the example. This cell defines configuration, so no data
table output is expected yet.

![Configuration cell execution status showing successful completion, duration, user, and timestamp](../../screenshot/run_output.png)

The explicit schema avoids guessing types. Decimal types preserve the sample's
four-decimal monetary precision instead of using floating-point amounts.
For a lakehouse without schemas, remove `dbo.` from both table variables and
from the SQL queries later in this guide.

**Checkpoint:** The notebook is attached to your own lakehouse and the
configuration cell runs successfully.

## 4. Exercise 1: Read and explore Bronze

Run this cell to read all three years without modifying the source files:

```python
# Read headerless Bronze CSVs using the configured schema; stop on malformed input.
bronze_df = (
    spark.read
    .option("header", "false")
    .option("mode", "FAILFAST")
    .option("dateFormat", "yyyy-MM-dd")
    .schema(order_schema)
    .csv(bronze_path)
    # Track the source file for each record without changing the original CSVs.
    .withColumn("SourceFile", F.input_file_name())
)

# Count all source records and stop if there is no data to transform.
bronze_count = bronze_df.count()
if bronze_count == 0:
    raise ValueError("Bronze is empty. Check the upload folder and source files.")

# Inspect the total row count, column types, and a small sample of records.
print(f"Bronze rows: {bronze_count}")
bronze_df.printSchema()
display(bronze_df.limit(10))

# Count records by order year to confirm coverage across the three source files.
display(
    bronze_df.groupBy(F.year("OrderDate").alias("OrderYear"))
    .count()
    .orderBy("OrderYear")
)
```

`FAILFAST` surfaces malformed input rather than silently accepting bad records.
`SourceFile` records the file each row came from.

![Exercise 1 output showing 32,718 Bronze rows and the DataFrame schema, including decimal monetary columns and SourceFile](../../screenshot/output_1.png)

![Exercise 1 table output showing a preview of 10 Bronze sales order records](../../screenshot/output_2.png)

![Exercise 1 counts by order year showing 1,201 rows for 2019, 2,733 for 2020, and 28,784 for 2021](../../screenshot/output_3.png)

For the sample checked when this lab was authored, expect:

| Year | Rows |
| --- | ---: |
| 2019 | 1,201 |
| 2020 | 2,733 |
| 2021 | 28,784 |
| **Total** | **32,718** |

If upstream data changes, investigate the differences and use the actual
validated source count, rather than forcing the data to match this table.

### Explore built-in visualizations

1. In a table output from this cell, select **+ New chart**.
2. Explore the **Suggested charts** pane. Select a suggestion and choose
   **Start editing** to adjust it, or select **Build my own** to choose your
   own chart type and fields.
3. Try a column chart on the year-count output with **OrderYear** as the
   category and **count** as the value. This compares record counts across
   all three years.
4. Return to the 10-row preview and explore suggestions such as unit price
   or tax by sales order number. Switch back to **Table** to inspect the
   underlying records.

![Notebook built-in chart showing unit price by sales order number alongside the Suggested charts pane, Start editing, and Build my own controls](../../screenshot/new_chart_suggestions.png)

**Note:** Charts use the data returned by that particular `display()` call.
The 10-row preview is only a sample, not the full sales dataset. Suggested
charts are starting points: summing unit prices is not the same as calculating
sales revenue. Later, you will build and chart a Gold table with meaningful
sales measures.

**Discuss:** Why should we preserve Bronze instead of cleaning the CSVs in place?

## 5. Exercise 2: Clean, validate, and write Silver

### Clean the records

Normalize whitespace, supply a label for missing customer names, and normalize
email casing. Keep the original Bronze files unchanged.

```python
# Start from the Bronze DataFrame; transformations leave the source CSVs unchanged.
clean_df = bronze_df
# Remove leading and trailing whitespace from the selected text columns.
for name in ["SalesOrderNumber", "CustomerName", "Email", "Item"]:
    clean_df = clean_df.withColumn(name, F.trim(F.col(name)))

clean_df = (
    clean_df
    # Label missing or blank customer names as Unknown after trimming.
    .withColumn(
        "CustomerName",
        F.when(
            F.col("CustomerName").isNull() | (F.col("CustomerName") == ""),
            F.lit("Unknown"),
        ).otherwise(F.col("CustomerName")),
    )
    # Normalize email casing so equivalent addresses have a consistent form.
    .withColumn("Email", F.lower(F.col("Email")))
)
```

### Validate and deduplicate

For this exercise, an order line must have an order number, positive line number,
valid date, nonempty product, positive quantity, and nonnegative price and tax.
Missing customer names have already been handled; email is not required.

```python
# Define the business rules that every order line must satisfy.
valid_row = (
    F.col("SalesOrderNumber").isNotNull()
    & (F.col("SalesOrderNumber") != "")
    & (F.col("SalesOrderLineNumber") > 0)
    & F.col("OrderDate").isNotNull()
    & F.col("Item").isNotNull()
    & (F.col("Item") != "")
    & (F.col("Quantity") > 0)
    & (F.col("UnitPrice") >= 0)
    & (F.col("Tax") >= 0)
)
# Treat unknown (null) rule results as invalid, not as records to silently skip.
# coalesce returns the first non-null value: keep True/False, or use False for null.
# The ~ operator negates that result so the filter keeps invalid records.
invalid_df = clean_df.filter(~F.coalesce(valid_row, F.lit(False)))
invalid_count = invalid_df.count()
# Show a sample of invalid records and stop before replacing the Silver table.
if invalid_count:
    display(invalid_df.limit(20))
    raise ValueError(f"Found {invalid_count} invalid rows. Resolve them before writing Silver.")

# Remove exact business-record duplicates, ignoring source-file metadata.
silver_df = clean_df.dropDuplicates(order_columns)
silver_count = silver_df.count()
print(f"Exact duplicate records removed: {bronze_count - silver_count}")

# Detect different records that still share the same order-number and line key.
conflicts_df = (
    silver_df.groupBy("SalesOrderNumber", "SalesOrderLineNumber")
    .count()
    .filter(F.col("count") > 1)
)
# Stop rather than arbitrarily choosing which conflicting record to keep.
if conflicts_df.count():
    display(conflicts_df.limit(20))
    raise ValueError("Conflicting records share an order-line key. Resolve them before writing Silver.")

# Calculate sales before tax and record when this transformation is evaluated.
silver_df = (
    silver_df
    .withColumn(
        "LineAmount",
        (F.col("Quantity") * F.col("UnitPrice")).cast("decimal(18,4)"),
    )
    .withColumn("ProcessedAt", F.current_timestamp())
)
```

Exact duplicates are compared using the nine business columns, not `SourceFile`.
Different records with the same order-line key stop the workflow rather than
arbitrarily choosing a winner. In production, invalid records might be sent to a
quarantine table; this introductory notebook stops and shows them explicitly.

![Validation and deduplication output showing zero exact duplicate records removed](../../screenshot/deduplication_output.png)

### Hack: Toggle diagnostic previews with a debug parameter

The separate parameter cell at the top defines a Boolean parameter, `debug`, set to
`False`. Instead of adding and removing display calls when troubleshooting,
use this one switch to control optional Silver previews:

- **`debug = True`:** Display `silver_df` instead of writing the Silver table.
- **`debug = False`:** Write Silver without displaying its records. Keep this
  setting in production to avoid unnecessary record output.

If a downstream pipeline or report contains unexpected data, change `debug` to
`True` at the top and run the notebook only through the Silver cell below.
You can inspect Silver without changing its destination table or editing
individual display calls. After investigating, set it back to `False` and
use **Run all** to refresh both Silver and Gold.

**Important:** In debug mode, stop after the Silver preview. The Gold cells
read the persisted Silver table, which may be missing or contain older data
when the Silver write is skipped. This switch does not disable Gold writes.
Validation checks and explicit error diagnostics remain active regardless of
the setting. A displayed preview is not proof that the entire dataset is correct.

### Persist Silver as Delta

**Write scope:** The next cells replace the two configured output tables.
Use only your own lakehouse. Preserve unrelated tables and do not point the
notebook at shared or production tables.

```python
# Preview in debug mode; otherwise persist and verify the Silver output.
if debug:
    display(silver_df)
else:
    # Rebuild the snapshot instead of appending rows on each run.
    silver_df.write.format("delta").mode("overwrite").saveAsTable(silver_table)

    # Read back the saved table and verify that all validated rows were written.
    saved_silver_count = spark.table(silver_table).count()
    if saved_silver_count != silver_count:
        raise ValueError("Saved Silver count does not match the validated DataFrame.")

    print(f"Saved Silver rows: {saved_silver_count}")
```

### Check before moving to Exercise 3

1. Confirm `debug = False` and that the Silver write cell completed successfully.
2. In the notebook's lakehouse explorer, open the **...** menu beside
   **Tables** and select **Refresh**.

   ![Lakehouse explorer showing the Tables menu and Refresh option](../../screenshot/refresh_silver_table.png)

3. Expand **Tables > dbo** and confirm **sales_silver** is listed. Without
   schemas, look directly under **Tables**.
4. Select **sales_silver** and inspect its preview before continuing.
   If it is missing, confirm the default lakehouse, rerun the parameter cell
   with `debug = False`, and rerun the Silver write cell, then refresh again.

For the verified sample, Silver contains **32,718** rows with unique
`SalesOrderNumber` and `SalesOrderLineNumber` pairs.

**Checkpoint:** Silver is a persisted Delta table, not just a DataFrame in the
current Spark session. Bronze is unchanged.

### Merge vs Overwrite vs Append

The Silver write above currently uses **`mode("overwrite")`**. Choose a write
strategy deliberately:

| Strategy | What happens | What happens if you rerun the same input? |
| --- | --- | --- |
| **Overwrite** | Replaces the table's current data with the DataFrame's complete snapshot. | Replaces it again; does not append duplicates, but removes existing rows absent from the input. |
| **Append** | Adds every input row without checking business keys. | Adds the same rows again, creating duplicates. Use only when new batches are controlled appropriately. |
| **MERGE** | Matches source and target on a business key, updates matching rows, and inserts new rows. | Updates existing keys rather than adding duplicate copies. Rows missing from the source remain unless deletion is explicitly configured. |

`MERGE` is a Delta operation, not a `.mode("merge")` option. It requires a
reliable key and a source with at most one record per key. Our validation
already checks the **SalesOrderNumber + SalesOrderLineNumber** key.

1. Add a **new PySpark code cell** immediately after the Silver table check.
2. Copy the following merge-based write logic into it. Keep `debug = False`
   to persist data, or `True` to preview without writing.
3. After trying the new cell, remove the earlier overwrite code cell from the
   notebook, or convert it to Markdown for reference. Keep only the new Silver
   write cell executable so **Run all** does not overwrite Silver before merging.

```python
from delta.tables import DeltaTable

# Keep the same preview-or-write behavior as the original Silver cell.
if debug:
    display(silver_df)
else:
    if not spark.catalog.tableExists(silver_table):
        # Bootstrap the Delta table on a first run without replacing existing data.
        silver_df.write.format("delta").mode("errorifexists").saveAsTable(silver_table)
    else:
        # Match validated order-line keys, update existing rows, and insert new rows.
        target = DeltaTable.forName(spark, silver_table)
        (
            target.alias("target")
            .merge(
                silver_df.alias("source"),
                "target.SalesOrderNumber = source.SalesOrderNumber "
                "AND target.SalesOrderLineNumber = source.SalesOrderLineNumber",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    # Check that the target still has only one row per business key.
    saved_silver = spark.table(silver_table)
    duplicate_keys = (
        saved_silver.groupBy("SalesOrderNumber", "SalesOrderLineNumber")
        .count()
        .filter(F.col("count") > 1)
    )
    if duplicate_keys.count():
        display(duplicate_keys.limit(20))
        raise ValueError("Silver contains duplicate order-line keys. Investigate before building Gold.")

    print(f"Saved Silver rows after MERGE: {saved_silver.count()}")
```

4. Run the new cell twice with `debug = False`. For the unchanged workshop
   sample, expect **32,718** rows after each run and no duplicate keys.
5. Refresh **Tables** again and confirm `sales_silver` is still available
   before moving to Exercise 3.

This merge updates all matching columns, including `ProcessedAt`, so reruns can
create new Delta versions even when business values are unchanged. It does not
remove target rows missing from the source, resolve preexisting target
duplicates, or automatically detect new files. This notebook still reads all
Bronze files; a merge alone is not a complete incremental-ingestion solution.

### Mini-exercise: Merge one existing row and one new row

Add a new PySpark cell after the merge cell. Create a two-row DataFrame named
`new_silver`: one row copied from the persisted Silver table and one synthetic
order line with a new order number. A DataFrame is sufficient here; you do not
need to save a separate staging table.

Set `debug = False` and run the parameter cell before this exercise, because
the goal is to test a real write.

```python
from delta.tables import DeltaTable

if debug:
    raise ValueError("Set debug = False and rerun the parameter cell to test the merge.")

# Reserve a recognizable order number so reruns update the same demo record.
demo_order = "LAB02-MERGE-001"
before_count = spark.table(silver_table).count()
demo_exists = (
    spark.table(silver_table)
    .filter(F.col("SalesOrderNumber") == demo_order)
    .limit(1)
    .count() > 0
)

# Choose an existing non-demo order line so the two source keys stay distinct.
existing_row = (
    spark.table(silver_table)
    .filter(F.col("SalesOrderNumber") != demo_order)
    .orderBy("SalesOrderNumber", "SalesOrderLineNumber")
    .limit(1)
)
if existing_row.count() == 0:
    raise ValueError("Silver has no existing order line. Complete the Silver write first.")

# Copy its valid values, changing the order number to create a new business key.
new_row = (
    existing_row
    .withColumn("SalesOrderNumber", F.lit(demo_order))
    .withColumn("SourceFile", F.lit("Synthetic merge exercise"))
    .withColumn("ProcessedAt", F.current_timestamp())
)
new_silver = existing_row.unionByName(new_row)
display(new_silver)

# Update the existing key and insert only the new key.
(
    DeltaTable.forName(spark, silver_table).alias("target")
    .merge(
        new_silver.alias("source"),
        "target.SalesOrderNumber = source.SalesOrderNumber "
        "AND target.SalesOrderLineNumber = source.SalesOrderLineNumber",
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# Expect +1 on the first run; reruns match both keys and add no further rows.
after_count = spark.table(silver_table).count()
expected_increase = 0 if demo_exists else 1
print(f"Silver before: {before_count}; after: {after_count}")
print(f"Row count change: {after_count - before_count} (expected +{expected_increase})")
if after_count != before_count + expected_increase:
    raise ValueError("Unexpected Silver row count after merging the two-row batch.")
```

**Expected result:** On the first run, the two-row batch increases Silver by
**one row**, not two: the copied row matches an existing key and the synthetic
row introduces a new key. With the verified sample, Silver increases from
**32,718 to 32,719** rows. Rerunning the cell adds **zero** rows because the
demo key now exists.

![Merge exercise output showing Silver increasing from 32,718 to 32,719 rows, a change of 1 as expected](../../screenshot/expected_results.png)

The synthetic row is not added to Bronze. The main Silver merge retains it
because that merge does not delete target rows absent from its source.
Gold will include this extra line in its totals when you run Exercise 3; its
date and product are copied from an existing group, so the number of Gold
groups should remain unchanged.

## 6. Exercise 3: Build a business-ready Gold aggregate

**Business question:** For each day and product, how many units were sold and
what was the sales amount before tax?

Before continuing, set `debug = False` and rerun the parameter and Silver cells
so that the persisted Silver table contains your latest transformed data.

Read the persisted Silver table, rather than relying on an in-memory DataFrame,
and aggregate at the **OrderDate + Item** grain:

```python
# Use persisted Silver as the trusted input for the Gold transformation.
silver_source = spark.table(silver_table)
# Produce one summary per date and product, with line counts and additive totals.
gold_df = (
    silver_source.groupBy("OrderDate", "Item")
    .agg(
        F.count("*").alias("OrderLineCount"),
        F.sum("Quantity").alias("UnitsSold"),
        F.sum("LineAmount").alias("SalesAmount"),
        F.sum("Tax").alias("TaxAmount"),
    )
)

# Replace the lab's Gold snapshot so reruns do not accumulate duplicate summaries.
gold_df.write.format("delta").mode("overwrite").saveAsTable(gold_table)

# Preview the highest-sales daily product groups from the saved Gold table.
gold_saved = spark.table(gold_table)
display(
    gold_saved.orderBy(F.col("SalesAmount").desc(), "OrderDate", "Item").limit(10)
)
print(f"Gold rows: {gold_saved.count()}")
```

`SalesAmount` is quantity times unit price, excluding tax. `TaxAmount` sums
the source's line-level tax; do not multiply tax by quantity again.

For the verified sample, expect **14,386** daily-product groups. Gold has fewer
rows because each record summarizes multiple order lines where applicable.
It intentionally omits customer names and email addresses.

### Reconcile Gold with Silver

```python
# Compute baseline totals from the detailed Silver order lines.
silver_totals = silver_source.agg(
    F.count("*").alias("OrderLineCount"),
    F.sum("Quantity").alias("UnitsSold"),
    F.sum("LineAmount").alias("SalesAmount"),
    F.sum("Tax").alias("TaxAmount"),
).first()

# Sum Gold's stored line counts, not its number of aggregated rows.
gold_totals = gold_saved.agg(
    F.sum("OrderLineCount").alias("OrderLineCount"),
    F.sum("UnitsSold").alias("UnitsSold"),
    F.sum("SalesAmount").alias("SalesAmount"),
    F.sum("TaxAmount").alias("TaxAmount"),
).first()

# Require all four measures to match; raise an error if aggregation lost data.
if silver_totals.asDict() != gold_totals.asDict():
    raise ValueError(f"Reconciliation failed: Silver={silver_totals}, Gold={gold_totals}")

print("Reconciliation passed: line counts, units, sales, and tax match.")
```

**Checkpoint:** Gold is a Delta table whose totals reconcile with Silver.
Gold's row count is not expected to equal Silver's row count.

## 7. Exercise 4: Query and visualize Gold

1. Add a new notebook code cell.
2. Use `%%sql` as the first line to switch that cell from Python to Spark SQL:

   ```sql
   %%sql
   -- Summarize daily Gold records into total units and sales for each product.
   SELECT
       Item,
       SUM(UnitsSold) AS units_sold,
       SUM(SalesAmount) AS sales_amount
   FROM dbo.daily_product_sales_gold
   GROUP BY Item
   -- Return the ten highest-sales products, using Item to break ties.
   ORDER BY sales_amount DESC, Item
   LIMIT 10;
   ```

   Alternatively, select **Spark SQL** from the cell's language dropdown and
   omit the `%%sql` line. Confirm the cell footer shows **Spark SQL**:

   ![Notebook cell language dropdown showing Spark SQL](../../screenshot/verify_sparksql.png)

3. In the result visualization controls, choose a bar chart with **Item** as
   the category and **sales_amount** as the value, if charting is available.

   ![Gold query visualization showing units sold by Item and a suggested sales_amount by Item chart](../../screenshot/sales_amount_item_barchart.png)

4. Explain why this query uses Gold instead of repeatedly cleaning the raw CSVs.

You can also query the Gold table from the lakehouse's **SQL analytics
endpoint** after metadata synchronization. Use T-SQL `TOP (10)` instead of
Spark SQL `LIMIT 10`, and omit `%%sql` outside the notebook.

## 8. Rerun, save, and share

1. Record the Silver and Gold row counts.
2. Select **Run all** to execute the notebook from top to bottom.
3. Confirm both counts are unchanged and reconciliation passes again.
4. Save the notebook and confirm it appears in your workspace.
5. Share the notebook only with authorized workshop participants. Recipients
   also need the appropriate lakehouse permissions and Fabric capacity access.

This notebook recomputes transformations from all Bronze files. Silver begins
with an **overwrite** example, then switches to a key-based **MERGE**; Gold
remains a full-refresh overwrite of the persisted Silver data. Reruns do not
append duplicate rows. Processing
timestamps and Delta history versions can change even when business data does
not. The two table writes are separate commits, not one cross-table transaction;
if a later cell fails, fix the problem and rerun the notebook in order.

Incremental file ingestion, scheduling, and a full star schema are
next steps, not part of this lab.

## 9. Explain what you built

- What is the grain of each layer?
- Why is an explicit schema important for a headerless CSV?
- What belongs in Silver versus Gold?
- Why validate before overwriting a trusted table?
- How does this notebook avoid duplicates when rerun?
- How is notebook code different from a persisted Delta table?

**Completion checklist**

- [ ] Attached the existing lakehouse and created a transformation notebook.
- [ ] Preserved the three source CSVs in Bronze.
- [ ] Explored the schema and counts by year.
- [ ] Cleaned, validated, and saved Silver.
- [ ] Compared write strategies and replaced the Silver overwrite cell with MERGE.
- [ ] Built Gold at a defined business grain.
- [ ] Reconciled Gold totals with Silver.
- [ ] Queried Gold using notebook Spark SQL.
- [ ] Reran the notebook without increasing business row counts.
- [ ] Saved the notebook for reuse.

## Troubleshooting

| Symptom | What to check |
| --- | --- |
| Spark session will not start | Confirm active Fabric capacity and workspace permissions with the facilitator. |
| Bronze path is missing | Confirm the correct default lakehouse and the exact `Files/bronze/orders` folder. |
| The first order disappears | The CSVs have no header. Use `header=false`. |
| CSV parse or validation failure | Check that all three files are the original order CSVs, not unrelated files, HTML downloads, or edited spreadsheets. Inspect the reported records. |
| Conflicting order-line keys | Inspect the records; do not bypass the check or arbitrarily drop conflicting values. |
| `dbo` cannot be resolved | Use a schema-enabled lakehouse or remove `dbo.` consistently for a lakehouse without schemas. |
| Python variables are undefined | Run the configuration and earlier cells in order after restarting Spark. |
| SQL endpoint cannot see a table yet | Allow metadata synchronization, then refresh the endpoint explorer. |
| Counts increase on rerun | Check that Silver uses the validated-key MERGE and Gold uses overwrite, not append. Check source and target key uniqueness. |

## Cleanup

Keep the reused lakehouse and notebook if the facilitator plans to reuse them.
Otherwise, after confirming with the facilitator, remove only your Lab 2
notebook, `sales_silver` and `daily_product_sales_gold` tables, and
`Files/bronze/orders` folder. Do not delete the reused lakehouse or unrelated items
unless the facilitator explicitly confirms they are no longer needed. Do not
delete the assigned workspace or another participant's items.

## References

- [Analyze data with Apache Spark in Fabric](https://microsoftlearning.github.io/mslearn-fabric/Instructions/Labs/02-analyze-spark.html)
- [Create a medallion architecture in a Microsoft Fabric lakehouse](https://microsoftlearning.github.io/mslearn-fabric/Instructions/Labs/03b-medallion-lakehouse.html)
- [Microsoft Learn: Medallion lakehouse architecture](https://learn.microsoft.com/en-us/fabric/onelake/onelake-medallion-lakehouse-architecture)
- [Source data: MicrosoftLearning/dp-data](https://github.com/MicrosoftLearning/dp-data)
