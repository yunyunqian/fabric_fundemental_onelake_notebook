# Microsoft Fabric Customer Lab Workshop

Hands-on labs for a facilitator-led customer workshop.

## Workshop status

This repository contains participant and facilitator guides for the workshop.
The guides are Markdown files; the hands-on exercises run in
Microsoft Fabric, not on a local Spark installation.

## Audience

People new to Microsoft Fabric who want to understand lakehouses and Delta tables.
Lab 1 does not require coding experience.
Lab 2 introduces PySpark notebooks and guided data transformation.

## Before the workshop

- Have a Microsoft Fabric account and access to the [Fabric portal](https://app.fabric.microsoft.com/).
- Have a workspace assigned to an active Fabric capacity or an eligible, active
  Fabric trial. A Power BI-only workspace is not sufficient for this exercise.
- Have permission to create lakehouses and upload data in that workspace
  (typically Contributor, Member, or Admin).
- Use a browser and a laptop that can download files from GitHub.
- Use workshop sample data only; do not upload customer or confidential data.

The facilitator should confirm these prerequisites before participants arrive.

## Labs

| Lab | Topic | Estimated time | Participant guide |
| --- | --- | --- | --- |
| 1 | Lakehouse basics: upload a CSV and create a Delta table | 25-35 minutes | [Start Lab 1](labs/01-lakehouse-and-delta/README.md) |
| 2 | Build a medallion transformation notebook: Bronze, Silver, and Gold | 60-75 minutes | [Start Lab 2](labs/02-medallion-transformation/README.md) |

## Facilitator resources

- [Lab 1 facilitator guide](facilitator/lab-01.md)
- [Lab 2 facilitator guide](facilitator/lab-02.md)

## Reviewing locally

1. Open this folder in Visual Studio Code.
2. Open this README and use **Open Preview** (`Ctrl+Shift+V`).
3. Follow the participant guide links and review the instructions.
4. Rehearse the lab in your Fabric workspace before delivering it.

No local package installation, web server, or build step is required.

## Contributing updates

Review changes and rehearse the affected Fabric exercises before publishing
updates. Review screenshots for identifying information and obtain approval
before sharing them. Do not commit downloaded data, credentials, tenant details,
or unapproved participant information.

## Sample data and references

Participants download the sample directly from
[MicrosoftLearning/dp-data](https://github.com/MicrosoftLearning/dp-data).
The dataset is not bundled with this workshop; its upstream terms apply.

- [Fabric lakehouse overview](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-overview)
- [Load files into a lakehouse](https://learn.microsoft.com/en-us/fabric/data-engineering/load-data-lakehouse)
- [Load files into Delta tables](https://learn.microsoft.com/en-us/fabric/data-engineering/load-to-tables)
- [Analyze data with Apache Spark in Fabric](https://microsoftlearning.github.io/mslearn-fabric/Instructions/Labs/02-analyze-spark.html)
- [Create a medallion architecture in a Microsoft Fabric lakehouse](https://microsoftlearning.github.io/mslearn-fabric/Instructions/Labs/03b-medallion-lakehouse.html)
