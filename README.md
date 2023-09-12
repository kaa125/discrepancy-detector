#  Discrepancy Detector for AWS RDS and BigQuery Data Comparison

The Discrepancy Detector is a data validation and comparison tool designed to ensure the consistency and accuracy of data between two different data sources: Amazon Web Services Relational Database Service (AWS RDS) and Google BigQuery. This project serves as a crucial quality assurance component in data pipelines, helping organizations maintain data integrity and identify potential issues in their data ingestion and transformation processes.

Key Features:

Data Source Integration: The Discrepancy Detector seamlessly connects to both AWS RDS and Google BigQuery, enabling it to retrieve data from these sources for comparison.
Schema Validation: The tool performs schema validation to ensure that the structure of the data in both sources is consistent. It checks that tables, columns, and data types match between RDS and BigQuery.
Data Comparison: The heart of the tool lies in its ability to compare data records between the two sources. It detects discrepancies, such as missing rows, differing column values, or unexpected data.
Customizable Queries: Users can define SQL queries to extract specific data subsets for comparison. This flexibility allows for targeted checks on critical data points.
Logging and Reporting: The tool logs the results of the comparison, highlighting any discrepancies found. It generates detailed reports, making it easy for data engineers and analysts to investigate and address issues.
Automation: It supports scheduled or event-driven execution to periodically validate data. Automation ensures that data consistency checks are performed regularly without manual intervention.
Alerting: The Discrepancy Detector can be configured to send alerts or notifications when significant discrepancies are detected. This feature enables quick response to data integrity issues.
