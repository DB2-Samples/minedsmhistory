# Mine Data Server Manager Historical Monitoring Data
This Python notebook retrieves data from the Data Server Manager monitoring history to map out resource usages across your Db2 Enterprise. You will need conntection privledges to the Data Server Manager repository database.

The notebook creates a view in the database that provides a list of databases and workloads in each database for each data point collected. The values collected are key resouce metrics: CPU seconds, Transactions, and Logical Reads. 

You can then map out a pivot table for the total resources or throughput per database and workload by week for each database in your Data Server Manager monitoring enterprise. 
