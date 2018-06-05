# Mine Data Server Manager Historical Monitoring Data
This Python notebook retrieves data from the Data Server Manager monitoring history to map out resource usage across your Db2 Enterprise. You will need connection privileges to the Data Server Manager repository database.

The notebook creates a view in the database that provides a list of databases and workloads in each database for each data point collected. The values collected are key resouce metrics: CPU seconds, Transactions, and Logical Reads. 

You can then map out a pivot table for the total resources or throughput per database and workload by week for each database in your Data Server Manager monitoring enterprise. 

# Using with DSX Desktop
An easy way to get started with Notebooks is to download the DSX Desktop from IBM: https://datascience.ibm.com/docs/content/desktop/welcome.html

Once you have installed the DSX desktop you can create a new notebook by selecting My Notebooks and New Notebook and From File. Download the MineRepository.ipynb file from this project and choose the file from in the Notebook File section of the Create Notebook form. Provide a new notebook and select Create Notebook.

You can then run the notebook by selecting the run cell icon for each step in the notebook. Make sure you update cell number three with your unique database connection information.

