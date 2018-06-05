# Data Server Manager Historical Monitoring Reports
These Python notebooks retrieve data from the Data Server Manager monitoring history to map out resource usage across your Db2 Enterprise. You will need connection privileges to the Data Server Manager repository database.

# The Reports 
single-db-resource-consumption: Reports on key resource usage for a single database
2-db-resource-comparison: Compares the resource usages of two different databases
enterprise-view: Compares multiple databases across your enterprise
Storage-usage-with-prediction: Displays storage usage with a prediction of future growth
transaction-log-usage-with-prediction: Displays transaction log usage with a prediction of possible future usage

# Supporting Scripts
The "-scripts" that support each of the reports include most of the detailed code to parse the report request as well as to format each report consistently.

# Using with DSX Desktop
An easy way to get started with Notebooks is to download the DSX Desktop from IBM: https://datascience.ibm.com/docs/content/desktop/welcome.html

Once you have installed the DSX desktop you can create a new notebook by selecting My Notebooks and New Notebook and From File. Download the MineRepository.ipynb file from this project and choose the file from in the Notebook File section of the Create Notebook form. Provide a new notebook and select Create Notebook.

You can then run the notebook by selecting the run cell icon for each step in the notebook. Make sure you update cell number three with your unique database connection information.

