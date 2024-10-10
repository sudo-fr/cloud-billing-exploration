```
  /$$$$$$  /$$   /$$ /$$$$$$$   /$$$$$$ 
 /$$__  $$| $$  | $$| $$__  $$ /$$__  $$
| $$  \__/| $$  | $$| $$  \ $$| $$  \ $$
|  $$$$$$ | $$  | $$| $$  | $$| $$  | $$
 \____  $$| $$  | $$| $$  | $$| $$  | $$
 /$$  \ $$| $$  | $$| $$  | $$| $$  | $$
|  $$$$$$/|  $$$$$$/| $$$$$$$/|  $$$$$$/
 \______/  \______/ |_______/  \______/ 
```

# cloud-billing-exploration

This repository is meant to document, explain and store all research linked to the analysis and processing of cloud billing data.

In the `src` folder you will find (at some point) one folder for each cloud provider group (MFST AZURE, GCP, ALIBABA, AWS) containing scripts and data used to process each one of the billing data (because most of the time, the billing data from one cloud provider to another differ). Each notebook in these subfolders has a noticeable use for the missions of SudoGroup when it comes to proposing optimizations in cloud usage (notably rightsizing, cost and consumption forecasting).

In each cloud provider subfolder you will find a notebook solely aimed at explaining the columns that can be found in a cloud bill from this particular cloud provider. The explanation are public, however the raw data in itself will not be available (only data that has been processed and hashed will be available in the `data` folder).
In each notebook you will find explanation of the models used, their deployment and evaluation.


> N.B. Each notebook is documented but I assumed that the reader has a minimum of knowledge in ML and basic models as well as common practices in data science / engineering. Otherwise please open an issue and I will be glad to answer it and document/modify the code accordingly.


--- 

Pierre Lague @SudoGroup :)