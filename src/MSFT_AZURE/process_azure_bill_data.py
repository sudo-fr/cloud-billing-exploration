"""
  /$$$$$$  /$$   /$$ /$$$$$$$   /$$$$$$ 
 /$$__  $$| $$  | $$| $$__  $$ /$$__  $$
| $$  \__/| $$  | $$| $$  \ $$| $$  \ $$
|  $$$$$$ | $$  | $$| $$  | $$| $$  | $$
 \____  $$| $$  | $$| $$  | $$| $$  | $$
 /$$  \ $$| $$  | $$| $$  | $$| $$  | $$
|  $$$$$$/|  $$$$$$/| $$$$$$$/|  $$$$$$/
 \______/  \______/ |_______/  \______/ 


 @Author : Pierre Lague

 @Email : p.lague@sudogroup.fr

 @Date : 10/10/2024
"""


import pandas as pd
import numpy as np
import spacy
import json
import tqdm

class Azure_Bill_Data_Processor():
    def __init__(self, df) -> None:
        self.azure_bill = df

    def create_processed_dataframe(self, filter_meter_category):
        """
        Create a processed DataFrame from Azure billing data.

        This function filters the Azure billing data based on a specified meter category,
        converts date strings to timestamps, and applies various encoding techniques to
        different columns of the data.

        Parameters:
        -----------
        filter_meter_category : str
            The meter category to filter the Azure billing data.

        Returns:
        --------
        pandas.DataFrame
            A processed DataFrame containing encoded and transformed Azure billing data.

        Notes:
        ------
        - The function applies different encoding techniques to various column groups:
          - Hexadecimal encoding for SubscriptionId, ProductOrderId, and timestamp__Date
          - Hash encoding for various categorical columns
          - Spacy vector encoding for text-based columns
          - Numerical columns are left as-is
        - The processed data is saved to a CSV file named 'Azure_data_processed.csv'
        - The function uses several helper methods like convert_to_datetime, hex_fct,
          hash_fct, and spacy_fct for data transformation.
        """

        # intial filter
        self.azure_bill = self.azure_bill.loc[self.azure_bill["MeterCategory"]==filter_meter_category]
        print(len(self.azure_bill))
        # converting to timestamp
        self.azure_bill = self.convert_to_datetime(self.azure_bill, "_Date")

        #if filter_meter_category=="Virtual Machines":
        #    self.azure_bill = self.add_performance_columns(self.azure_bill)
        #    numerical_cols = ["Quantity", "Cost", "VCPUs", "ConsumedQty"] # has to be numerical, be sure about it :)

        hex_col = ["SubscriptionId","ProductOrderId","timestamp__Date"] # don't edit that, it's all you need at the moment
        ############ EDIT NAMES WITH WHAT YOU WANT ##############
        hash_col = ["MeterRegion", "BillingCurrency", "ResourceLocation", "ConsumedService", "ServiceInfo2", "UnitOfMeasure", "ProductOrderName", "OfferId", "IsAzureCreditEligible", "PublisherName", "ChargeType", "Frequency", "PublisherType", "PricingModel", "SubscriptionEnv", "ResourceGroupEnv"]
        spacy_col = ["Product", "PartNumber", "MeterCategory", "MeterSubCategory", "MeterName", "ResourceId", "ResourceName", "AdditionalInfo", "PlanName", "benefitId", "benefitName"]
        numerical_cols = ["Quantity", "Cost"] 

        # transofrming all interesting features into numerical values
        hex_df = self.azure_bill[hex_col]
        hash_df = self.azure_bill[hash_col]
        spacy_df = self.azure_bill[spacy_col]
        num_df = self.azure_bill[numerical_cols]

        # applying processing functions
        for col in hex_df.columns:
            hex_df[col] = hex_df[col].apply(lambda x: self.hex_fct(x))
        for col in hash_df.columns:
            hash_df[col] = hash_df[col].apply(lambda x:self.hash_fct(x))
        for col in spacy_df.columns:
            spacy_df[col] = spacy_df[col].apply(lambda x: self.spacy_fct(x)) # THIS ONE IS PRETTY LONG SO BE CAREFUL WHEN YOU EXECUTE IT

        
        # concatenating for the final df
        total_df = pd.concat([hex_df, hash_df, spacy_df, num_df])
        total_df.to_csv('./data/Azure_data_processed.csv')

        # OPEN IT AFTERWARDS

    def convert_to_datetime(self, df, label):
        """
        Convert date strings to datetime and create a timestamp column.

        Parameters:
        -----------
        df : pandas.DataFrame
            The DataFrame containing the date column to be converted.
        label : str
            The name of the column containing date strings.

        Returns:
        --------
        pandas.DataFrame
            A DataFrame with the original date column replaced by a timestamp column.

        Notes:
        ------
        - The original date column is converted to datetime using the format '%m/%d/%Y'
        - A new column named 'timestamp_{label}' is created with Unix timestamps
        - The original date column is dropped from the DataFrame
        """

        # Convert date strings to datetime
        df.loc[df.index, label] = (pd.to_datetime(df[label], format='%m/%d/%Y'))
        df[f'timestamp_{label}'] = df[label].apply(lambda x: x.timestamp())
        df = df.drop(columns=[label])

        return df
    

    def hex_fct(self, label):
        """
        Convert a string to its hexadecimal representation.

        Parameters:
        -----------
        label : str or float
            The input value to be converted to hexadecimal.

        Returns:
        --------
        int
            The hexadecimal representation of the input as an integer.

        Notes:
        ------
        - If the input is null or a float, the function returns 0
        - For string inputs, hyphens are removed before conversion
        - The function assumes the input string is a valid hexadecimal representation
        """
        if pd.isnull(label) or isinstance(label, float):
            return 0
        else:
            return int(label.replace("-", ""), 16)


    def hash_fct(self,x):
        """
        Compute the hash value of the input.

        Parameters:
        -----------
        x : Any
            The input value to be hashed.

        Returns:
        --------
        int
            The hash value of the input.

        Notes:
        ------
        - This function uses Python's built-in hash() function
        - The hash value may vary between Python sessions or implementations
        """
        return hash(x)


    def spacy_fct(self, col):
        """
        Compute the average word vector for a text using spaCy.

        Parameters:
        -----------
        col : str or float
            The input text to be processed.

        Returns:
        --------
        float
            The mean value of the word vectors in the input text.

        Notes:
        ------
        - This function requires the 'en_core_web_sm' model from spaCy
        - If the input is null, the function returns 0
        - For text inputs, it computes the mean of word vectors using spaCy
        """
        nlp = spacy.load('en_core_web_sm')
        if pd.isnull(col):
            return 0
        else:
            col = np.mean(nlp(col).vector)
            return col
        

    def get_cpus(self, dict_str):
        """
        Extract the number of VCPUs from a JSON string.

        Parameters:
        -----------
        dict_str : str
            A JSON string containing information about VCPUs.

        Returns:
        --------
        int or None
            The number of VCPUs if found in the JSON string, None otherwise.

        Notes:
        ------
        - This function attempts to parse the input string as JSON
        - It looks for a 'VCPUs' key in the resulting dictionary
        - If parsing fails or the key is not found, it returns None
        """
        try:
            return json.loads(str(dict_str)).get("VCPUs")
        except json.JSONDecodeError:
            return None
        
    def get_CQ(self, dict_str):
        """
        Extract the ConsumedQuantity from a JSON string.

        Parameters:
        -----------
        dict_str : str
            A JSON string containing information about ConsumedQuantity.

        Returns:
        --------
        float or None
            The ConsumedQuantity if found in the JSON string, None otherwise.

        Notes:
        ------
        - This function attempts to parse the input string as JSON
        - It looks for a 'ConsumedQuantity' key in the resulting dictionary
        - If parsing fails or the key is not found, it returns None
        """
        try:
            return json.loads(str(dict_str)).get("ConsumedQuantity")
        except json.JSONDecodeError:
            return None

    def add_performance_columns(self, df):
        """
        Add performance-related columns to the DataFrame.

        Parameters:
        -----------
        df : pandas.DataFrame
            The input DataFrame to which performance columns will be added.

        Returns:
        --------
        pandas.DataFrame
            A copy of the input DataFrame with additional performance columns.

        Notes:
        ------
        - This function adds two new columns: 'ConsumedQty' and 'VCPUs'
        - It extracts these values from the 'AdditionalInfo' column, assuming it contains JSON data
        - The 'VCPUs' column is further processed to contain boolean values indicating if the value is an integer
        - The function returns a copy of the input DataFrame to avoid modifying the original
        """
        df["AdditionalInfo"].astype("str")
        # Create a copy of the DataFrame to ensure we're not working on a slice
        df_copy = df.copy()
        
        # Add the new column
        df_copy['ConsumedQty'] = df_copy['AdditionalInfo'].apply(self.get_CQ)
        df_copy['VCPUs'] = df_copy['AdditionalInfo'].apply(self.get_cpus)
        df_copy['VCPUs'] = df["VCPUs"].map(lambda x: isinstance(x,int))
        
        return df_copy
    


#azure_bill = pd.read_csv(r"C:\Users\Pierre LAGUE\Documents\SUDO\scripts\CLOUD_PROVIDER_BILL\data\azure_sample_10000.csv")
#processor = Azure_Bill_Data_Processor(azure_bill)
#processor.create_processed_dataframe("Virtual Machines")