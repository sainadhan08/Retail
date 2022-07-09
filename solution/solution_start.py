import argparse
import os
import json
import csv
from pyspark.sql import SparkSession
import pandas


def get_params() -> dict:
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="./input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="./input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="./input_data/starter/transactions/")
    parser.add_argument('--output_location', required=False, default="./output_data/outputs/")
    return vars(parser.parse_args())

def main():
    params = get_params()
    #Taking all the dates transactions.json file for last 6 months from the given start date
    d = os.listdir('../input_data/starter/transactions')
    #print(d)
    columns = ["customer_id", "product_id", "price", "date_of_purchase"]
    data_file = open('data_file.csv', 'w')
    writer = csv.DictWriter(data_file, fieldnames=columns)
    writer.writeheader()
    #Consolidating all transactions json to one json file.
    for i in d:
        p = open("../input_data/starter/transactions/{date}/transactions.json".format(date=i))
        for j in p:
            k = json.loads(j)
            t = k["basket"]
            # print(t)
            for i in t:
                i["customer_id"] = k["customer_id"]
                i["date_of_purchase"] = k["date_of_purchase"]
                t[0] = i
            writer.writerows(t)

    data_file.close()
    #Removing duplicates from the data_file.csv
    with open('data_file.csv', 'r') as in_file, open('transactions.csv', 'w') as out_file:

        seen = set()  # set for fast O(1) amortized lookup

        for line in in_file:
            if line in seen:
                continue  # skip duplicate

            seen.add(line)
            out_file.write(line)
    out_file.close()
    data_file.close()
    #loading all the csv files rquired to generate output
    customers=pandas.read_csv("../input_data/starter/customers.csv")
    products=pandas.read_csv("../input_data/starter/products.csv")
    transactions=pandas.read_csv("../solution/transactions.csv")
    df1 = pandas.merge(customers, transactions, on='customer_id', how='inner')
    #print(df1)
    df2=pandas.merge(df1, products, on='product_id', how='inner')
   # print(df2.columns)
    #grouping and taking the purchase count
    output1=df2.groupby(['customer_id', 'loyalty_score', 'product_id','product_category'])['customer_id'].count()\
         .reset_index(name='purchase_count')

    #print(output1)
    l=[]
    for i in output1.values:
        d=dict()
        d['customer_id']=i[0]
        d['loyalty_score']=i[1]
        d['product_id']=i[2]
        d['product_category']=i[3]
        d['purchase_count']=i[4]
        #print(i)

        l.append(d)
    out = dict()
    out["FINALOUTPUT"] = l
    #print(out)
    output=json.dumps(out)
    #Writing the output json file
    with open("../output_data/outputs/output.json", "w") as outfile:
        outfile.write(output)
    out_file.close()





if __name__ == "__main__":
    main()
