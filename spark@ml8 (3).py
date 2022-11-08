import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, StringType
from pyspark.sql import functions as f
from pyspark.context import SparkContext
import sys
import pyspark.sql.utils


spark = SparkSession.builder.appName("Masking").getOrCreate()
spark.sparkContext.addPyFile("s3://harshodai-landingzone/dependencyfiles/delta-core_2.12-0.8.0.jar")
from delta.tables import *

class configuration:
    def __init__(self):
        self.jsonData = self.read_config()
        self.ingest_actives_source = self.jsonData['ingest-Actives']['source']['data-location']
        self.ingest_actives_dest = self.jsonData['ingest-Actives']['destination']['data-location']
        self.ingest_viewership_source = self.jsonData['ingest-Viewership']['source']['data-location']
        self.ingest_viewership_dest = self.jsonData['ingest-Viewership']['destination']['data-location']
        self.transformation_cols_actives = self.jsonData['masked-Actives']['transformation-cols']
        self.transformation_cols_viewership = self.jsonData['masked-Viewership']['transformation-cols']
        self.ingest_raw_actives_source = self.jsonData['masked-Actives']['source']['data-location']
        self.ingest_raw_viewership_source = self.jsonData['masked-Viewership']['source']['data-location']
        self.masking_col_actives= self.jsonData['masked-Actives']['masking-cols']
        self.masking_col_viewership= self.jsonData['masked-Viewership']['masking-cols']
        self.dest_staging_actives= self.jsonData['masked-Actives']['destination']['data-location']
        self.dest_staging_viewership= self.jsonData['masked-Viewership']['destination']['data-location']
        self.actives_partition_cols= self.jsonData['masked-Actives']['partition-cols']
        self.viewership_partition_cols= self.jsonData['masked-Viewership']['partition-cols']
        self.lookupDataLocation= self.jsonData['lookup-dataset']['data-location']
        self.piiColumns= self.jsonData['lookup-dataset']['pii-cols']
        
    def read_config(self):
        configData = spark.sparkContext.textFile("s3://harshodai-landingzone/config_files/app_config.json").collect()
        dataString = ''.join(configData)
        jsonData = json.loads(dataString)
        return jsonData
        

class transformation:
    
    def read_data(self,path,format):
        df=spark.read.parquet(path)
        return df

    def write_data(self,df,path,format):
        df.write.mode("overwrite").parquet(path)
        
    def writeDataByPartition(self,df,path,format,colName=[]):
        df.write.mode("overwrite").partitionBy(colName[0],colName[1]).parquet(path)
    
    def transformation(self,df,cast_dict):
        key_list = []
        for key in cast_dict.keys():
            key_list.append(key)

        for column in key_list:
            if cast_dict[column].split(",")[0] == "DecimalType":
                df = df.withColumn(column,df[column].cast(DecimalType(scale=int(cast_dict[column].split(",")[1]))))
            elif cast_dict[column] == "ArrayType-StringType":
                df = df.withColumn(column,f.concat_ws(",",f.col(column)))
        return df
    
    def masking_data(self,df,column_list):
        for column in column_list:
            df = df.withColumn("Masked_"+column,f.sha2(f.col(column),256))
        return df
    
    def scdDataset(self,df,lookupDataLocation,piiColumns,datasetName):
        df_source = df.withColumn("begin_date",f.current_date())
        df_source = df_source.withColumn("update_date",f.lit("null"))
        pii_cols = [i for i in piiColumns if i in df.columns]
        columns_needed = []
        insert_dict = {}
        
        for col in pii_cols:
            if col in df.columns:
                columns_needed += [col,"Masked_"+col]
        
        source_columns_used = columns_needed + ['begin_date','update_date']
        df_source = df_source.select(*source_columns_used)
        try:
            targetTable = DeltaTable.forPath(spark,lookupDataLocation+datasetName)
            delta_df = targetTable.toDF()
        except pyspark.sql.utils.AnalysisException:
            print('Table does not exist')
            df_source = df_source.withColumn("flag_active",f.lit("true"))
            df_source.write.format("delta").mode("overwrite").save(lookupDataLocation+datasetName)
            print('Table Created Sucessfully!')
            targetTable = DeltaTable.forPath(spark,lookupDataLocation+datasetName)
            delta_df = targetTable.toDF()
            delta_df.show(100)
        
        for i in columns_needed:
            insert_dict[i] = "updates."+i
        
        insert_dict['begin_date'] = f.current_date()
        insert_dict['flag_active'] = "True" 
        insert_dict['update_date'] = "null"
        
        _condition ="dtName.flag_active == true AND "+" OR ".join(["updates."+i+" <> "+ "dtName."+i for i in [x for x in columns_needed if x.startswith("Masked_")]])
        column = ",".join(["dtName"+"."+i for i in [x for x in pii_cols]])
        print('..._condition',_condition)
        
        updatedColumnsToInsert = df_source.alias("updates").join(targetTable.toDF().alias("dtName"), pii_cols).where(_condition)
        
        stagedUpdates = (
          updatedColumnsToInsert.selectExpr('NULL as mergeKey',*[f"updates.{i}" for i in df_source.columns]).union(df_source.selectExpr("concat("+','.join([x for x in pii_cols])+") as mergeKey", "*")))
        
        targetTable.alias("dtName").merge(stagedUpdates.alias("updates"),"concat("+str(column)+") = mergeKey").whenMatchedUpdate(
            condition = _condition,
            set = {
                # Set current to false and endDate to source's effective date."flag_active" : "False",
                "update_date" : f.current_date()
            }
            ).whenNotMatchedInsert(values = insert_dict).execute()
        for i in pii_cols:
            df = df.drop(i).withColumnRenamed("Masked_"+i, i)
        
        return df
        
if __name__ == "__main__":
    config_obj=configuration()
    trans_obj=transformation()
    dataset_name = sys.argv[2]
    print(dataset_name)
    
    if "actives1" in dataset_name.lower() and dataset_name.endswith('.parquet'):
        Data_actives=trans_obj.read_data(config_obj.ingest_actives_source,"parquet") 
        trans_obj.write_data(Data_actives,config_obj.ingest_actives_dest,"parquet")

        Data_raw_actives=trans_obj.read_data(config_obj.ingest_raw_actives_source ,"parquet")

        Data_staging_actives=trans_obj.transformation(Data_raw_actives,config_obj.transformation_cols_actives)
        masked_actives=trans_obj.masking_data(Data_staging_actives,config_obj.masking_col_actives)

        dfActives = trans_obj.scdDataset(masked_actives,config_obj.lookupDataLocation,config_obj.piiColumns,dataset_name)
        trans_obj.writeDataByPartition(dfActives,config_obj.dest_staging_actives,"parquet",config_obj.actives_partition_cols)

    elif "viewership1" in dataset_name.lower() and dataset_name.endswith('.parquet'):
        Data_viewership=trans_obj.read_data(config_obj.ingest_viewership_source,"parquet")
        trans_obj.write_data(Data_viewership,config_obj.ingest_viewership_dest,"parquet")

        Data_raw_viewership=trans_obj.read_data(config_obj.ingest_raw_viewership_source ,"parquet")

        Data_staging_viewership=trans_obj.transformation(Data_raw_viewership,config_obj.transformation_cols_viewership)
        masked_viewership=trans_obj.masking_data(Data_staging_viewership,config_obj.masking_col_viewership)

        dfViewer = trans_obj.scdDataset(masked_viewership,config_obj.lookupDataLocation,config_obj.piiColumns,dataset_name)
        trans_obj.writeDataByPartition(dfViewer,config_obj.dest_staging_viewership,"parquet",config_obj.viewership_partition_cols)

    spark.stop()
    