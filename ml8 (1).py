from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import decimal
import time
import boto3
import requests
import pandas as pd
import pyarrow.parquet as pq
import s3fs

def read_config(**kwargs):
    print("Kwargs",kwargs)
    app_configuration = kwargs['dag_run'].conf['app_configuration']
    print(f"Reading configuration data from {app_configuration}")
    path_lists = app_configuration.replace(":","").split("/")

    s3 = boto3.resource(path_lists[0])
    obj = s3.Object(path_lists[2],"/".join(path_lists[3:]))

    content_file = obj.get()['Body'].read().decode('utf-8')

    jsonData = json.loads(content_file)
    return jsonData

def copy_data(**kwargs):
    
    ti = kwargs['ti']
    jsonData = ti.xcom_pull(task_ids='readConfig')
    dataset_name = kwargs['dag_run'].conf['dataset_name']
    if dataset_name == "actives1.parquet":
        actives_source_path_lists = (jsonData['ingest-Actives']['source']['data-location']).replace(":","").split("/")
        actives_destination_path_lists = (jsonData['ingest-Actives']['destination']['data-location']).replace(":","").split("/")
        print(actives_source_path_lists,actives_destination_path_lists)
        s3_client = boto3.client(actives_source_path_lists[0])
        print(f"Copying data from {actives_source_path_lists[2] + '/'+ '/'.join(actives_source_path_lists[3:])} to {actives_destination_path_lists[2] + '/'+ '/'.join(actives_destination_path_lists[3:])}")
        s3_client.copy_object(
            CopySource = {'Bucket': actives_source_path_lists[2], 'Key': "/".join(actives_source_path_lists[3:])},
            Bucket = actives_destination_path_lists[2],
            Key = "/".join(actives_destination_path_lists[3:])
        )
    else:
        viewership_source_path_lists = (jsonData['ingest-Viewership']['source']['data-location']).replace(":","").split("/")
        viewership_destination_path_lists = (jsonData['ingest-Viewership']['destination']['data-location']).replace(":","").split("/")
        print(viewership_source_path_lists, viewership_destination_path_lists)
        s3_client = boto3.client(viewership_source_path_lists[0])
        print(f"Copying data from {viewership_source_path_lists[2] + '/'+ '/'.join(viewership_source_path_lists[3:])} to {viewership_destination_path_lists[2] + '/'+ '/'.join(viewership_destination_path_lists[3:])}")
        s3_client.copy_object(
            CopySource = {'Bucket': viewership_source_path_lists[2], 'Key': "/".join(viewership_source_path_lists[3:])},
            Bucket = viewership_destination_path_lists[2],
            Key = "/".join(viewership_destination_path_lists[3:])
        ) 
     
region_name = 'ap-southeast-1'

def pre_validation(**kwargs):
    ti = kwargs['ti'] 
    config = ti.xcom_pull(task_ids = 'readConfig')
    dataset_name = kwargs['dag_run'].conf['dataset_name']
    if dataset_name == "actives1.parquet":
        landing_zone_bucket = (config['ingest-Actives']['source']['data-location']).replace(":","").split("/") 
        raw_zone_bucket = (config['ingest-Actives']['destination']['data-location']).replace(":","").split("/")
        print(landing_zone_bucket, raw_zone_bucket)
        s3 = s3fs.S3FileSystem(anon=False)  #uses default credentials
        source_path = config['ingest-Actives']['source']['data-location'] 
        destination_path = config['ingest-Actives']['destination']['data-location']
        #To read data from both landing zone and raw zone buckets;
        df1= pq.ParquetDataset(source_path, filesystem= s3).read_pandas().to_pandas() # or we can use dask dataframes
        df2 = pq.ParquetDataset(destination_path, filesystem= s3).read_pandas().to_pandas()
        #data availability check
        if df2[df2.columns[0]].count()!= 0:
            for raw_columnname in df2.columns:
                if df2[raw_columnname].count() == df1[raw_columnname].count():
                    print('count satisfied!',str(raw_columnname))
                else:
                    raise ValueError("Count mismatch", str(raw_columnname))
        else:
            raise ValueError("No Data Available!")

    else:
        landing_zone_bucket = (config['ingest-Viewership']['source']['data-location']).replace(":","").split("/") 
        raw_zone_bucket = (config['ingest-Viewership']['destination']['data-location']).replace(":","").split("/")
        print(landing_zone_bucket, raw_zone_bucket)
        s3 = s3fs.S3FileSystem(anon=False)  
        source_path = config['ingest-Viewership']['source']['data-location']
        destination_path = config['ingest-Viewership']['destination']['data-location']
        print(source_path , destination_path)
        df1= pq.ParquetDataset(source_path, filesystem= s3).read_pandas().to_pandas() 
        df2 = pq.ParquetDataset(destination_path, filesystem= s3).read_pandas().to_pandas()
        if df2[df2.columns[0]].count()!= 0:
            for raw_columnname in df2.columns:
                if df2[raw_columnname].count() == df1[raw_columnname].count():
                    print('count satisfied!',str(raw_columnname))
                else:
                    raise ValueError("Count mismatch", str(raw_columnname))
        else:
            raise ValueError("No Data Available!")
        
    print("prevalidation Success")

def security_group_id(group_name, region_name):
    ec2 = boto3.client('ec2', region_name=region_name)
    response = ec2.describe_security_groups(GroupNames=[group_name])
    return response['SecurityGroups'][0]['GroupId']

def create_cluster(**kwargs):
    emr = boto3.client('emr', region_name=region_name)
    master_security_group_id = security_group_id(Variable.get("securityGroupName"), region_name=region_name)
    slave_security_group_id = master_security_group_id
    cluster_response = emr.run_job_flow(
            Name= Variable.get("Name"),
            LogUri=Variable.get("LogUri"),
            ReleaseLabel= Variable.get("ReleaseLabel"),
            Instances={
            'InstanceGroups': [
                    {
                        'Name': "Master nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1
                    },
                    {
                        'Name': "Slave nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1
                    }
                ],
            'KeepJobFlowAliveWhenNoSteps': True,
            'Ec2KeyName' : 'harshodaikp',
            #'TerminationProtected':True,
            'EmrManagedMasterSecurityGroup': master_security_group_id,
            'EmrManagedSlaveSecurityGroup': slave_security_group_id

        },
        BootstrapActions= [
                {
                'Name': 'Install boto3',
                'ScriptBootstrapAction': {
                        'Path': Variable.get("dependenciesPath"),
                        }
                }
            ],
        Configurations=[
              {
                'Classification': 'spark',
                'Properties':{
                    "spark.master":"yarn",
                    "spark.submit.deployMode":"cluster",
                    "spark.driver.memory":"4g",
                    "spark.executor.memory":"1g",
                    "spark.driver.cores":'1',
                    "spark.executor.cores":'2',
                    "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version":'2',
                    "spark.hadoop.fs.s3a.block.size":"512M",
                    "maximizeResourceAllocation": "true"
                }
              }
        ],
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        AutoTerminationPolicy = {"IdleTimeout": 1800},
        VisibleToAllUsers=True,
        Applications=[
                { 'Name': 'hadoop' },
                { 'Name': 'spark' },
                { 'Name': 'hive' },
                { 'Name': 'livy' }
            ],
        )
    return cluster_response['JobFlowId'] 


def waiting_for_cluster(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    emr = boto3.client('emr', region_name=region_name)
    emr.get_waiter('cluster_running').wait(ClusterId=cluster_id)

def retrieve_cluster_dns(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    print(cluster_id)
    emr = boto3.client('emr', region_name=region_name)
    response = emr.describe_cluster(ClusterId=cluster_id)
    return response['Cluster']['MasterPublicDnsName']
def datatype_check(data,column_name):
    i1=0
    if(type(column_name[i1][0]) == float): 
        x = str(column_name[i1][0])
        x = x[::-1]
        print(x)
        count = 0
        for i in x:
            if i == '.':
                break
            else:
                count+=1
        if count <=7:
            print("Data is valid")
        else: 
            print("Data is not valid")       
    elif (column_name[i].split(",")[0] == "StringType"):
            if (pd.api.types.is_string_dtype(data[i])):
                print(f"Data Type is valid for {i}")
            else:
                print("Data type not valid")
                
def post_validation(**kwargs):
    ti = kwargs['ti'] 
    config_file = ti.xcom_pull(task_ids = 'readConfig')
    dataset_name = kwargs['dag_run'].conf['dataset_name']
    if dataset_name == "actives1.parquet":
        raw_zone_bucket = config_file['masked-Actives']['source']['data-location']
        staging_zone_bucket = config_file['masked-Actives']['destination']['data-location']
        print(raw_zone_bucket, staging_zone_bucket)
        s3 = s3fs.S3FileSystem(anon=False)  
        df_rawzonedata= pq.ParquetDataset(raw_zone_bucket, filesystem= s3).read_pandas().to_pandas() 
        df_stagingdata = pq.ParquetDataset(staging_zone_bucket, filesystem= s3).read_pandas().to_pandas()
        if df_stagingdata[df_stagingdata.columns[0]].count()!= 0:
            for raw_columnname in df_stagingdata.columns:
                if df_stagingdata[raw_columnname].count() == df_rawzonedata[raw_columnname].count():
                    print('count satisfied!',str(raw_columnname))
                else:
                    raise ValueError("Count mismatch", str(raw_columnname))
        else:
            raise ValueError("No Data Available!")
    else:
        raw_zone_bucket = config_file['masked-Viewership']['source']['data-location']
        staging_zone_bucket = config_file['masked-Viewership']['destination']['data-location']
        print(raw_zone_bucket, staging_zone_bucket)
        s3 = s3fs.S3FileSystem(anon=False)  
        df_rawzonedata= pq.ParquetDataset(raw_zone_bucket, filesystem= s3).read_pandas().to_pandas() #or we can use dask dataframes for larger datasets
        df_stagingdata = pq.ParquetDataset(staging_zone_bucket, filesystem= s3).read_pandas().to_pandas()
        
        if df_stagingdata[df_stagingdata.columns[0]].count() != 0:
            for raw_columnname in df_stagingdata.columns:
                if df_stagingdata[raw_columnname].count() == df_rawzonedata[raw_columnname].count():
                    print('count satisfied!',str(raw_columnname))
                else:
                    raise ValueError("Count mismatch", str(raw_columnname))
        else:
            raise ValueError("No Data Available!")

    print("postvalidation Success")

def livy_submit(**kwargs):
    ti = kwargs['ti']
    app_configuration = kwargs['dag_run'].conf['app_configuration']
    print(app_configuration)
    dataset_name = kwargs['dag_run'].conf['dataset_name']
    print(dataset_name)
    master_dns  = ti.xcom_pull(task_ids='retrieve_cluster_dns')
    print(master_dns)
    spark_code_path = kwargs['dag_run'].conf['spark_job']
    host = 'http://' + master_dns + ':8998'
    data = {"file": spark_code_path, "className": "com.example.SparkApp", "args":[app_configuration,dataset_name]}
    print(data)
    headers = {'Content-Type': 'application/json'}
    r = requests.post(host + '/batches', data=json.dumps(data), headers=headers)
    r.json()

dag = DAG(
    dag_id = 'ML8',
    start_date = datetime(2022,7,26),
    catchup = False,
    schedule_interval = '@once'
)

read_config = PythonOperator(
    task_id = 'readConfig',
    python_callable = read_config,
    dag = dag
)

copy_data = PythonOperator(
    task_id = 'copyData',
    python_callable = copy_data,
    dag = dag
)

pre_validation = PythonOperator(
    task_id = "prevalidation",
    python_callable= pre_validation,
    dag = dag
    
)

create_cluster = PythonOperator(
    task_id="create_cluster",
    python_callable= create_cluster,
    dag=dag
)

waiting_for_cluster = PythonOperator(
    task_id="waiting_for_cluster",
    python_callable= waiting_for_cluster,
    dag=dag
)

retrieve_cluster_dns = PythonOperator(
    task_id="retrieve_cluster_dns",
    python_callable=retrieve_cluster_dns,
    dag=dag
)
post_validation = PythonOperator(
    task_id = "postvalidation",
    python_callable= post_validation,
    dag = dag
    
)

livy_submit = PythonOperator(
    task_id="livy_task",
    python_callable=livy_submit,
    dag=dag
)

read_config >> copy_data >> pre_validation >> create_cluster >> waiting_for_cluster >> retrieve_cluster_dns >> livy_submit >> post_validation
