import os
import sys
from pyspark import SparkContext
import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import SparkSession
# %matplotlib inline
%pylab inline
matplotlib.rcParams['figure.figsize'] = (10, 6)

NationalRate = "/Users/pushparajparab/Desktop/Hospital_Revised_Flatfiles/HCAHPS__Hospital.csv"
df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(NationalRate)

df.registerTempTable('Mytable')


query = " SELECT Hospital_Name,HCAHPS_Answer_Description,Patient_Survey_Star_Rating"
query += " FROM MyTable M"
query += " WHERE HCAHPS_Answer_Description LIKE '%star rating%'"
query += " AND Patient_Survey_Star_Rating != 'Not Available'"
query += " order by M.Hospital_Name,HCAHPS_Answer_Description"
distint = spark.sql(query)
# distint.show(20, False)
# Complications = "/Users/pushparajparab/Desktop/Hospital_Revised_Flatfiles/Complications_Hospital.csv";
# df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(Complications)
# df.registerTempTable('Data')


query = " SELECT HCAHPS_Answer_Description,Patient_Survey_Star_Rating,count(Patient_Survey_Star_Rating) as count"
query += " FROM MyTable M"
query += " WHERE HCAHPS_Answer_Description LIKE '%star rating%'"
query += " AND HCAHPS_Answer_Description NOT LIKE '%Recommend%'"
query += " AND HCAHPS_Answer_Description NOT LIKE '%Summary%'"
query += " AND HCAHPS_Answer_Description NOT LIKE '%Overall%'"
query += " AND Patient_Survey_Star_Rating != 'Not Available'"
query += " group by M.Patient_Survey_Star_Rating,HCAHPS_Answer_Description"
query += " order by count(Patient_Survey_Star_Rating) desc"
distint = spark.sql(query)
distint.show(20, False)

_size =[]
_text = []
_x=[]
_y=[]
_dataList = distint.collect()
for i in range(len(_dataList)):
    _x.append(_dataList[i][0])
    _y.append(_dataList[i][1])
    _size.append(_dataList[i][2]*0.08)
    _text.append('Count: '+ str(_dataList[i][2]))