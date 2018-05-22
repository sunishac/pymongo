import os
import sys
from pyspark import SparkContext
from matplotlib.pylab import *
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession


#df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('C:\Users\chala\Desktop\twData.csv')
#print(type(df))
#df.registerTempTable('Table1')
#sel = spark.sql("SELECT id FROM Table1")
#sel.show()

#data = sel.collect()
#x_axis=[]
#y_axis=[]

#for i in range(len(data)):
#	x_axis.append(data[i][0])
#	y_axis.append(data[i][0])

#pos = np.arange(len(x_axis))
print("pyspark ")


