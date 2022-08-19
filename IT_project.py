#!/usr/bin/env python
# coding: utf-8


import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession


spark=SparkSession.builder.appName('IT project').getOrCreate()


df1=spark.read.csv('/home/alex/Desktop/python_working/IT2019.csv',header=True,inferSchema=True)
df2=spark.read.csv('/home/alex/Desktop/python_working/IT2020.csv',header=True,inferSchema=True)
df3=spark.read.csv('/home/alex/Desktop/python_working/IT2018.csv',header=True,inferSchema=True)


# to Pandas for ease of visualization for column filtering/creating between 3 datasets

pdf19 = df1.toPandas()
pdf20 = df2.toPandas()
pdf18 = df3.toPandas()



new18 = pdf18.filter(['Timestamp', 'Age','Gender','City','Position','Years of experience',
                      'Your level','Main language at work','Company size','Company type',
                      'Current Salary','Salary one year ago'], axis=1)


#filtering columns

new19 = pdf19.filter(['Zeitstempel', 'Age','Gender','City','Position (without seniority)',
                      'Years of experience','Seniority level','Main language at work',
                      'Company size','Company type',
                      'Yearly brutto salary (without bonus and stocks)',
                      'Yearly bonus',
                      'Yearly stocks',
                      'Yearly brutto salary (without bonus and stocks) one year ago. Only answer if staying in same country',
                      'Yearly bonus one year ago. Only answer if staying in same country',
                      'Yearly stocks one year ago. Only answer if staying in same country',
                      'Number of vacation days',
                      'Your main technology / programming language'], axis=1)



#filling na's to 0 for numeric columns

new19['Yearly bonus'] = new19['Yearly bonus'].fillna(0)
new19['Yearly stocks'] = new19['Yearly stocks'].fillna(0)
new19['Yearly bonus one year ago. Only answer if staying in same country'] = new19['Yearly bonus one year ago. Only answer if staying in same country'].fillna(0)
new19['Yearly stocks one year ago. Only answer if staying in same country'] = new19['Yearly stocks one year ago. Only answer if staying in same country'].fillna(0)



# creating new column to include salary, stocks, and bonus as one value

new19['Stocks and Bonus'] = new19['Yearly bonus'] + new19['Yearly stocks']
new19['Stocks and Bonus last year'] = new19['Yearly bonus one year ago. Only answer if staying in same country'] + new19['Yearly stocks one year ago. Only answer if staying in same country']
new19['Salary and bonus and stocks'] = new19['Yearly brutto salary (without bonus and stocks)'] + new19['Stocks and Bonus'] 
new19['Salary and bonus and stocks last year'] = new19['Yearly brutto salary (without bonus and stocks) one year ago. Only answer if staying in same country'] + new19['Stocks and Bonus last year']


new20 = pdf20.filter(['Timestamp', 'Age','Gender','City','Position ',
                       'Total years of experience','Seniority level','Main language at work',
                      'Company size','Company type',
                      'Yearly brutto salary (without bonus and stocks) in EUR',
                      'Yearly bonus + stocks in EUR',
                      'Annual brutto salary (without bonus and stocks) one year ago. Only answer if staying in the same country',
                       'Annual bonus+stocks one year ago. Only answer if staying in same country',
                      'Number of vacation days',
                      'Your main technology / programming language'], axis=1)


import pandas as pd


new20['Yearly bonus + stocks in EUR'] = pd.to_numeric(new20['Yearly bonus + stocks in EUR'], errors='coerce')
new20['Annual bonus+stocks one year ago. Only answer if staying in same country'] = pd.to_numeric(new20['Annual bonus+stocks one year ago. Only answer if staying in same country'], errors='coerce')




new20['Yearly bonus + stocks in EUR'] = new20['Yearly bonus + stocks in EUR'].fillna(0)
new20['Annual bonus+stocks one year ago. Only answer if staying in same country'] = new20['Annual bonus+stocks one year ago. Only answer if staying in same country'].fillna(0)


new20['Salary and bonus and stocks'] = new20['Yearly brutto salary (without bonus and stocks) in EUR'] + new20['Yearly bonus + stocks in EUR']
new20['Salary and bonus and stocks last year'] = new20['Annual brutto salary (without bonus and stocks) one year ago. Only answer if staying in the same country'] + new20['Annual bonus+stocks one year ago. Only answer if staying in same country']



# create null columns so df can be merged with 2019 and 2020 df

new18['Salary inclusive'] = new18['Current Salary']
new18['Salary inclusive one year ago'] = "None"
new18['Number of vacation days'] = "None"
new18['Your main technology / programming language'] = "None"


new18.columns



# build 2019 df in correct order

new19 = new19.filter(['Zeitstempel', 'Age','Gender','City','Position (without seniority)',
                      'Years of experience','Seniority level','Main language at work',
                      'Company size','Company type',
                      'Yearly brutto salary (without bonus and stocks)',
                      'Yearly brutto salary (without bonus and stocks) one year ago. Only answer if staying in same country',
                      'Salary and bonus and stocks',
                      'Salary and bonus and stocks last year',
                      'Number of vacation days',
                      'Your main technology / programming language'], axis=1)


new19.columns


# build 2020 df in correct order

new20 = new20.filter(['Timestamp', 'Age','Gender','City', 'Position ',
                      'Total years of experience','Seniority level','Main language at work',
                      'Company size','Company type',
                       'Yearly brutto salary (without bonus and stocks) in EUR',
                       'Annual brutto salary (without bonus and stocks) one year ago. Only answer if staying in the same country',
                      'Salary and bonus and stocks',
                      'Salary and bonus and stocks last year',
                    'Number of vacation days',
                      'Your main technology / programming language',], axis=1)



new20.columns


# pandas df to Spark df for merging/cleaning/analysis

spark18=spark.createDataFrame(new18)
spark19=spark.createDataFrame(new19)
spark20=spark.createDataFrame(new20)



from functools import reduce
from pyspark.sql import DataFrame
df =reduce(DataFrame.unionAll,[spark18, spark19, spark20])
df.printSchema()


from pyspark.sql.types import IntegerType,BooleanType,DoubleType


# cast columns to intended datatypes

df = df.withColumn("Age", df.Age.cast(IntegerType()))
df = df.withColumn("Years of experience", df['Years of experience'].cast(IntegerType()))
df = df.withColumn("Salary inclusive", df['Salary inclusive'].cast(DoubleType()))
df = df.withColumn("Salary inclusive one year ago", df['Salary inclusive one year ago'].cast(DoubleType()))
df = df.withColumn("Number of vacation days", df['Number of vacation days'].cast(IntegerType()))

from pyspark.sql.functions import regexp_replace, to_date

df = df.withColumn('Timestamp', df.Timestamp.substr(1,10)) 
df = df.withColumn('Timestamp', regexp_replace(df['Timestamp'],'/', '.'))
df = df.withColumn('Timestamp',to_date(df['Timestamp'],"dd.MM.yyyy"))


from pyspark.sql.functions import  count, col, when

df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()


df.groupBy('Gender').count().show()


# Cleaning gender column

df = df.withColumn('Gender', regexp_replace(df.Gender,'Female','F'))
df = df.withColumn('Gender', regexp_replace(df.Gender,'Male','M'))
df = df.withColumn('Gender', regexp_replace(df.Gender,'M','T'))

# drop null and "diverse" values in Gender to just have T (True:Male) and F (False:Female) for Boolean

df = df.where(df.Gender != 'Diverse')
df = df.dropna(subset='Gender')
df = df.withColumn("Gender", df.Gender.cast(BooleanType()))


df.groupBy('Gender').count().show()


# Cleaning Company size column

df.groupBy('Company size').count().show()

df = df.withColumn('Company Size', regexp_replace(df['Company Size'],'11-50','10-50'))
df = df.withColumn('Company Size', regexp_replace(df['Company Size'],'50-100','51-100'))
df = df.withColumn('Company Size', regexp_replace(df['Company Size'],'100-1000','101-1000'))

df.groupBy('Company size').count().show()

#Cleaning Current Salary column
# describe, drop nan values (5), remove extremely large outliers (salary>500,000)

from pyspark.sql.functions import isnan, when, count, col

df.select([count(when(isnan('Current Salary'),True))]).show() # 5 items without salary
df = df.dropna(subset='Current Salary')
df.select([count(when(isnan('Current Salary'),True))]).show()

df.filter(df['Current Salary'] >= 500000).show() # 2 outliers
df = df.where(df['Current Salary'] <= 500000)

# description after missing and outliers removed (7 rows)
df.describe(['Current Salary']).show()

df.filter(df['Salary inclusive'] >= 500000).show() # 3 outliers
df = df.where(df['Salary inclusive'] <= 500000)

df.describe(['Years of experience']).show()


df.filter(df['Years of experience'] >= 50).show()
df = df.where(df['Years of experience'] <= 50)


df.describe(['Age']).show()
# average age is ~ 30-31
df.groupBy('Age').count().filter(df['Age'] < 18).show()
# 199 rows with age O

#replace 0's with average age of 31
df = df.na.replace(0, 31)

df.groupBy('Age').count().filter(df['Age'] < 18).show()
# 199 rows with age O

df.describe(['Age']).show()




# Cleaning Programming language column

df = df.withColumnRenamed('Your main technology / programming language', 'Programming language')

df.groupBy('Programming language').count().show()

df.groupBy('Programming language').count().filter(df['Programming language'].contains('Py')).show()
df.groupBy('Programming language').count().filter(df['Programming language'].contains('py')).show()

# Creating a new column for cleaned programming language
df = df.withColumn('Prog Language 2', df['Programming language'])

# Grouping all python-y columns under the same label "Python" in "Prog Langauge 2" column

#df = df.withColumn('Prog Language 2', regexp_replace(df['Prog Language 2'],'Python ', 'Python'))
#df = df.withColumn('Prog Language 2', regexp_replace(df['Prog Language 2'],'Ml/Python', 'Python'))
#df = df.withColumn('Prog Language 2', regexp_replace(df['Prog Language 2'],'python ', 'Python'))
#df = df.withColumn('Prog Language 2', regexp_replace(df['Prog Language 2'],'python', 'Python'))
#df = df.withColumn('Prog Language 2', regexp_replace(df['Prog Language 2'],'Pyrhon', 'Python'))
#df = df.withColumn('Prog Language 2', regexp_replace(df['Prog Language 2'],'pythin', 'Python'))


df.groupBy('Prog Language 2').count().filter(df['Prog Language 2'].contains('Py')).show()
df.groupBy('Prog Language 2').count().filter(df['Prog Language 2'].contains('py')).show()


# Javascript / Typescript

df.groupBy('Prog Language 2').count().filter(df['Prog Language 2'].contains('script')).show()
df.groupBy('Prog Language 2').count().filter(df['Prog Language 2'].contains('Script')).show()

#df = df.replace(['Javascript', 'Typescript','JavaScript','javascript','TypeScript','typescript','JavaScript ',
                #'TypeScript/JavaScript','Javascript ','JS'],
                
                #['Javascript / Typescript','Javascript / Typescript','Javascript / Typescript',
                #'Javascript / Typescript','Javascript / Typescript','Javascript / Typescript',
                 #'Javascript / Typescript','Javascript / Typescript','Javascript / Typescript','Javascript / Typescript'],'Prog Language 2')


# C/C++

#df.groupBy('Prog Language 2').count().filter(df['Prog Language 2'].contains('C')).show()
#df.groupBy('Prog Language 2').count().filter(df['Prog Language 2'].contains('C++')).show()

#df = df.replace(['C++','C'],
                
                #['C/C++','C/C++'],'Prog Language 2')



# Java, .NET, Go, PHP, SAP / ABAP, SQL


#df = df.replace(['java','JAVA'],
                
                #['Java','Java'],'Prog Language 2')

#df = df.replace(['.net','.Net'],
                
                #['.NET','.NET'],'Prog Language 2')

#df = df.replace(['go','golang','Golang'],
                
                #['Go','Go','Go'],'Prog Language 2')

#df = df.replace(['php','Php'],
                
                #['PHP','PHP'],'Prog Language 2')

#df = df.replace(['SAP','SAP ABAP','SAP BW / ABAP'],
                
                #['SAP / ABAP','SAP / ABAP','SAP / ABAP'],'Prog Language 2')

#df = df.replace(['Sql','sql'],
                
                #['SQL','SQL'],'Prog Language 2')


#df.repartition(1).write.csv('/home/alex/Desktop/python_working/rep')


#to pandas to save as one CSV file for Tableau

pdf = df.toPandas()
pdf.to_csv('/home/alex/Desktop/python_working/final.csv')


# df.printSchema()


# df.describe(['Years of experience']).show()


#minimize Company type for Regression 

df.groupBy('Company type').count().show()


df = df.replace(['Agency'],['Consulting / Agency'],'Company type')
df = df.replace(['Outsource'],['Bodyshop / Outsource'],'Company type')
df = df.filter(df['Company type'].isin(['Bank','Bodyshop / Outsource','Consulting / Agency','Product','Startup','University']))
df.groupBy('Company type').count().show()


#minimize levels for Regression 

df.groupBy('Your level').count().show()
df = df.filter(df['Your level'].isin(['Senior','Middle','Junior','Principal','Head','Lead']))
df.groupBy('Your level').count().show()


df.approxQuantile(["Salary inclusive"], [0.5],.01)

#create boolean column based on median salary
df = df.withColumn("Sal_high",when(col("Salary inclusive") >= 70000,1).otherwise(0)) 



df.groupBy('Sal_high').count().show()


df = df.withColumn("Sal_high", df['Sal_high'].cast(DoubleType()))


#for gender classification

df = df.withColumn("Gender", df['Gender'].cast(DoubleType()))


df.groupBy('Gender').count().show()




# Machine Learning


df.printSchema()


from pyspark.sql.functions import isnan, when, count, col


df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()


#USE ONLY FOR GENDER CLASSIFICATION

catcolslong = ['Position','Prog Language 2']
catcols = ['Your level','Company Size','Company type']
numcols = ['Age','Years of experience','Salary inclusive']



#USE ONLY FOR GENDER CLASSIFICATION: fill null values with mean for numericals 

from pyspark.ml.feature import Imputer

df.withColumn("Number of vacation days",df["Number of vacation days"].cast('double'))
df.withColumn("Years of experience",df["Years of experience"].cast('double'))
df.withColumn("Age",df['Age'].cast('double'))

Impcols = ['Salary inclusive','Age','Years of experience','Number of vacation days','Salary inclusive one year ago']
imputer = Imputer(inputCols=Impcols, outputCols=Impcols)
model=imputer.fit(df)
df = model.transform(df)




# SALARY PREDICTION COLS

catcolslong = ['Position','Prog Language 2']
catcols = ['Your level','Company Size','Company type']
numcols = ['Gender','Age','Years of experience']




# FOR SALARY PREDICTION: fill null values with mean for numericals 

from pyspark.ml.feature import Imputer

df.withColumn("Number of vacation days",df["Number of vacation days"].cast('double'))
df.withColumn("Years of experience",df["Years of experience"].cast('double'))
df.withColumn("Age",df['Age'].cast('double'))

Impcols = ['Age','Years of experience','Number of vacation days','Salary inclusive one year ago']
imputer = Imputer(inputCols=Impcols, outputCols=Impcols).setStrategy("median")
model=imputer.fit(df)
df = model.transform(df)


# fill null values with string in categorical cols

df = df.na.fill(value='None',subset=['City','Position','Main language at work','Company size','Programming language','Prog Language 2'])


# convert categorical columns to string indexes

from pyspark.ml.feature import StringIndexer
Istages=[]

for col in catcols:
    indexer = StringIndexer(inputCol=col, outputCol = col+'Index', handleInvalid='skip')
    Istages +=[indexer]



from pyspark.ml import Pipeline

partPipeline = Pipeline().setStages(Istages)
pipModel = partPipeline.fit(df)
df = pipModel.transform(df)


from pyspark.ml.feature import OneHotEncoder, VectorAssembler

stages =[]
for col in catcols:
    encoder = OneHotEncoder(inputCols=[col+"Index"], outputCols=[col+"classVec"])
    stages += [encoder]
    

# assemble feature vectors (converted categorical cols and numerical)

assemblerIn = [c+"classVec" for c in catcols] + numcols
print(assemblerIn)


assembler=VectorAssembler(inputCols=assemblerIn, outputCol='features')

stages +=[assembler]



partPipeline = Pipeline().setStages(stages)
pipModel = partPipeline.fit(df)
df = pipModel.transform(df)



# scaling

from pyspark.ml.feature import StandardScaler
sScaler = StandardScaler(withStd=True).setInputCol('features').setOutputCol('scaled_features')
df = sScaler.fit(df).transform(df)
df.select('features','scaled_features').show()

 # training, testing

train, test = df.randomSplit([.8,.2],seed=1)


# GENDER CLASSIFICATION

from pyspark.ml.classification import DecisionTreeClassifier

dt = DecisionTreeClassifier(featuresCol = 'scaled_features', labelCol = 'Gender', maxDepth = 10)
dtModel = dt.fit(train)
predictions = dtModel.transform(test)




from pyspark.ml.evaluation import BinaryClassificationEvaluator

evaluator = BinaryClassificationEvaluator().setLabelCol("Gender")

print("Test Area Under ROC: " + str(evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})))







# predict if salary over 70000 (median) - Decision Tree Classification: ROC .70

from pyspark.ml.classification import DecisionTreeClassifier

dt = DecisionTreeClassifier(featuresCol = 'scaled_features', labelCol = 'Sal_high', maxDepth = 5)
dtModel = dt.fit(train)
predictions = dtModel.transform(test)



from pyspark.ml.evaluation import BinaryClassificationEvaluator

evaluator = BinaryClassificationEvaluator().setLabelCol("Sal_high")

print("Test Area Under ROC: " + str(evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})))




#Linear regression take 2 - r2: .25

from pyspark.ml.regression import LinearRegression

lr = LinearRegression(featuresCol = 'scaled_features', labelCol='Salary inclusive', maxIter=10, regParam=0.3, elasticNetParam=0.8)
lr_model = lr.fit(train)
print("Coefficients: " + str(lr_model.coefficients))
print("Intercept: " + str(lr_model.intercept))

trainingSummary = lr_model.summary
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)



#Decision tree predict salary - .2 r2

from pyspark.ml.regression import DecisionTreeRegressor

dt = DecisionTreeRegressor().setLabelCol("Salary inclusive").setFeaturesCol("scaled_features")
model = dt.fit(train)
test_dt = model.transform(test)
test_dt.show(truncate=False)

from pyspark.ml.evaluation import RegressionEvaluator

evaluator = RegressionEvaluator().setLabelCol("Salary inclusive")

print(evaluator.evaluate(test_dt, {evaluator.metricName:"r2"}))






