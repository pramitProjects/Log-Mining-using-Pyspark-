from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import matplotlib 
matplotlib.use('Agg') # Must be before importing matplotlib.pyplot or pylab! 
import matplotlib.pyplot as plt
from pyspark.sql.functions import substring
from pyspark.sql.functions import col
import numpy as np


spark = SparkSession.builder \
        .master("local[4]") \
        .appName("Lab 2 Exercise") \
        .config("spark.local.dir","/fastdata/acq21ps") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN") 

logFile = spark.read.text("../Data/NASA_access_log_Jul95.gz").cache()  # add it to cache, so it can be used in the following steps efficiently      
logFile.show(20, False)

# Forming the five different columns using regexp_extract
data = logFile.withColumn('host_name', F.regexp_extract('value', '^(.*) - -.*', 1)) \
                .withColumn('timestamp', F.regexp_extract('value', '.* - - \[(.*)\].*',1)) \
                .withColumn('request', F.regexp_extract('value', '.*\"(.*)\".*',1)) \
                .withColumn('HTTP reply code', F.split('value', ' ').getItem(F.size(F.split('value', ' ')) -2).cast("int")) \
                .withColumn('bytes in the reply', F.split('value', ' ').getItem(F.size(F.split('value', ' ')) - 1).cast("int")).drop("value").cache()


               
data.show(20,False)

# getting the day of month from the timestamp column and casting it into int
data = data.withColumn('day_of_month', substring('timestamp',1,2))
data = data.withColumn('day_of_month', col('day_of_month').cast("int"))

print('============ Question A ===============')

print('\n')
# Getting the total requests from Germany, Canada, Singapore by finding the host names that endswith .de,.ca and .sg respectively             
GermanHostsCount = data.filter(col('host_name').endswith('.de')).count()
CanadaHostsCount = data.filter(col('host_name').endswith('.ca')).count()
SingaporeHostsCount = data.filter(col('host_name').endswith('.sg')).count()

print('The total requests for all hosts in Germany is ', GermanHostsCount)
print('The total requests for all hosts in Canada is ', CanadaHostsCount)
print('The total requests for all hosts in Singapore is ', SingaporeHostsCount)


# plotting the total requests by country

countries = ["Germany","Canada","Singapore"]
requests = [GermanHostsCount,CanadaHostsCount,SingaporeHostsCount]
X_axis = np.arange(len(countries))
plt.bar(X_axis-0.2, requests, 0.4)
plt.xticks(X_axis, countries)
plt.xlabel("Country")
plt.ylabel("Total Request")
plt.title("Total Requests per Country")
plt.legend()
plt.savefig("../Output/Requests_Per_Country.png")

print('\n')
print('\n')

print("================= Question B ================")

# START UNCOMMENTING FROM BELOW
print('\n')

# Getting the unique german host names from german host dataframe
GermanHosts = data.filter(col('host_name').endswith('.de'))

UniqueGermanHostsCount = GermanHosts.select('host_name').distinct().count()
print(f"There are {UniqueGermanHostsCount} unique German hosts")

print('\n')

# getting the unique canada host names from the canada host dataframe
CanadaHosts = data.filter(col('host_name').endswith('.ca'))
UniqueCanadaHostsCount = CanadaHosts.select('host_name').distinct().count()
print(f"There are {UniqueCanadaHostsCount} unique Canada hosts")

print('\n')

# getting the unique singapore host names from the singapore host dataframe
SingaporeHosts = data.filter(col('host_name').endswith('.sg'))
UniqueSingaporeHostsCount = SingaporeHosts.select('host_name').distinct().count()
print(f"There are {UniqueSingaporeHostsCount} unique Singapore hosts")

print('\n')

# finding the top 9 most frequent german hosts
GermanHostsCount_df = GermanHosts.groupBy('host_name').count()
Top9GermanHosts_df = GermanHostsCount_df.orderBy(col('count').desc()).limit(9)
print('The Top 9 German Hosts are: \n')
Top9GermanHostList = Top9GermanHosts_df.select('host_name').rdd.flatMap(lambda x: x).collect()
print(Top9GermanHostList)

print('\n')
# finding the top 9 most frequent canada hosts 

CanadaHostsCount_df = CanadaHosts.groupBy('host_name').count()
Top9CanadaHosts_df = CanadaHostsCount_df.orderBy(col('count').desc()).limit(9)
print('The Top 9 Canada Hosts are: \n')
Top9CanadaHostList = Top9CanadaHosts_df.select('host_name').rdd.flatMap(lambda x: x).collect()
print(Top9CanadaHostList)

print('\n')
# finding the most top 9 frequent singapore hosts

SingaporeHostsCount_df = SingaporeHosts.groupBy('host_name').count()
Top9SingaporeHosts_df = SingaporeHostsCount_df.orderBy(col('count').desc()).limit(9)
print('The Top 9 Singapore Hosts are: \n')
Top9SingaporeHostList = Top9SingaporeHosts_df.select('host_name').rdd.flatMap(lambda x: x).collect()
print(Top9SingaporeHostList)

print('\n')
print('\n')

print('============= Question C ===============')

print ('\n')
print('The Three Graphs for Top 9 hosts from Germany, Canada and Singapore are Shown in the figure Q1_figC_Germany.png, Q1_figC_Canada.png and Q1_figC_Singapore.png  ')

# FOR GERMANY

TotalTop9GermanHostsCount = Top9GermanHosts_df.selectExpr("sum(count)").collect()[0][0] # computing the total request made by the top 9 most frequent hosts
RemainingGermanHostsCount = GermanHostsCount - TotalTop9GermanHostsCount #computing the total requests made by the remaining hosts (i.e other than the top 9 hosts)
#Getting the percentage of request(with respect to total request made in that country) made by each of the top 9 hosts 
Top9GermanHostsWithPercentage_df = Top9GermanHosts_df.withColumn("percentage",(col("count")/GermanHostsCount)*100)
#Getting the percentage of request made by the remaining hosts
RemainingGermanHostsPercentage = (RemainingGermanHostsCount/GermanHostsCount)*100
final_df = Top9GermanHostsWithPercentage_df
y = final_df.select('count').rdd.flatMap(lambda x: x).collect()
y.append(RemainingGermanHostsCount)
x = final_df.select('host_name').rdd.flatMap(lambda x: x).collect()
x.append('Remaining German Host Requests')
percent = final_df.select('percentage').rdd.flatMap(lambda x: x).collect()
percent.append(RemainingGermanHostsPercentage)
colors = ['yellowgreen','red','gold','lightskyblue','white','lightcoral','blue','pink', 'darkgreen','yellow','grey']
#Representing the above 10 entities in a pie plot
plt.clf()
plt.figure(figsize=(10, 10))
patches, texts = plt.pie(y, colors=colors, startangle=90, radius=1.2)
labels = ['{0} - {1:1.2f} %'.format(i,j) for i,j in zip(x, percent)]
sort_legend = True
if sort_legend:
    patches, labels, dummy =  zip(*sorted(zip(patches, labels, y),
                                          key=lambda x: x[2],
                                          reverse=True))

plt.legend(patches, labels, loc='center left', bbox_to_anchor=(-0.1, 1.),
            fontsize=8)
plt.savefig("../Output/Top9_Requests_Germany.png",bbox_inches='tight')


# FOR CANADA

TotalTop9CanadaHostsCount = Top9CanadaHosts_df.selectExpr("sum(count)").collect()[0][0] # computing the total request made by the top 9 most frequent hosts
RemainingCanadaHostsCount = CanadaHostsCount - TotalTop9CanadaHostsCount #computing the total requests made by the remaining hosts (i.e other than the top 9 hosts)
#Getting the percentage of request(with respect to total request made in that country) made by each of the top 9 hosts 
Top9CanadaHostsWithPercentage_df = Top9CanadaHosts_df.withColumn("percentage",(col("count")/CanadaHostsCount)*100)
#Getting the percentage of request made by the remaining hosts
RemainingCanadaHostsPercentage = (RemainingCanadaHostsCount/CanadaHostsCount)*100
final_df = Top9CanadaHostsWithPercentage_df
y = final_df.select('count').rdd.flatMap(lambda x: x).collect()
y.append(RemainingCanadaHostsCount)
x = final_df.select('host_name').rdd.flatMap(lambda x: x).collect()
x.append('Remaining Canada Host Requests')
percent = final_df.select('percentage').rdd.flatMap(lambda x: x).collect()
percent.append(RemainingCanadaHostsPercentage)
colors = ['yellowgreen','red','gold','lightskyblue','white','lightcoral','blue','pink', 'darkgreen','yellow','grey']
#Representing the above 10 entities in a pie plot
plt.clf()
plt.figure(figsize=(10, 10))
patches, texts = plt.pie(y, colors=colors, startangle=90, radius=1.2)
labels = ['{0} - {1:1.2f} %'.format(i,j) for i,j in zip(x, percent)]
sort_legend = True
if sort_legend:
    patches, labels, dummy =  zip(*sorted(zip(patches, labels, y),
                                          key=lambda x: x[2],
                                          reverse=True))

plt.legend(patches, labels, loc='center left', bbox_to_anchor=(-0.1, 1.),
            fontsize=8)
plt.savefig("../Output/Top9_Requests_Canada.png",bbox_inches='tight')

#  FOR SINGAPORE

TotalTop9SingaporeHostsCount = Top9SingaporeHosts_df.selectExpr("sum(count)").collect()[0][0] # computing the total request made by the top 9 most frequent hosts
RemainingSingaporeHostsCount = SingaporeHostsCount - TotalTop9SingaporeHostsCount #computing the total requests made by the remaining hosts (i.e other than the top 9 hosts)
#Getting the percentage of request(with respect to total request made in that country) made by each of the top 9 hosts 
Top9SingaporeHostsWithPercentage_df = Top9SingaporeHosts_df.withColumn("percentage",(col("count")/SingaporeHostsCount)*100)
#Getting the percentage of request made by the remaining hosts
RemainingSingaporeHostsPercentage = (RemainingSingaporeHostsCount/SingaporeHostsCount)*100
final_df = Top9SingaporeHostsWithPercentage_df
y = final_df.select('count').rdd.flatMap(lambda x: x).collect()
y.append(RemainingSingaporeHostsCount)
x = final_df.select('host_name').rdd.flatMap(lambda x: x).collect()
x.append('Remaining Singapore Host Requests')
percent = final_df.select('percentage').rdd.flatMap(lambda x: x).collect()
percent.append(RemainingSingaporeHostsPercentage)
colors = ['yellowgreen','red','gold','lightskyblue','white','lightcoral','blue','pink', 'darkgreen','yellow','grey']
#Representing the above 10 entities in a pie plot
plt.clf()
plt.figure(figsize=(10, 10))
patches, texts = plt.pie(y, colors=colors, startangle=90, radius=1.2)
labels = ['{0} - {1:1.2f} %'.format(i,j) for i,j in zip(x, percent)]
sort_legend = True
if sort_legend:
    patches, labels, dummy =  zip(*sorted(zip(patches, labels, y),
                                          key=lambda x: x[2],
                                          reverse=True))

plt.legend(patches, labels, loc='center left', bbox_to_anchor=(-0.1, 1.),
            fontsize=8)
plt.savefig("../Output/Top9_Requets_Singapore.png",bbox_inches='tight')

print('\n')
print('\n')

# STOP UNCOMMENTING ABOVE
print("====================== Question D ========================")
# ========================= GERMANY =================================
GermanHosts = data.filter(col('host_name').endswith('.de'))
GermanHostsCount_df = GermanHosts.groupBy('host_name').count()
Top9GermanHosts_df = GermanHostsCount_df.orderBy(col('count').desc()).limit(9)
HighestGermanHost = Top9GermanHosts_df.first()['host_name']
HighestGermanHost_df = data.filter(data["host_name"] == HighestGermanHost) #Getting the most frequent host in that country
#HighestGermanHost_df.show()
# Extract day of month and hour
df = HighestGermanHost_df.withColumn('day_of_month', substring('timestamp',1,2))
df = df.withColumn('hourly', substring('timestamp', 13, 2))
df.show()
df = df.select('host_name','day_of_month','hourly') #forming the dataframe for the highest host with the corresponding day and hour
df = df.withColumn('day_of_month', col('day_of_month').cast("int"))
df = df.withColumn('hourly', col('hourly').cast("int"))

print("Highest German Host dataframe\n")
df.show()

row1 = data.agg({"day_of_month": "min"}).collect()[0] #getting the minimum day in the log file
row2 = data.agg({"day_of_month": "max"}).collect()[0] #getting the maximum day in the log file
min_day = row1["min(day_of_month)"]
max_day = row2["max(day_of_month)"]

# ================================ CANADA ===================================
CanadaHosts = data.filter(col('host_name').endswith('.ca'))
CanadaHostsCount_df = CanadaHosts.groupBy('host_name').count()
Top9CanadaHosts_df = CanadaHostsCount_df.orderBy(col('count').desc()).limit(9)
HighestCanadaHost = Top9CanadaHosts_df.first()['host_name']
HighestCanadaHost_df = data.filter(data["host_name"] == HighestCanadaHost) #Getting the most frequent host in that country

# Convert timestamp to datetime and extract day of month and hour
df1 = HighestCanadaHost_df.withColumn('day_of_month', substring('timestamp',1,2))
df1 = df1.withColumn('hour_of_day', substring('timestamp', 13, 2))

# Select relevant columns
df1 = df1.select('host_name', 'day_of_month', 'hour_of_day')  #forming the dataframe for the highest host with the corresponding day and hour
df1 = df1.withColumn('day_of_month', col('day_of_month').cast("int"))
df1 = df1.withColumn('hour_of_day', col('hour_of_day').cast("int"))

# ============================ SINGAPORE ======================================

SingaporeHosts = data.filter(col('host_name').endswith('.sg'))
SingaporeHostsCount_df = SingaporeHosts.groupBy('host_name').count()
Top9SingaporeHosts_df = SingaporeHostsCount_df.orderBy(col('count').desc()).limit(9)
HighestSingaporeHost = Top9SingaporeHosts_df.first()['host_name']
HighestSingaporeHost_df = data.filter(data["host_name"] == HighestSingaporeHost)  #Getting the most frequent host in that country
#HighestGermanHost_df.show()
df2 = HighestSingaporeHost_df.withColumn('day_of_month', substring('timestamp',1,2))
# Extract day of month and hour
df2 = df2.withColumn('visit_hour',substring('timestamp', 13, 2))
#df.show()
df2 = df2.select('host_name','day_of_month','visit_hour')  #forming the dataframe for the highest host with the corresponding day and hour
df2 = df2.withColumn('day_of_month', col('day_of_month').cast("int"))
df2 = df2.withColumn('visit_hour', col('visit_hour').cast("int"))
#df.show()


# -------------------- GRAPH GERMANY ----------------------------
plt.clf()
x_axis = np.arange(min_day, max_day+1)
y_axis = np.arange(0, 24)
y_axis = list(y_axis)
y_axis.reverse()
y_axis = np.array(y_axis)
# creating a 2D array of zeros for the heatmap
heatmap = np.zeros((24, max_day-min_day+1))
#Populate the heatmap with the counts
rows = df.take(df.count()) # getting the rows from the newly formed dataframe of the highest host
for row in rows:
    day = row['day_of_month']
    hour = row['hourly']
    count = 1  
    heatmap[23-hour, day-min_day] += count
# creating the heatmap plot
fig, ax = plt.subplots(figsize=(10, 8))
im = ax.imshow(heatmap, cmap='Reds')
#Setting the x-axis and y-axis labels
ax.set_xticks(np.arange(len(x_axis)))
ax.set_yticks(np.arange(len(y_axis)))
ax.set_xticklabels(x_axis)
ax.set_yticklabels(y_axis)
#Setting the x-axis and y-axis labels title
ax.set_xlabel('Day of Month')
ax.set_ylabel('Hour of Visit')
ax.set_title('Heatmap of Highest German Host Activity')
#Adding a colorbar to the graph
cbar = ax.figure.colorbar(im, ax=ax, shrink=1)
plt.setp(ax.get_xticklabels(), rotation=45, ha="right",
          rotation_mode="anchor")
#creating text annotations in the graph
for i in range(len(y_axis)):
    for j in range(len(x_axis)):
        text = ax.text(j, i, int(heatmap[i, j]),
                        ha="center", va="center", color="w",fontsize=8)

fig.tight_layout()
# Show the heatmap plot
plt.savefig("../Output/Heatmap_Germany.png")


# -------------- GRAPH CANADA ------------------------------
plt.clf()
x_axis = np.arange(min_day, max_day+1)
y_axis = np.arange(0, 24)
y_axis = list(y_axis)
y_axis.reverse()
y_axis = np.array(y_axis)
heatmap = np.zeros((24, max_day-min_day+1))

rows = df1.take(df1.count())
for row in rows:
    day = row['day_of_month']
    hour = row['hour_of_day']
    count = 1  
    heatmap[23-hour, day-min_day] += count

fig, ax = plt.subplots(figsize=(10, 8))
im = ax.imshow(heatmap, cmap='Reds')

ax.set_xticks(np.arange(len(x_axis)))
ax.set_yticks(np.arange(len(y_axis)))
ax.set_xticklabels(x_axis)
ax.set_yticklabels(y_axis)

ax.set_xlabel('Day of Month')
ax.set_ylabel('Hour of Visit')
ax.set_title('Heatmap of Highest Canada Host Activity')

cbar = ax.figure.colorbar(im, ax=ax, shrink = 1)

plt.setp(ax.get_xticklabels(), rotation=45, ha="right",
          rotation_mode="anchor")

# Loop over data dimensions and create text annotations.
for i in range(len(y_axis)):
    for j in range(len(x_axis)):
        text = ax.text(j, i, int(heatmap[i, j]),
                        ha="center", va="center", color="w",fontsize=8)


fig.tight_layout()

#Showing the heatmap plot
plt.savefig("../Output/Heatmap_Canada.png")

# -------------------------- GRAPH SINGAPORE ------------------
plt.clf()
x_axis = np.arange(min_day, max_day+1)
y_axis = np.arange(0, 24)
y_axis = list(y_axis)
y_axis.reverse()
y_axis = np.array(y_axis)
# Create a 2D array of zeros to represent the heatmap
heatmap = np.zeros((24, max_day-min_day+1))
# Populate the heatmap with the counts
rows = df2.take(df2.count())
for row in rows:
    day = row['day_of_month']
    hour = row['visit_hour']
    count = 1
    heatmap[23-hour, day-min_day] += count
# Create the heatmap plot
fig, ax = plt.subplots(figsize=(10, 8))
im = ax.imshow(heatmap, cmap='Reds')
# Set the x-axis and y-axis labels
ax.set_xticks(np.arange(len(x_axis)))
ax.set_yticks(np.arange(len(y_axis)))
ax.set_xticklabels(x_axis)
ax.set_yticklabels(y_axis)
# Set the x-axis and y-axis labels and title
ax.set_xlabel('Day of Month')
ax.set_ylabel('Hour of Visit')
ax.set_title('Heatmap of Highest Singapore Host Activity')
# Add colorbar legend
cbar = ax.figure.colorbar(im, ax=ax, shrink=1)
# Rotate the x-axis tick labels
plt.setp(ax.get_xticklabels(), rotation=45, ha="right",
          rotation_mode="anchor")
# Loop over data dimensions and create text annotations.
for i in range(len(y_axis)):
    for j in range(len(x_axis)):
        text = ax.text(j, i, int(heatmap[i, j]),
                        ha="center", va="center", color="w")

fig.tight_layout()
#Showing the heatmap plot
plt.savefig("../Output/Heatmap_Singapore.png")
