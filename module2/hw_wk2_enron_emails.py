
# coding: utf-8

# ### Create RDD from all of the mail directory files

# In[6]:


rdd = sc.wholeTextFiles("hdfs://sandbox.hortonworks.com:8020/course2/maildir_sample/*/*/*")
print(rdd.count())


# In[10]:


import re
import time

def extract_fields(mail_ref):
    mail_str = mail_ref[1]
    
    header_data = mail_str.split("\n\n", 1)[0]
    
    values = []                    
    start_matching = False         
    val = ""                       
    
    header_value_map = {}
    header = None
    pattern = r"[^\s:]+:\s"
    
    for line in header_data.split("\n"):  
        line = line.strip()

        if re.match(pattern, line): 

            start_matching = True   
            if val:                 
                values.append(val)  
                header_value_map[header] = val
                val = ""            

            val += re.sub(pattern, "", line) 
        else:
            if start_matching:      
                val += "{}\n".format(line) 

        found_header = re.findall(pattern,  line)
        if found_header:
            header = found_header[0].strip().rstrip(":")
            
    return (header_value_map["Message-ID"], header_value_map["Date"], 
            header_value_map["From"], header_value_map.get("To"), mail_str)
    
extractedRdd = rdd.map(extract_fields)

df = sqlContext.createDataFrame(extractedRdd, ['messageId', 'dateStr', 'fromAddrs', 'toAddrs', 'raw'])
df.show()


df.write.csv('hdfs://sandbox.hortonworks.com:8020/course2/enron_emails_%s.csv' % int(time.time()))


# ### Store dataframe in HIVE

# In[5]:


# the create below didn't store the data properly 
# sqlContext.sql("create table enron_emails as select * from enron_emails_tmp");
# so I ran manual SQL to create HIVE tablet then do insert here
# this works .. but due to VM issues it creates duplicate insert every time.. its like it resends the request internally
"""
CREATE EXTERNAL TABLE enron_email(
        messageID STRING, 
        dateStr STRING,
        fromAddrs STRING,
        toAddrs STRING,
        raw STRING)
    COMMENT 'Email data dump'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS ORC;
"""
df.createOrReplaceTempView("enron_emails_tmp") 

sqlContext.sql("insert into table enron_email select * from enron_emails_tmp")

