
# coding: utf-8

# In[1]:


data_path = "rajeev_data" 

import os
if not os.path.isdir(data_path): # creates path if it does not exist
    os.makedirs(data_path)

import pandas as pd
import urllib.request
import zipfile


# In[2]:


os.chdir(data_path)

ssa_url = 'http://www.socialsecurity.gov/oact/babynames/names.zip' 

if not os.path.isfile("names.zip"):
    #print "Downloading."
    import urllib
    urllib.request.urlretrieve(ssa_url, 'names.zip')
else: print("Data already downloaded.")

if not os.path.isfile("yob1880.txt") or not os.path.isfile("yob2013.txt"):
    #print "Extracting."
    import zipfile
    with zipfile.ZipFile('names.zip') as zf:
        for member in zf.infolist():
            zf.extract(member)
else: print("Data already extracted.")

os.chdir("../")


# In[3]:


redo_dataframes = False
os.chdir(data_path)

if (redo_dataframes == True or
    not os.path.isfile("yob.pickle") or 
    not os.path.isfile("names.pickle") or 
    not os.path.isfile("years.pickle")):

    print("Processing.")
    
    # read individual files, yob1880.txt, yob1881.txt, etc. and assemble into a dataframe
    years = range(1880, 2017) # stops at 2017: update this when Social Security Administration adds to data 
    parts = []
    part_columns = ['name', 'sex', 'births']
    for year in years:
        path = 'yob' + str(year) + '.txt'
        part_df = pd.read_csv(path, names=part_columns)
        part_df['year'] = year
        parts.append(part_df)
    yob = pd.concat(parts, ignore_index=True)
    
    # add column 'pct': the number of births of that name and sex in that year
    # divided by the total number of births of that sex in that year, multiplied by
    # 100 to turn into a percentage and reduce leading zeroes
    def add_pct(group):
        births = group.births.astype(float)
        group['pct'] = (births / births.sum() * 100)
        return group
    yob = yob.groupby(['year', 'sex']).apply(add_pct)
    #add rank of each name each year each sex
    yob['ranked'] = yob.groupby(['year', 'sex'])['births'].rank(ascending=False)
    yob.to_pickle("yob.pickle")
    
    # names dataframe: discards individual birth or pct values, and instead collects data on unique names.
    # There is one row per unique combination of name and sex.
    yobf = yob[yob.sex == 'F']
    yobm = yob[yob.sex == 'M']
    names_count = pd.DataFrame(yobf['name'].value_counts())
    names_count.columns= ['year_count']
    names_min = pd.DataFrame(yobf.groupby('name').year.min()) 
    names_min.columns = ['year_min']
    names_max = pd.DataFrame(yobf.groupby('name').year.max()) 
    names_max.columns = ['year_max']
    names_pctsum = pd.DataFrame(yobf.groupby('name').pct.sum()) 
    names_pctsum.columns = ['pct_sum']
    names_pctmax = pd.DataFrame(yobf.groupby('name').pct.max())
    names_pctmax.columns = ['pct_max']
    names_f = names_count.join(names_min)
    names_f = names_f.join(names_max)
    names_f = names_f.join(names_pctsum)
    names_f = names_f.join(names_pctmax)
    names_f['sex'] = "F"
    names_f.reset_index(inplace=True, drop=False)
    names_f.columns = ['name', 'year_count', 'year_min', 'year_max', 'pct_sum', 'pct_max', 'sex']
    names_f = names_f[['name', 'sex', 'year_count', 'year_min', 'year_max', 'pct_sum', 'pct_max']]
    names_count = pd.DataFrame(yobm['name'].value_counts()) 
    names_count.columns=['year_count']
    names_min = pd.DataFrame(yobm.groupby('name').year.min()) 
    names_min.columns = ['year_min']
    names_max = pd.DataFrame(yobm.groupby('name').year.max()) 
    names_max.columns = ['year_max']
    names_pctsum = pd.DataFrame(yobm.groupby('name').pct.sum()) 
    names_pctsum.columns = ['pct_sum']
    names_pctmax = pd.DataFrame(yobm.groupby('name').pct.max()) 
    names_pctmax.columns = ['pct_max']
    names_m = names_count.join(names_min)
    names_m = names_m.join(names_max)
    names_m = names_m.join(names_pctsum)
    names_m = names_m.join(names_pctmax)
    names_m['sex'] = "M"
    names_m.reset_index(inplace=True, drop=False)
    names_m.columns = ['name', 'year_count', 'year_min', 'year_max', 'pct_sum', 'pct_max', 'sex']
    names_m = names_m[['name', 'sex', 'year_count', 'year_min', 'year_max', 'pct_sum', 'pct_max']]
    names = pd.concat([names_f, names_m], ignore_index=True)
    names.to_pickle('names.pickle')
    
    # create years dataframe. Discards individual name data, aggregating by year.
    total = pd.DataFrame(yob.pivot_table('births', index='year', columns = 'sex', aggfunc=sum))
    total.reset_index(drop=False, inplace=True)
    total.columns = ['year', 'births_f', 'births_m']
    total['births_t'] = total.births_f + total.births_m
    newnames = pd.DataFrame(names.groupby('year_min').year_min.count())
    newnames.columns = ['firstyearcount']
    newnames.reset_index(drop=False, inplace=True)
    newnames.columns = ['year', 'new_names']
    uniquenames = pd.DataFrame()
    for yr in range(1880, 2017):
        uniquenames = uniquenames.append(pd.DataFrame([{'year':yr, 'unique_names':len(yob[yob.year == yr].name.unique())}]), ignore_index=True)
    years = pd.merge(left=total, right=newnames, on='year', right_index=False, left_index=False)
    years = pd.merge(left=years, right=uniquenames, on='year', right_index=False, left_index=False)
    years['sexratio'] = 100.0 * years.births_m / years.births_f
    years.to_pickle('years.pickle')
    
else:
    
    print("Reading from pickle.")
    yob = pd.read_pickle('yob.pickle')
    names = pd.read_pickle('names.pickle')
    years = pd.read_pickle('years.pickle')
    
os.chdir("../")


# In[4]:


os.chdir(data_path)

if (redo_dataframes == True or
    not os.path.isfile("yob1900.pickle") or 
    not os.path.isfile("names1900.pickle") or 
    not os.path.isfile("years1900.pickle")):

    yob1900 = yob[yob.year >= 1900]
    yob1900.to_pickle("yob1900.pickle")

    yobf = yob1900[yob1900.sex == 'F']
    yobm = yob1900[yob1900.sex == 'M']
    names_count = pd.DataFrame(yobf['name'].value_counts())
    names_count.columns= ['year_count']
    names_min = pd.DataFrame(yobf.groupby('name').year.min()) 
    names_min.columns = ['year_min']
    names_max = pd.DataFrame(yobf.groupby('name').year.max()) 
    names_max.columns = ['year_max']
    names_pctsum = pd.DataFrame(yobf.groupby('name').pct.sum()) 
    names_pctsum.columns = ['pct_sum']
    names_pctmax = pd.DataFrame(yobf.groupby('name').pct.max())
    names_pctmax.columns = ['pct_max']
    names_f = names_count.join(names_min)
    names_f = names_f.join(names_max)
    names_f = names_f.join(names_pctsum)
    names_f = names_f.join(names_pctmax)
    names_f['sex'] = "F"
    names_f.reset_index(inplace=True, drop=False)
    names_f.columns = ['name', 'year_count', 'year_min', 'year_max', 'pct_sum', 'pct_max', 'sex']
    names_f = names_f[['name', 'sex', 'year_count', 'year_min', 'year_max', 'pct_sum', 'pct_max']]
    names_count = pd.DataFrame(yobm['name'].value_counts()) 
    names_count.columns=['year_count']
    names_min = pd.DataFrame(yobm.groupby('name').year.min()) 
    names_min.columns = ['year_min']
    names_max = pd.DataFrame(yobm.groupby('name').year.max()) 
    names_max.columns = ['year_max']
    names_pctsum = pd.DataFrame(yobm.groupby('name').pct.sum()) 
    names_pctsum.columns = ['pct_sum']
    names_pctmax = pd.DataFrame(yobm.groupby('name').pct.max()) 
    names_pctmax.columns = ['pct_max']
    names_m = names_count.join(names_min)
    names_m = names_m.join(names_max)
    names_m = names_m.join(names_pctsum)
    names_m = names_m.join(names_pctmax)
    names_m['sex'] = "M"
    names_m.reset_index(inplace=True, drop=False)
    names_m.columns = ['name', 'year_count', 'year_min', 'year_max', 'pct_sum', 'pct_max', 'sex']
    names_m = names_m[['name', 'sex', 'year_count', 'year_min', 'year_max', 'pct_sum', 'pct_max']]
    names1900 = pd.concat([names_f, names_m], ignore_index=True)
    names1900.to_pickle('names1900.pickle')
    
    # create years dataframe. Discards individual name data, aggregating by year.
    total = pd.DataFrame(yob1900.pivot_table('births', index='year', columns = 'sex', aggfunc=sum))
    total.reset_index(drop=False, inplace=True)
    total.columns = ['year', 'births_f', 'births_m']
    total['births_t'] = total.births_f + total.births_m
    newnames = pd.DataFrame(names.groupby('year_min').year_min.count())
    newnames.columns = ['firstyearcount']
    newnames.reset_index(drop=False, inplace=True)
    newnames.columns = ['year', 'new_names']
    uniquenames = pd.DataFrame()
    for yr in range(1900, 2017):
        uniquenames = uniquenames.append(pd.DataFrame([{'year':yr, 'unique_names':len(yob1900[yob1900.year == yr].name.unique())}]), ignore_index=True)
    years1900 = pd.merge(left=total, right=newnames, on='year', right_index=False, left_index=False)
    years1900 = pd.merge(left=years, right=uniquenames, on='year', right_index=False, left_index=False)
    years1900['sexratio'] = 100.0 * years.births_m / years.births_f
    years1900.to_pickle('years.pickle')
    
else:
    
    print("Reading from pickle (1900+ versions).")
    yob1900 = pd.read_pickle('yob1900.pickle')
    names1900 = pd.read_pickle('names1900.pickle')
    years1900 = pd.read_pickle('years1900.pickle')
    
os.chdir("../")


# In[5]:


print("Tail of dataframe 'yob':")
yob.tail()


# In[6]:


print("\nTail of dataframe 'names':")
names.tail()


# In[7]:


print("\nTail of dataframe 'years':")
years.tail()


# In[8]:


print("Tail of dataframe 'yob1900':")
yob1900.tail()


# In[9]:


print("Tail of dataframe 'names1900':")
names1900.tail()


# In[10]:


years1900 = years1900[['year', 'births_f', 'births_m', 'births_t', 'new_names', 'unique_names_x', 'sexratio']]
years1900.columns = ['year', 'births_f', 'births_m', 'births_t', 'new_names', 'unique_names', 'sexratio']
# above lines correct outer merge problem
print("Tail of dataframe 'years1900':")
years1900.tail()

