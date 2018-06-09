import sklearn
import plotly.plotly as py
import plotly.graph_objs as go
import pandas as pd
import numpy as np
from itertools import cycle

from sklearn import svm, datasets
from sklearn.metrics import roc_curve, auc
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import label_binarize
from sklearn.multiclass import OneVsRestClassifier
from scipy import interp

filename = 'data/diabetes.csv'
data = pd.read_csv(filename)

# function to check 0 in column
def chkColumnForVal(col_name,val):
    print (col_name)
    rowcnt=0
    out_array=[]
    for t in df[col_name]:
        if(t<val):
            out_array.append(rowcnt)
        rowcnt=rowcnt+1
    return len(out_array)

#function to find mean,median,mode
def cal_mmm(col_name): 
    mean = df[col_name].mean()
    mode = df[col_name].mode()
    #median = df[col_name].median
    mmm_array=[mean,mode]
    return mmm_array