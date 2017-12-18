#!usr/bin/env python
#-*- coding:utf-8 _*-  
""" 
@author:ZhangHui
@file: getUserAgent.py 
@time: 2017/12/17 
"""
import os
import numpy as np
BASEDIR = os.path.abspath('.')
UserAgentFile = BASEDIR+'/User_Agent.txt'
def getUserAgent():
    file = open(UserAgentFile,'r')
    lines = [i for i in file]
    headers = {'User-Agent':lines[np.random.randint(0,len(lines))].lstrip('User-Agent: ').rstrip('\n')}
    file.close()
    return headers