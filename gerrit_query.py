#!/usr/bin/env python
import subprocess
import json
import argparse
import os
import time
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import sys


class query_gerrit():
    es=Elasticsearch("http://127.0.0.1:9201/:9201")
    def __init__(self,user,gerrit,status,project):
        self.gerrit=gerrit
        self.user=user
        self.project=project
        self.status=status
        self.gerrit_cmd="ssh -p 29418 %s@%s gerrit query status:%s 'project:%s' --format json --current-patch-set" % (self.user,self.gerrit,self.status,self.project)
    def get_changes(self,iterakey):
        #contact with ingemar to algin the selngerrit query limit with central gerrit
        max_found=500
        if iterakey:
            params=" --start %d"%(iterakey*max_found)
        else:
            params=""
        output=subprocess.check_output(self.gerrit_cmd+params,shell=True)
        result=output.decode()
        result_list=result.splitlines()
        last=json.loads(result_list[-1])
        if "rowCount" in last.keys():
            result_list.pop()
        nums=len(result_list)
        print(nums)
        if nums==max_found:
            next_iterakey=iterakey+1
        else:
            next_iterakey=False
        return result_list,next_iterakey
    def execute(self):
        result=subprocess.check_output(self.gerrit_cmd,shell=True)
        result_list=result.splitlines()
        return result_list
    def update_index(self,query_list):
        project_name=self.project.split("/")
        if self.project=="wmr/wmr_mpsw" or self.project=="wmr/wmr_rnc":
            indexname="gerrit-stats_"+project_name[-1]+"_lindholmen"
        else:
            indexname="gerrit-stats_"+project_name[-1]+"_central"
        item_package=[]
        for eachitem in query_list:
            item_dict=json.loads(eachitem)
            if "currentPatchSet" in item_dict.keys():
                item_dict["currentPatchSet"]={"number":int(item_dict["currentPatchSet"]["number"]),"sizeInsertions":item_dict["currentPatchSet"]["sizeInsertions"],"sizeDeletions":item_dict["currentPatchSet"]["sizeDeletions"]}
            #extract useful information from json
            if "commitMessage" in item_dict.keys() and "subject" in item_dict.keys() :
                try:
                    item_dict.pop("commitMessage")
                    item_dict.pop("subject")
                except Exception:
                    print("pop dict item failed")
            else:
                print("check the correct key") 
            item_dict["createdOn"]=time.strftime("%Y-%m-%d",time.localtime(item_dict["createdOn"]))
            item_dict["lastUpdated"]=time.strftime("%Y-%m-%d",time.localtime(item_dict["lastUpdated"]))
            item_package.append(item_dict)
        actions=[{
            '_index': indexname,
            '_type':project_name[-1],
            '_id':d['id'],
            '_source':d 
        }
        for d in item_package]
        helpers.bulk(query_gerrit.es,actions)
        
if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument('-a','--address',help="please input the gerrit name ex:selngerrit.mo.sw.ericsson.se or gerrit.ericsson.se")
    parser.add_argument('-p','--project',help="the repo name in gerrit ex:wmr/wmr_mpsw")
    username=os.environ['USER']
    args=parser.parse_args()
    statuses=["abandoned","merged","open"]
    defaultencoding='utf-8'
    if sys.getdefaultencoding()!=defaultencoding:
        reload(sys)
        sys.setdefaultencoding(defaultencoding)
    for status in statuses:
        iterakey=0
        query=query_gerrit(username,args.address,status,args.project)
        while True:
            query_list,returned_iterakey=query.get_changes(iterakey)
            query.update_index(query_list) 
            if not returned_iterakey:
                break
            else:
                iterakey=returned_iterakey   

