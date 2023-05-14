#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# Author:haojian
# File Name: cmParse.py
# Created Time: 2022/6/8 10:07:46
# cm文件位置：/data/esbftp/cm/4G/NOKIA/OMC4/CM/actual_export_RESULT_2022-06-08.xml


import os,sys
import logging
from logging.handlers import RotatingFileHandler
import xml.etree.ElementTree as ET
import datetime
import re
import math

os.chdir(sys.path[0])
#assert ('linux' in sys.platform), '该代码只能在 Linux 下执行'
if 'linux' in sys.platform:
    inpath=''
    outpath1='./'
    outpath2='/data/output/cm/nokia/4g/'
    logpath='../log/'
else:
    inpath='./'
    outpath1='./'
    outpath2='./'
    logpath='./'

#handler = RotatingFileHandler('cmParse.log',maxBytes = 100*1024*1024,backupCount = 3)
handler = logging.FileHandler(logpath+'cmParse_'+datetime.datetime.now().strftime("%Y%m%d")+'.log')
#handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
console = logging.StreamHandler()
#console.setLevel(logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(level = logging.INFO)
logger.addHandler(handler)
if 'linux' not in sys.platform:
    logger.addHandler(console)

def fcn2feq(fcn):
    fcn=int(fcn)
    if fcn<600:	
        ful=1920+0.1*(fcn-0);fdl=ful+190
    elif fcn<1200:	
        ful=1850+0.1*(fcn-600);fdl=ful+80
    elif fcn<1950:	
        ful=1710+0.1*(fcn-1200);fdl=ful+95
    elif fcn<2400:	
        ful=1710+0.1*(fcn-1950);fdl=ful+400
    elif fcn<2650:	
        ful=824+0.1*(fcn-2400);fdl=ful+45
    elif fcn<2750:	
        ful=830+0.1*(fcn-2650);fdl=ful+45
    elif fcn<3450:	
        ful=2500+0.1*(fcn-2750);fdl=ful+120
    elif fcn<3800:	
        ful=880+0.1*(fcn-3450);fdl=ful+45
    elif fcn<4150:	
        ful=1749.9+0.1*(fcn-3800);fdl=ful+95
    elif fcn<4750:	
        ful=1710+0.1*(fcn-4150);fdl=ful+400
    elif fcn<5000:	
        ful=1427.9+0.1*(fcn-4750);fdl=ful+48
    elif fcn<5180:	
        ful=698+0.1*(fcn-5000);fdl=ful+30
    elif fcn<5280:	
        ful=777+0.1*(fcn-5180);fdl=ful+-31
    elif fcn<5380:	
        ful=788+0.1*(fcn-5280);fdl=ful+-30
    elif fcn<5850:	
        ful=704+0.1*(fcn-5730);fdl=ful+30
    elif fcn<6000:	
        ful=815+0.1*(fcn-5850);fdl=ful+45
    elif fcn<6150:	
        ful=830+0.1*(fcn-6000);fdl=ful+45
    elif fcn<6450:	
        ful=832+0.1*(fcn-6150);fdl=ful+-41
    elif fcn<6600:	
        ful=1447.9+0.1*(fcn-6450);fdl=ful+48
    elif fcn<7400:	
        ful=3410+0.1*(fcn-6600);fdl=ful+100
    elif fcn<7700:	
        ful=2000+0.1*(fcn-7500);fdl=ful+180
    elif fcn<8040:	
        ful=1626.5+0.1*(fcn-7700);fdl=ful+-101.5
    elif fcn<8690:	
        ful=1850+0.1*(fcn-8040);fdl=ful+80
    elif fcn<9040:	
        ful=814+0.1*(fcn-8690);fdl=ful+45
    elif fcn<9210:	
        ful=807+0.1*(fcn-9040);fdl=ful+45
    elif fcn<9660:	
        ful=703+0.1*(fcn-9210);fdl=ful+55
    else:
        ful=0;fdl=0
    return ful,fdl

def deal_with_file(xmlName,xmlCache,omc):
    with open(xmlName,encoding='utf8') as f:
        cellList=dict()
        xmlCache=f.read(1000)
        #获取xml导出时间
        sdate=re.search(r'(?<=dateTime=")\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}\+08:00(?=")',xmlCache).group()
        sdate=sdate[0:10]+' '+sdate[11:19]
        while True:
            txtCache=f.read(1000)
            if txtCache == '':
                break
            xmlCache+=txtCache
            while True:
                if '<managedObject' not in xmlCache:
                    break
                xmlCache=xmlCache[xmlCache.find('<managedObject'):]
                if '/>' in xmlCache and '>' not in xmlCache[:xmlCache.find('/>')]:
                    xmltext=xmlCache[:xmlCache.find('/>')+2]
                    xmlCache=xmlCache[xmlCache.find('/>')+2:]
                elif '</managedObject>' in xmlCache:
                    xmltext=xmlCache[:xmlCache.find('</managedObject>')+16]
                    xmlCache=xmlCache[xmlCache.find('</managedObject>')+16:]
                else:
                    break
                root = ET.fromstring(xmltext) 
                if root.attrib['class'] == 'LNCEL':
                    cellDistName=root.attrib['distName']
                    logger.info(root.attrib['class']+'\t'+root.attrib['distName'])
                    if cellDistName not in cellList:
                        cellList[cellDistName]=dict()
                    cellList[cellDistName]['version']=root.attrib['version']
                    for p in list(root):
                        if p.tag=='list' and p.attrib['name']=='furtherPlmnIdL':
                            cellList[cellDistName]['share']='1'
                        elif p.tag=='p':
                            cellList[cellDistName][p.attrib['name']]=p.text
                elif root.attrib['class'] == 'LNCEL_FDD':
                    #open('cell_fdd.xml','w',encoding='utf8').write(xmltext)
                    #break
                    cellDistName=root.attrib['distName'][:-12]
                    logger.info(root.attrib['class']+'\t'+root.attrib['distName'])
                    if cellDistName not in cellList:
                        cellList[cellDistName]=dict()
                    for p in list(root):
                        if p.tag=='p':
                            cellList[cellDistName][p.attrib['name']]=p.text
                    #if cellDistName == 'PLMN-PLMN/MRBTS-159917/LNBTS-159917/LNCEL-17':
                    #    break

###########################################
        for cellDistName in cellList:
        #if True:
            #cellDistName='PLMN-PLMN/MRBTS-159917/LNBTS-159917/LNCEL-17'
            if 'lcrId' in cellList[cellDistName]:
                #print(cellDistName)
                eNodebId=cellDistName.split('/')[1][6:]		#基站号
                lcrid=cellList[cellDistName].get('lcrId','')		#小区号
                #sdate=sdate		#分析时间
                isalive='1'		#是否在网
                islock='0' if cellList[cellDistName].get('administrativeState','3')=='1' else '1'		#是否闭锁 administrativeState:unlocked (1), shutting down (2), locked (3)
                vendor='诺基亚'		#厂家
                eci='127.'+eNodebId+'.'+lcrid		#对象编号
                cellname=re.sub(r'[,|]','_',cellList[cellDistName].get('cellName',''))		#小区名
                #eNodebId=''		#基站号
                #lcrid=''		#小区号
                isp='联通'		#承建运营商
                share=cellList[cellDistName].get('share','0')		#是否共享
                if 'earfcnDL' in cellList[cellDistName]:
                    earfcn=cellList[cellDistName]['earfcnDL']		#频点(下行)
                    ful,fdl=fcn2feq(earfcn)         ##上行中心频率(MHz),下行中心频率(MHz)
                    ful=str(ful);fdl=str(fdl)
                else:
                    earfcn=ful=fdl=''
                pci=cellList[cellDistName].get('phyCellId','')		#物理小区id
                tac=cellList[cellDistName].get('tac','')		#tac
                dlChBw=cellList[cellDistName].get('dlChBw','') 		#载波宽度(MHz)
                Bandwidth='' if not dlChBw.isdigit() else str(int(dlChBw)/10) 
                dlRsBoost=cellList[cellDistName].get('dlRsBoost','')		#Pa
                #print(dlRsBoost)
                Pa=str((1000-int(dlRsBoost))/100) if dlRsBoost.isdigit() else ''     #Pa
                #print(Pa)
                Pb=cellList[cellDistName].get('dlpcMimoComp','')		#Pb
                pMax=cellList[cellDistName].get('pMax','')		#总功率（dBm）
                #print(pMax)
                numPrb=0 if not dlChBw.isdigit() else 6 if dlChBw=='14' else int(dlChBw)/2 #prb数量
                #print(numPrb)
                rspwr='{0:.2f}'.format(int(pMax)/10-10*math.log10(numPrb*12)-float(Pa)) if pMax.isdigit() and numPrb>0 else '' #参考信号功率（dBm）
                #print(rspwr)
############################################
                csvList.append([cellDistName,cellList[cellDistName]['version'],cellList[cellDistName].get('share','0'),cellList[cellDistName].get('cellTechnology','0'),cellList[cellDistName].get('name',''),cellList[cellDistName].get('cellName',''),cellList[cellDistName].get('lcrId',''),cellList[cellDistName].get('eutraCelId',''),omc])
                csvList2.append([sdate,isalive,islock,vendor,eci,cellname,eNodebId,lcrid,isp,share,earfcn,ful,fdl,pci,tac,Bandwidth,Pa,Pb,rspwr,])





if __name__ == '__main__':
    csvList=[['distName','version','share','cellTechnology','name','cellName','lcrId','eutraCelId','omc']]
    csvList2=[['sdate','isalive','islock','vendor','eci','cellname','eNodebId','lcrid','isp','share','earfcn','ful','fdl','pci','tac','Bandwidth','Pa','Pb','rspwr']]
    for i in [p for p in os.scandir('/data/esbftp/cm/4G/NOKIA/') if p.is_dir()]:
        omc=i.name
        xmlList=[j for j in os.scandir(os.path.join(i.path,'CM'))]
        #xmlList=[j for j in os.scandir('/data/esbftp/cm/4G/NOKIA/OMC1/CM')]
        xmlList.sort(key=lambda p:p.name)
        logger.info(xmlList[-1].path)
        deal_with_file(xmlList[-1].path,csvList,omc)
    #deal_with_file('actual_export_RESULT_2022-06-18.xml',csvList,'OMC4')
    csvName=outpath1+'nokiaCm_'+datetime.datetime.now().strftime("%Y%m%d%H")+'.csv'
    open(csvName+'.tmp','w').write('\n'.join(['|'.join(i) for i in csvList]))
    os.remove(csvName) if os.path.isfile(csvName) else None
    os.rename(csvName+'.tmp',csvName)
    csvName=outpath2+'nokiaCm_'+datetime.datetime.now().strftime("%Y%m%d%H")+'.csv' if 'linux' in sys.platform else outpath2+'nokiaCm2_'+datetime.datetime.now().strftime("%Y%m%d%H")+'.csv'
    open(csvName+'.tmp','w').write('\n'.join([','.join(i) for i in csvList2]))
    os.remove(csvName) if os.path.isfile(csvName) else None
    os.rename(csvName+'.tmp',csvName)

