# counter库从文件读取
# 增加一些指标V3
# v7，如果小区在字典，按字典判断类型是否NB；不在字典则当宏站输出
# 20220228 修改多个15分钟在一个文件的情况deal_with_file
import xml.etree.ElementTree as ET
from multiprocessing import Pool
from multiprocessing import Manager
import gzip
import os
import time
import csv
import datetime
import sys
import logging
logger = logging.getLogger(__name__)
logger.setLevel(level = logging.INFO)
handler = logging.FileHandler("/home/nokia/nifi33/jyc/pm/log.txt")
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logger.addHandler(handler)
logger.addHandler(console)


fil = ''
cell_dic_file = '/home/nokia/nifi33/jyc/pm/cell.csv'
hour_delay = 1
if len(sys.argv) > 1:
    d0 = str(sys.argv[1])
else:
    d0=(datetime.datetime.now() - datetime.timedelta(hours=hour_delay)).strftime('%Y%m%d%H')
output_path = '/data/output/pm/nokia/4g'
#output_path = '/home/nifi/jyc/'
output_name = os.path.join(output_path,'nokia_4g_'+d0+'00.csv')

#/data/esbftp/pm/4G/NOKIA/OMC1/PM/2021082513/  OMC1---OMC5
# os.chdir(r'd:\\workdir\\20200727\\')
counter_file = '/home/nokia/nifi33/jyc/pm/counters.csv'
a_counters = []
with open(counter_file,'r') as f:
    lines = f.readlines()
    for line in lines:
        a_counters.append(line.strip())
# a_counters=['M8020C3','M8005C234','M8005C235','M8005C236','M8005C237','M8005C238','M8005C239','M8005C240','M8005C241','M8005C242','M8005C243','M8005C244','M8005C245','M8005C246','M8005C247','M8005C248','M8005C249','M8005C250','M8005C251','M8005C252','M8005C253','M8005C254','M8005C255','M8010C36','M8010C37','M8010C38','M8010C39','M8010C40','M8010C41','M8010C42','M8010C43','M8010C44','M8010C45','M8010C46','M8010C47','M8010C48','M8010C49','M8010C50','M8010C51','M8026C254','M8001C153','M8026C259','M8001C154','M8001C155','M8012C91','M8012C93','M8012C95','M8012C97','M8012C99','M8012C101','M8012C103','M8012C105','M8012C107','M8012C156','M8012C157','M8012C158','M8012C159','M8012C160','M8012C161','M8012C162','M8012C163','M8012C164','M8012C117','M8012C119','M8012C121','M8012C123','M8012C125','M8012C127','M8012C129','M8012C131','M8012C133','M8012C165','M8012C166','M8012C167','M8012C168','M8012C169','M8012C170','M8012C171','M8012C172','M8012C173','M8013C17','M8013C18','M8013C19','M8013C21','M8013C34','M8013C31','M8013C5','M8006C188','M8006C189','M8006C190','M8006C191','M8006C192','M8006C193','M8006C194','M8006C195','M8006C196','M8006C197','M8006C198','M8006C199','M8006C200','M8006C201','M8006C202','M8006C203','M8006C204','M8006C205','M8006C206','M8006C207','M8006C208','M8006C209','M8006C210','M8006C211','M8006C212','M8006C213','M8006C214','M8006C215','M8006C216','M8006C217','M8006C218','M8006C219','M8006C220','M8006C221','M8006C222','M8006C223','M8006C244','M8006C245','M8006C248','M8006C249','M8013C43','M8013C44','M8006C256','M8006C257','M8006C259','M8006C261','M8006C277','M8006C255','M8006C258','M8006C260','M8012C19','M8012C20','M8011C47','M8011C51','M8010C112','M8010C113','M8051C55','M8051C56','M8005C284','M8005C285','M8005C286','M8005C287','M8005C288','M8005C289','M8005C290','M8005C291','M8005C292','M8005C293','M8005C294','M8005C295','M8005C296','M8005C297','M8005C298','M8005C299','M8005C300','M8005C301','M8005C302','M8005C303','M8005C304','M8005C305','M8001C216','M8001C217','M8006C244','M8006C245','M8006C246','M8006C247','M8006C248','M8006C249','M8006C250','M8006C251','M8006C252','M8016C11','M8014C14','M8014C19','M8014C0','M8014C7','M8011C47','M8020C3','M8011C51','M8026C255','M8001C305','M8026C260','M8001C314','M8013C67','M8048C0','M8048C1','M8048C5','M8048C6']
d_cell = {}
d_type = {}
# kpi = []
with open(cell_dic_file,'r') as f:
    lines = f.readlines()
    for line in lines:
        cols = line.strip().split(',')
        d_cell[cols[0]] = cols[1]
        d_type[cols[0]] = cols[2]



def get_divide(a,b,c=4):
    if b==0:
        return 0
    else:
        return round(a/b,c)

def get_cell(cell):
    #PLMN-PLMN/MRBTS-558676/LNBTS-558676/LNCEL-13
    s = cell.split('LNBTS-')[1].split('/LNCEL-')
    cell =  s[0]+'-'+s[1]
    cell2 = '127.'+s[0]+'.'+s[1]
    # print(cell)
    if cell in d_cell:
        if d_type[cell] == 'NB-IoT':
            return 0
        else:
            return d_cell[cell]
    else:
        return cell2

def get_time(starttime):
    #2021-06-10T10:45:00.000+08:00:00
    return starttime.replace('T',' ')[:19]

def get_value_in_dic(d,k):
    if k in d:
        return d[k]
    else:
        #logger.warning("%s not in dic" % k)
        return 0


def deal_with_kpi(cell,dd,starttime,interval):
    interval = int(interval)
    # print(cell)
    cell = get_cell(cell)
    if cell == 0:
        return 0
    else:
        starttime = get_time(starttime)
        d = dd
        a_tuifu = max(10*get_value_in_dic(d,'M8020C3')/1000-9,0)
        a_keyong = 60*interval-a_tuifu
        a_tongji = 60*interval
        a_keyonglv = get_divide(a_keyong,a_tongji)
        a_ganrao = get_divide((-130*get_value_in_dic(d,'M8005C234')-119*get_value_in_dic(d,'M8005C235')-117*get_value_in_dic(d,'M8005C236')-115*get_value_in_dic(d,'M8005C237')-113*get_value_in_dic(d,'M8005C238')-111*get_value_in_dic(d,'M8005C239')-109*get_value_in_dic(d,'M8005C240')-107*get_value_in_dic(d,'M8005C241')-105*get_value_in_dic(d,'M8005C242')-103*get_value_in_dic(d,'M8005C243')-101*get_value_in_dic(d,'M8005C244')-99*get_value_in_dic(d,'M8005C245')-97*get_value_in_dic(d,'M8005C246')-95*get_value_in_dic(d,'M8005C247')-93*get_value_in_dic(d,'M8005C248')-91*get_value_in_dic(d,'M8005C249')-89*get_value_in_dic(d,'M8005C250')-87*get_value_in_dic(d,'M8005C251')-85*get_value_in_dic(d,'M8005C252')-83*get_value_in_dic(d,'M8005C253')-81*get_value_in_dic(d,'M8005C254')-80*get_value_in_dic(d,'M8005C255')),(get_value_in_dic(d,'M8005C234')+get_value_in_dic(d,'M8005C235')+get_value_in_dic(d,'M8005C236')+get_value_in_dic(d,'M8005C237')+get_value_in_dic(d,'M8005C238')+get_value_in_dic(d,'M8005C239')+get_value_in_dic(d,'M8005C240')+get_value_in_dic(d,'M8005C241')+get_value_in_dic(d,'M8005C242')+get_value_in_dic(d,'M8005C243')+get_value_in_dic(d,'M8005C244')+get_value_in_dic(d,'M8005C245')+get_value_in_dic(d,'M8005C246')+get_value_in_dic(d,'M8005C247')+get_value_in_dic(d,'M8005C248')+get_value_in_dic(d,'M8005C249')+get_value_in_dic(d,'M8005C250')+get_value_in_dic(d,'M8005C251')+get_value_in_dic(d,'M8005C252')+get_value_in_dic(d,'M8005C253')+get_value_in_dic(d,'M8005C254')+get_value_in_dic(d,'M8005C255')))-get_divide((-10*get_value_in_dic(d,'M8005C284')-9*get_value_in_dic(d,'M8005C285')-7*get_value_in_dic(d,'M8005C286')-5*get_value_in_dic(d,'M8005C287')-3*get_value_in_dic(d,'M8005C288')-1*get_value_in_dic(d,'M8005C289')+1*get_value_in_dic(d,'M8005C290')+3*get_value_in_dic(d,'M8005C291')+5*get_value_in_dic(d,'M8005C292')+7*get_value_in_dic(d,'M8005C293')+9*get_value_in_dic(d,'M8005C294')+11*get_value_in_dic(d,'M8005C295')+13*get_value_in_dic(d,'M8005C296')+15*get_value_in_dic(d,'M8005C297')+17*get_value_in_dic(d,'M8005C298')+19*get_value_in_dic(d,'M8005C299')+21*get_value_in_dic(d,'M8005C300')+23*get_value_in_dic(d,'M8005C301')+25*get_value_in_dic(d,'M8005C302')+27*get_value_in_dic(d,'M8005C303')+29*get_value_in_dic(d,'M8005C304')+30*get_value_in_dic(d,'M8005C305')),(get_value_in_dic(d,'M8005C284')+get_value_in_dic(d,'M8005C285')+get_value_in_dic(d,'M8005C286')+get_value_in_dic(d,'M8005C287')+get_value_in_dic(d,'M8005C288')+get_value_in_dic(d,'M8005C289')+get_value_in_dic(d,'M8005C290')+get_value_in_dic(d,'M8005C291')+get_value_in_dic(d,'M8005C292')+get_value_in_dic(d,'M8005C293')+get_value_in_dic(d,'M8005C294')+get_value_in_dic(d,'M8005C295')+get_value_in_dic(d,'M8005C296')+get_value_in_dic(d,'M8005C297')+get_value_in_dic(d,'M8005C298')+get_value_in_dic(d,'M8005C299')+get_value_in_dic(d,'M8005C300')+get_value_in_dic(d,'M8005C301')+get_value_in_dic(d,'M8005C302')+get_value_in_dic(d,'M8005C303')+get_value_in_dic(d,'M8005C304')+get_value_in_dic(d,'M8005C305')))
        a_cqi0 = get_value_in_dic(d,'M8010C36')
        a_cqi1 = get_value_in_dic(d,'M8010C37')
        a_cqi2 = get_value_in_dic(d,'M8010C38')
        a_cqi3 = get_value_in_dic(d,'M8010C39')
        a_cqi4 = get_value_in_dic(d,'M8010C40')
        a_cqi5 = get_value_in_dic(d,'M8010C41')
        a_cqi6 = get_value_in_dic(d,'M8010C42')
        a_cqi7 = get_value_in_dic(d,'M8010C43')
        a_cqi8 = get_value_in_dic(d,'M8010C44')
        a_cqi9 = get_value_in_dic(d,'M8010C45')
        a_cqi10 = get_value_in_dic(d,'M8010C46')
        a_cqi11 = get_value_in_dic(d,'M8010C47')
        a_cqi12 = get_value_in_dic(d,'M8010C48')
        a_cqi13 = get_value_in_dic(d,'M8010C49')
        a_cqi14 = get_value_in_dic(d,'M8010C50')
        a_cqi15 = get_value_in_dic(d,'M8010C51')
        a_cqi = get_divide((0*get_value_in_dic(d,'M8010C36')+1*get_value_in_dic(d,'M8010C37')+2*get_value_in_dic(d,'M8010C38')+3*get_value_in_dic(d,'M8010C39')+4*get_value_in_dic(d,'M8010C40')+5*get_value_in_dic(d,'M8010C41')+6*get_value_in_dic(d,'M8010C42')+7*get_value_in_dic(d,'M8010C43')+8*get_value_in_dic(d,'M8010C44')+9*get_value_in_dic(d,'M8010C45')+10*get_value_in_dic(d,'M8010C46')+11*get_value_in_dic(d,'M8010C47')+12*get_value_in_dic(d,'M8010C48')+13*get_value_in_dic(d,'M8010C49')+14*get_value_in_dic(d,'M8010C50')+15*get_value_in_dic(d,'M8010C51')),(get_value_in_dic(d,'M8010C36')+get_value_in_dic(d,'M8010C37')+get_value_in_dic(d,'M8010C38')+get_value_in_dic(d,'M8010C39')+get_value_in_dic(d,'M8010C40')+get_value_in_dic(d,'M8010C41')+get_value_in_dic(d,'M8010C42')+get_value_in_dic(d,'M8010C43')+get_value_in_dic(d,'M8010C44')+get_value_in_dic(d,'M8010C45')+get_value_in_dic(d,'M8010C46')+get_value_in_dic(d,'M8010C47')+get_value_in_dic(d,'M8010C48')+get_value_in_dic(d,'M8010C49')+get_value_in_dic(d,'M8010C50')+get_value_in_dic(d,'M8010C51')),2)
        a_cqigt7 = get_divide((get_value_in_dic(d,'M8010C43')+get_value_in_dic(d,'M8010C44')+get_value_in_dic(d,'M8010C45')+get_value_in_dic(d,'M8010C46')+get_value_in_dic(d,'M8010C47')+get_value_in_dic(d,'M8010C48')+get_value_in_dic(d,'M8010C49')+get_value_in_dic(d,'M8010C50')+get_value_in_dic(d,'M8010C51')),(get_value_in_dic(d,'M8010C36')+get_value_in_dic(d,'M8010C37')+get_value_in_dic(d,'M8010C38')+get_value_in_dic(d,'M8010C39')+get_value_in_dic(d,'M8010C40')+get_value_in_dic(d,'M8010C41')+get_value_in_dic(d,'M8010C42')+get_value_in_dic(d,'M8010C43')+get_value_in_dic(d,'M8010C44')+get_value_in_dic(d,'M8010C45')+get_value_in_dic(d,'M8010C46')+get_value_in_dic(d,'M8010C47')+get_value_in_dic(d,'M8010C48')+get_value_in_dic(d,'M8010C49')+get_value_in_dic(d,'M8010C50')+get_value_in_dic(d,'M8010C51')))
        a_cqilt7 = get_divide((get_value_in_dic(d,'M8010C36')+get_value_in_dic(d,'M8010C37')+get_value_in_dic(d,'M8010C38')+get_value_in_dic(d,'M8010C39')+get_value_in_dic(d,'M8010C40')+get_value_in_dic(d,'M8010C41')+get_value_in_dic(d,'M8010C42')),(get_value_in_dic(d,'M8010C36')+get_value_in_dic(d,'M8010C37')+get_value_in_dic(d,'M8010C38')+get_value_in_dic(d,'M8010C39')+get_value_in_dic(d,'M8010C40')+get_value_in_dic(d,'M8010C41')+get_value_in_dic(d,'M8010C42')+get_value_in_dic(d,'M8010C43')+get_value_in_dic(d,'M8010C44')+get_value_in_dic(d,'M8010C45')+get_value_in_dic(d,'M8010C46')+get_value_in_dic(d,'M8010C47')+get_value_in_dic(d,'M8010C48')+get_value_in_dic(d,'M8010C49')+get_value_in_dic(d,'M8010C50')+get_value_in_dic(d,'M8010C51')))
        a_ul_sdu_lost = get_value_in_dic(d,'M8026C254')
        a_ul_sdu_total = get_value_in_dic(d,'M8001C153')+get_value_in_dic(d,'M8026C254')
        a_dl_sdu_lost = get_value_in_dic(d,'M8026C259')
        a_dl_sdu_total = get_value_in_dic(d,'M8001C154')+get_value_in_dic(d,'M8026C259')
        a_ul_sdu_diubaolv = get_divide(get_value_in_dic(d,'M8026C254'),(get_value_in_dic(d,'M8001C153')+get_value_in_dic(d,'M8026C254')))
        a_ul_sdu_diubaolv_qci1 = get_divide(get_value_in_dic(d,'M8026C255'),(get_value_in_dic(d,'M8001C305')+get_value_in_dic(d,'M8026C255')))
        a_dl_sdu_diubaolv_qci1 = get_divide(get_value_in_dic(d,'M8026C260'),(get_value_in_dic(d,'M8001C314')+get_value_in_dic(d,'M8026C260')))
        a_dl_sdu_qibaolv = get_divide(get_value_in_dic(d,'M8001C155'),(get_value_in_dic(d,'M8001C154')+get_value_in_dic(d,'M8026C259')))
        a_ul_mbps = get_divide((get_value_in_dic(d,'M8012C91')+get_value_in_dic(d,'M8012C93')+get_value_in_dic(d,'M8012C95')+get_value_in_dic(d,'M8012C97')+get_value_in_dic(d,'M8012C99')+get_value_in_dic(d,'M8012C101')+get_value_in_dic(d,'M8012C103')+get_value_in_dic(d,'M8012C105')+get_value_in_dic(d,'M8012C107')),(get_value_in_dic(d,'M8012C156')+get_value_in_dic(d,'M8012C157')+get_value_in_dic(d,'M8012C158')+get_value_in_dic(d,'M8012C159')+get_value_in_dic(d,'M8012C160')+get_value_in_dic(d,'M8012C161')+get_value_in_dic(d,'M8012C162')+get_value_in_dic(d,'M8012C163')+get_value_in_dic(d,'M8012C164')))/1000
        a_dl_mbps = get_divide((get_value_in_dic(d,'M8012C117')+get_value_in_dic(d,'M8012C119')+get_value_in_dic(d,'M8012C121')+get_value_in_dic(d,'M8012C123')+get_value_in_dic(d,'M8012C125')+get_value_in_dic(d,'M8012C127')+get_value_in_dic(d,'M8012C129')+get_value_in_dic(d,'M8012C131')+get_value_in_dic(d,'M8012C133')),(get_value_in_dic(d,'M8012C165')+get_value_in_dic(d,'M8012C166')+get_value_in_dic(d,'M8012C167')+get_value_in_dic(d,'M8012C168')+get_value_in_dic(d,'M8012C169')+get_value_in_dic(d,'M8012C170')+get_value_in_dic(d,'M8012C171')+get_value_in_dic(d,'M8012C172')+get_value_in_dic(d,'M8012C173')))/1000
        a_rrc_req = (get_value_in_dic(d,'M8013C17')+get_value_in_dic(d,'M8013C18')+get_value_in_dic(d,'M8013C19')+get_value_in_dic(d,'M8013C21')+get_value_in_dic(d,'M8013C34')+get_value_in_dic(d,'M8013C31'))
        a_rrc_suc = get_value_in_dic(d,'M8013C5')
        a_rrc_suc_r = get_divide(a_rrc_suc,a_rrc_req)
        a_rrc_congest = get_value_in_dic(d,'M8013C67')
        a_rrc_avg = get_value_in_dic(d,'M8051C55')
        a_rrc_max = get_value_in_dic(d,'M8051C56')
        a_erab_req = (get_value_in_dic(d,'M8006C188')+get_value_in_dic(d,'M8006C197')+get_value_in_dic(d,'M8006C189')+get_value_in_dic(d,'M8006C198')+get_value_in_dic(d,'M8006C190')+get_value_in_dic(d,'M8006C199')+get_value_in_dic(d,'M8006C191')+get_value_in_dic(d,'M8006C200')+get_value_in_dic(d,'M8006C192')+get_value_in_dic(d,'M8006C201')+get_value_in_dic(d,'M8006C193')+get_value_in_dic(d,'M8006C202')+get_value_in_dic(d,'M8006C194')+get_value_in_dic(d,'M8006C203')+get_value_in_dic(d,'M8006C195')+get_value_in_dic(d,'M8006C204')+get_value_in_dic(d,'M8006C196')+get_value_in_dic(d,'M8006C205'))
        a_erab_req_qci3 = get_value_in_dic(d,'M8006C190')+get_value_in_dic(d,'M8006C199')
        a_erab_req_qci4 = get_value_in_dic(d,'M8006C191')+get_value_in_dic(d,'M8006C200')
        a_erab_req_qci6 = get_value_in_dic(d,'M8006C193')+get_value_in_dic(d,'M8006C202')
        a_erab_req_qci7 = get_value_in_dic(d,'M8006C194')+get_value_in_dic(d,'M8006C203')
        a_erab_req_qci8 = get_value_in_dic(d,'M8006C195')+get_value_in_dic(d,'M8006C204')
        a_erab_req_qci9 = get_value_in_dic(d,'M8006C196')+get_value_in_dic(d,'M8006C205')
        a_erab_suc = (get_value_in_dic(d,'M8006C206')+get_value_in_dic(d,'M8006C215')+get_value_in_dic(d,'M8006C207')+get_value_in_dic(d,'M8006C216')+get_value_in_dic(d,'M8006C208')+get_value_in_dic(d,'M8006C217')+get_value_in_dic(d,'M8006C209')+get_value_in_dic(d,'M8006C218')+get_value_in_dic(d,'M8006C210')+get_value_in_dic(d,'M8006C219')+get_value_in_dic(d,'M8006C211')+get_value_in_dic(d,'M8006C220')+get_value_in_dic(d,'M8006C212')+get_value_in_dic(d,'M8006C221')+get_value_in_dic(d,'M8006C213')+get_value_in_dic(d,'M8006C222')+get_value_in_dic(d,'M8006C214')+get_value_in_dic(d,'M8006C223'))
        a_erab_suc_qci3 = get_value_in_dic(d,'M8006C208')+get_value_in_dic(d,'M8006C217')
        a_erab_suc_qci4 = get_value_in_dic(d,'M8006C209')+get_value_in_dic(d,'M8006C218')
        a_erab_suc_qci6 = get_value_in_dic(d,'M8006C211')+get_value_in_dic(d,'M8006C220')
        a_erab_suc_qci7 = get_value_in_dic(d,'M8006C212')+get_value_in_dic(d,'M8006C221')
        a_erab_suc_qci8 = get_value_in_dic(d,'M8006C213')+get_value_in_dic(d,'M8006C222')
        a_erab_suc_qci9 = get_value_in_dic(d,'M8006C214')+get_value_in_dic(d,'M8006C223')
        a_erab_congest = (get_value_in_dic(d,'M8006C245')+get_value_in_dic(d,'M8006C249')+get_value_in_dic(d,'M8006C244')+get_value_in_dic(d,'M8006C248'))
        a_s1_att = get_value_in_dic(d,'M8013C43')
        a_s1_suc = get_value_in_dic(d,'M8013C44')
        a_s1_suc_r = get_divide(get_value_in_dic(d,'M8013C44'),get_value_in_dic(d,'M8013C43'))
        a_erab_suc_r = get_divide((get_value_in_dic(d,'M8006C206')+get_value_in_dic(d,'M8006C215')+get_value_in_dic(d,'M8006C207')+get_value_in_dic(d,'M8006C216')+get_value_in_dic(d,'M8006C208')+get_value_in_dic(d,'M8006C217')+get_value_in_dic(d,'M8006C209')+get_value_in_dic(d,'M8006C218')+get_value_in_dic(d,'M8006C210')+get_value_in_dic(d,'M8006C219')+get_value_in_dic(d,'M8006C211')+get_value_in_dic(d,'M8006C220')+get_value_in_dic(d,'M8006C212')+get_value_in_dic(d,'M8006C221')+get_value_in_dic(d,'M8006C213')+get_value_in_dic(d,'M8006C222')+get_value_in_dic(d,'M8006C214')+get_value_in_dic(d,'M8006C223')),(get_value_in_dic(d,'M8006C188')+get_value_in_dic(d,'M8006C197')+get_value_in_dic(d,'M8006C189')+get_value_in_dic(d,'M8006C198')+get_value_in_dic(d,'M8006C190')+get_value_in_dic(d,'M8006C199')+get_value_in_dic(d,'M8006C191')+get_value_in_dic(d,'M8006C200')+get_value_in_dic(d,'M8006C192')+get_value_in_dic(d,'M8006C201')+get_value_in_dic(d,'M8006C193')+get_value_in_dic(d,'M8006C202')+get_value_in_dic(d,'M8006C194')+get_value_in_dic(d,'M8006C203')+get_value_in_dic(d,'M8006C195')+get_value_in_dic(d,'M8006C204')+get_value_in_dic(d,'M8006C196')+get_value_in_dic(d,'M8006C205')))
        a_erab_suc_r_qci1 = get_divide((get_value_in_dic(d,'M8006C206')+get_value_in_dic(d,'M8006C215')),(get_value_in_dic(d,'M8006C188')+get_value_in_dic(d,'M8006C197')))
        a_erab_suc_r_qci2 = get_divide((get_value_in_dic(d,'M8006C207')+get_value_in_dic(d,'M8006C216')),(get_value_in_dic(d,'M8006C189')+get_value_in_dic(d,'M8006C198')))
        a_erab_suc_r_qci3 = get_divide((get_value_in_dic(d,'M8006C208')+get_value_in_dic(d,'M8006C217')),(get_value_in_dic(d,'M8006C190')+get_value_in_dic(d,'M8006C199')))
        a_erab_suc_r_qci4 = get_divide((get_value_in_dic(d,'M8006C209')+get_value_in_dic(d,'M8006C218')),(get_value_in_dic(d,'M8006C191')+get_value_in_dic(d,'M8006C200')))
        a_erab_suc_r_qci5 = get_divide((get_value_in_dic(d,'M8048C1')+get_value_in_dic(d,'M8048C6')),(get_value_in_dic(d,'M8048C0')+get_value_in_dic(d,'M8048C5')))
        a_radio_suc_r = a_rrc_suc_r * a_erab_suc_r
        a_erab_abnormal = (get_value_in_dic(d,'M8006C256')+get_value_in_dic(d,'M8006C259')+get_value_in_dic(d,'M8006C257')+get_value_in_dic(d,'M8006C261')+get_value_in_dic(d,'M8006C277'))
        a_context_release_abnormal = (get_value_in_dic(d,'M8006C256')+get_value_in_dic(d,'M8006C259')+get_value_in_dic(d,'M8006C257')+get_value_in_dic(d,'M8006C261')+get_value_in_dic(d,'M8006C277'))
        a_context_release_normal = (get_value_in_dic(d,'M8006C255')+get_value_in_dic(d,'M8006C258')+get_value_in_dic(d,'M8006C260'))
        a_lte_drop_r = get_divide((get_value_in_dic(d,'M8006C256')+get_value_in_dic(d,'M8006C259')+get_value_in_dic(d,'M8006C257')+get_value_in_dic(d,'M8006C261')+get_value_in_dic(d,'M8006C277')),(get_value_in_dic(d,'M8006C255')+get_value_in_dic(d,'M8006C258')+get_value_in_dic(d,'M8006C260')))
        a_lte_release = (get_value_in_dic(d,'M8006C255')+get_value_in_dic(d,'M8006C258')+get_value_in_dic(d,'M8006C260'))
        a_ho_pingpang = ""
        a_ho_out_suc_r = get_divide(get_value_in_dic(d,'M8014C19')+get_value_in_dic(d,'M8014C7'),get_value_in_dic(d,'M8014C14')+get_value_in_dic(d,'M8014C0'))
        a_ul_tra_mb = get_value_in_dic(d,'M8012C19')/1000000
        a_dl_tra_mb = get_value_in_dic(d,'M8012C20')/1000000
        a_total_tra_mb = get_value_in_dic(d,'M8012C19')/1000000+get_value_in_dic(d,'M8012C20')/1000000
        a_rrc_fail_license = ""
        a_rrc_conn_suc = a_rrc_suc
        dk = max(get_value_in_dic(d,'M8001C216'),get_value_in_dic(d,'M8001C217'))
        if dk > 75:
            prb_total = 100
        elif dk > 50:
            prb_total = 75
        elif dk > 25:
            prb_total = 50
        elif dk > 15:
            prb_total = 25
        elif dk > 6:
            prb_total = 15
        else:
            prb_total = 6
        
        prb_keyong = prb_total * get_divide(get_value_in_dic(d,'M8020C3'),(60*interval))
        a_ul_prb_utilization = get_divide(get_value_in_dic(d,'M8011C47'),(interval*60*1000)*prb_keyong)
        a_dl_prb_utilization = get_divide(get_value_in_dic(d,'M8011C51'),(interval*60*1000)*prb_keyong)
        a_rrc_congest_r_license = ""
        a_shuangliubi = get_divide(get_value_in_dic(d,'M8010C113'),(get_value_in_dic(d,'M8010C112')+get_value_in_dic(d,'M8010C113')))
        a_radio_utilization = ""
        a_sgnb_add_req = ""
        a_sgnb_add_suc = ""
        a_sgnb_add_r = ""
        a_dl_16qam_utilization = get_divide(100*(get_value_in_dic(d,'M8001C55')+get_value_in_dic(d,'M8001C56')+get_value_in_dic(d,'M8001C57')+get_value_in_dic(d,'M8001C58')+get_value_in_dic(d,'M8001C59')+get_value_in_dic(d,'M8001C60')+get_value_in_dic(d,'M8001C61')),get_value_in_dic(d,'M8012C90'))
        a_dl_64qam_utilization = get_divide(100*(get_value_in_dic(d,'M8001C62')+get_value_in_dic(d,'M8001C63')+get_value_in_dic(d,'M8001C64')+get_value_in_dic(d,'M8001C65')+get_value_in_dic(d,'M8001C66')+get_value_in_dic(d,'M8001C67')+get_value_in_dic(d,'M8001C68')+get_value_in_dic(d,'M8001C69')+get_value_in_dic(d,'M8001C70')+get_value_in_dic(d,'M8001C71')+get_value_in_dic(d,'M8001C72')+get_value_in_dic(d,'M8001C73')),get_value_in_dic(d,'M8012C90'))
        a_erab_congest_radio = get_value_in_dic(d,'M8006C244')+get_value_in_dic(d,'M8006C248')
        a_erab_congest_trans = get_value_in_dic(d,'M8006C245')+get_value_in_dic(d,'M8006C249')
        a_erab_fail_ue = get_value_in_dic(d,'M8006C246')+get_value_in_dic(d,'M8006C250')
        a_erab_fail_core = get_value_in_dic(d,'M8006C252')
        a_erab_fail_trans = get_value_in_dic(d,'M8006C245')+get_value_in_dic(d,'M8006C249')
        a_erab_fail_radio = get_value_in_dic(d,'M8006C247')+get_value_in_dic(d,'M8006C251')
        a_erab_fail_resource = get_value_in_dic(d,'M8006C244')+get_value_in_dic(d,'M8006C248')
        a_rrc_release_csfb = get_value_in_dic(d,'M8016C11')
        a_dl_highmsc_r = get_divide((get_value_in_dic(d,'M8001C62')+get_value_in_dic(d,'M8001C63')+get_value_in_dic(d,'M8001C64')+get_value_in_dic(d,'M8001C65')+get_value_in_dic(d,'M8001C66')+get_value_in_dic(d,'M8001C67')+get_value_in_dic(d,'M8001C68')+get_value_in_dic(d,'M8001C69')+get_value_in_dic(d,'M8001C70')+get_value_in_dic(d,'M8001C72')+get_value_in_dic(d,'M8001C73')),(get_value_in_dic(d,'M8001C45')+get_value_in_dic(d,'M8001C46')+get_value_in_dic(d,'M8001C47')+get_value_in_dic(d,'M8001C48')+get_value_in_dic(d,'M8001C49')+get_value_in_dic(d,'M8001C50')+get_value_in_dic(d,'M8001C51')+get_value_in_dic(d,'M8001C52')+get_value_in_dic(d,'M8001C53')+get_value_in_dic(d,'M8001C54')+get_value_in_dic(d,'M8001C55')+get_value_in_dic(d,'M8001C56')+get_value_in_dic(d,'M8001C57')+get_value_in_dic(d,'M8001C58')+get_value_in_dic(d,'M8001C59')+get_value_in_dic(d,'M8001C60')+get_value_in_dic(d,'M8001C61')+get_value_in_dic(d,'M8001C62')+get_value_in_dic(d,'M8001C63')+get_value_in_dic(d,'M8001C64')+get_value_in_dic(d,'M8001C65')+get_value_in_dic(d,'M8001C66')+get_value_in_dic(d,'M8001C67')+get_value_in_dic(d,'M8001C68')+get_value_in_dic(d,'M8001C69')+get_value_in_dic(d,'M8001C70')+get_value_in_dic(d,'M8001C71')+get_value_in_dic(d,'M8001C72')+get_value_in_dic(d,'M8001C73')))
        a_ul_highmsc_r = get_divide((get_value_in_dic(d,'M8001C91')+get_value_in_dic(d,'M8001C92')+get_value_in_dic(d,'M8001C93')+get_value_in_dic(d,'M8001C94')+get_value_in_dic(d,'M8001C95')+get_value_in_dic(d,'M8001C96')+get_value_in_dic(d,'M8001C97')+get_value_in_dic(d,'M8001C98')+get_value_in_dic(d,'M8001C99')+get_value_in_dic(d,'M8001C100')+get_value_in_dic(d,'M8001C101')+get_value_in_dic(d,'M8001C102')),(get_value_in_dic(d,'M8001C74')+get_value_in_dic(d,'M8001C75')+get_value_in_dic(d,'M8001C76')+get_value_in_dic(d,'M8001C77')+get_value_in_dic(d,'M8001C78')+get_value_in_dic(d,'M8001C79')+get_value_in_dic(d,'M8001C80')+get_value_in_dic(d,'M8001C81')+get_value_in_dic(d,'M8001C82')+get_value_in_dic(d,'M8001C83')+get_value_in_dic(d,'M8001C84')+get_value_in_dic(d,'M8001C85')+get_value_in_dic(d,'M8001C86')+get_value_in_dic(d,'M8001C87')+get_value_in_dic(d,'M8001C88')+get_value_in_dic(d,'M8001C89')+get_value_in_dic(d,'M8001C90')+get_value_in_dic(d,'M8001C91')+get_value_in_dic(d,'M8001C92')+get_value_in_dic(d,'M8001C93')+get_value_in_dic(d,'M8001C94')+get_value_in_dic(d,'M8001C95')+get_value_in_dic(d,'M8001C96')+get_value_in_dic(d,'M8001C97')+get_value_in_dic(d,'M8001C98')+get_value_in_dic(d,'M8001C99')+get_value_in_dic(d,'M8001C100')+get_value_in_dic(d,'M8001C101')+get_value_in_dic(d,'M8001C102')))
        a_csfb_suc = ""
        a_ho_s1_out_req = get_value_in_dic(d,'M8014C14')
        a_ho_s1_out_suc = get_value_in_dic(d,'M8014C19')
        a_ho_x2_out_req = get_value_in_dic(d,'M8014C0')
        a_ho_x2_out_suc = get_value_in_dic(d,'M8014C7')
        a_erab_req_qci1 = get_value_in_dic(d,'M8006C188') + get_value_in_dic(d,'M8006C197')
        a_erab_req_qci5 = get_value_in_dic(d,'M8048C0') + get_value_in_dic(d,'M8048C5')
        a_erab_suc_qci1 = get_value_in_dic(d,'M8006C206') + get_value_in_dic(d,'M8006C215')
        a_erab_suc_qci5 = get_value_in_dic(d,'M8048C1') + get_value_in_dic(d,'M8048C6')
        a_erab_normal_qci1 = get_value_in_dic(d,'M8006C276')+get_value_in_dic(d,'M8006C278')+get_value_in_dic(d,'M8006C317')+get_value_in_dic(d,'M8006C319')+get_value_in_dic(d,'M8006C267')+get_value_in_dic(d,'M8006C270')+get_value_in_dic(d,'M8006C89')+get_value_in_dic(d,'M8006C98')+get_value_in_dic(d,'M8006C304')+get_value_in_dic(d,'M8006C307')+get_value_in_dic(d,'M8006C315')+get_value_in_dic(d,'M8006C273')
        a_erab_abnormal_qci1 = (get_value_in_dic(d,'M8006C272')+get_value_in_dic(d,'M8006C316')+get_value_in_dic(d,'M8006C318')+get_value_in_dic(d,'M8006C320')+get_value_in_dic(d,'M8006C268')+get_value_in_dic(d,'M8006C269')+get_value_in_dic(d,'M8006C271')+get_value_in_dic(d,'M8006C280')-get_value_in_dic(d,'M8006C301'))
        a_lte_drop_r_qci1 = get_divide(a_erab_abnormal_qci1,(a_erab_normal_qci1 + a_erab_abnormal_qci1))
        a_lru_blind = get_value_in_dic(d,'M8013C76')
        a_lru_not_blind = 0
        #以下是2022-4-7新添加内容
        a_succoutintraenb = get_value_in_dic(d,'M8009C7')
        a_attoutintraenb = get_value_in_dic(d,'M8009C6')
        a_rru_puschprbassn = get_value_in_dic(d,'M8011C50')/(interval*60*1000)
        #原公式a_rru_puschprbtot = (ulChBw/2)*(10*M8020C3/60*interval)
        #ulChBw为cm，用prb_total推算{100:20,75:15,50:10,25:5,15:3,6:1.4}
        chBw={100:20,75:15,50:10,25:5,15:3,6:1.4}
        a_rru_puschprbtot = (chBw[prb_total]/2)*(10*get_value_in_dic(d,'M8020C3')/(60*interval))
        a_rru_pdschprbassn = get_value_in_dic(d,'M8011C54')/(interval*60*1000)
        a_rru_pdschprbtot = (chBw[prb_total]/2)*(10*get_value_in_dic(d,'M8020C3')/(60*interval))
        a_effectiveconnmean = get_value_in_dic(d,'M8051C55')
        a_effectiveconnmax = get_value_in_dic(d,'M8051C56')
        #PDCCH信道CCE可用个数
        #a_rru_pdcchcceavail = 5M:(20*1000*60*interval)*(10*M8020C3/60*interval)
        #                    10M:(41*1000*60*interval)*(10*M8020C3/60*interval)
        #                    15M:(62*1000*60*interval)*(10*M8020C3/60*interval)
        #                    20M:(84*1000*60*interval)*(10*M8020C3/60*interval)
        a_rru_pdcchcceavail = (lambda prb_total: 84 if prb_total==100 else (62 if prb_total==75 else (41 if prb_total==50 else (20 if prb_total==25 else 0))))(prb_total)*1000*60*interval*(10*get_value_in_dic(d,'M8020C3')/(60*interval))
        #PDCCH信道CCE占用个数
        a_rru_pdcchcceutil = get_value_in_dic(d,'M8011C39')+2*get_value_in_dic(d,'M8011C40')+4*get_value_in_dic(d,'M8011C41')+8*get_value_in_dic(d,'M8011C42')
        #a_pdcch_signal_occupy_ratio = 100%*sum(M8011C39+2*M8011C40+4*M8011C41+8*M8011C42)/PDCCH信道CCE可分配个数
        a_pdcch_signal_occupy_ratio = get_divide(a_rru_pdcchcceutil,a_rru_pdcchcceavail)
        #a_succexecinc = sum(M8015C2)统计目标小区+sum(M8021C30-M8021C35-M8021C36)
        #M8015C2没有办法统计
        a_succexecinc = get_value_in_dic(d,'M8021C30')-get_value_in_dic(d,'M8021C35')-get_value_in_dic(d,'M8021C36')
        a_succconnreestab_nonsrccell = get_value_in_dic(d,'M8008C5')
        a_rrc_reconn_rate = get_divide(get_value_in_dic(d,'M8008C4'),a_rrc_req)
        a_enb_handover_succ_rate = get_divide(get_value_in_dic(d,'M8009C7'),get_value_in_dic(d,'M8009C6'))
        #a_down_pdcch_ch_cce_occ_rate = 100%*sum(M8011C39+2*M8011C40+4*M8011C41+8*M8011C42)/PDCCH信道CCE可分配个数
        a_down_pdcch_ch_cce_occ_rate = a_pdcch_signal_occupy_ratio
        a_down_pdcp_sdu_avg_delay = get_value_in_dic(d,'M8001C2')
        a_mr_sinrul_gt0_ratio = 0 #应答公式为空
        a_mr_sinrul_gt0_ratio_fz = 0 #应答公式为空
        a_mr_sinrul_gt0_ratio_fm = 0 #应答公式为空
        a_vendor = 'NOKIA'
        a_lte_wireless_drop_ratio_cell = a_lte_drop_r #应答公式为空
        pm = [starttime,cell,a_tuifu,a_tongji,a_keyonglv,a_ganrao,a_cqi0,a_cqi1,a_cqi2,a_cqi3,a_cqi4,a_cqi5,a_cqi6,a_cqi7,a_cqi8,a_cqi9,a_cqi10,a_cqi11,a_cqi12,a_cqi13,a_cqi14,a_cqi15,a_cqi,a_cqigt7,a_cqilt7,a_ul_sdu_lost,a_ul_sdu_total,a_dl_sdu_lost,a_dl_sdu_total,a_ul_sdu_diubaolv,a_ul_sdu_diubaolv_qci1,a_dl_sdu_diubaolv_qci1,a_dl_sdu_qibaolv,a_ul_mbps,a_dl_mbps,a_rrc_req,a_rrc_suc,a_rrc_congest,a_rrc_avg,a_rrc_max,a_erab_req,a_erab_req_qci3,a_erab_req_qci4,a_erab_req_qci6,a_erab_req_qci7,a_erab_req_qci8,a_erab_req_qci9,a_erab_suc,a_erab_suc_qci3,a_erab_suc_qci4,a_erab_suc_qci6,a_erab_suc_qci7,a_erab_suc_qci8,a_erab_suc_qci9,a_erab_congest,a_s1_att,a_s1_suc,a_s1_suc_r,a_erab_suc_r,a_erab_suc_r_qci1,a_erab_suc_r_qci2,a_erab_suc_r_qci3,a_erab_suc_r_qci4,a_erab_suc_r_qci5,a_radio_suc_r,a_erab_abnormal,a_context_release_abnormal,a_context_release_normal,a_lte_drop_r,a_lte_release,a_ho_pingpang,a_ho_out_suc_r,a_ul_tra_mb,a_dl_tra_mb,a_total_tra_mb,a_rrc_fail_license,a_rrc_conn_suc,a_ul_prb_utilization,a_dl_prb_utilization,a_rrc_congest_r_license,a_shuangliubi,a_radio_utilization,a_sgnb_add_req,a_sgnb_add_suc,a_sgnb_add_r,a_dl_16qam_utilization,a_dl_64qam_utilization,a_erab_congest_radio,a_erab_congest_trans,a_erab_fail_ue,a_erab_fail_core,a_erab_fail_trans,a_erab_fail_radio,a_erab_fail_resource,a_rrc_release_csfb,a_dl_highmsc_r,a_ul_highmsc_r,a_csfb_suc,a_ho_s1_out_req,a_ho_s1_out_suc,a_ho_x2_out_req,a_ho_x2_out_suc,a_erab_req_qci1,a_erab_req_qci5,a_erab_suc_qci1,a_erab_suc_qci5,a_erab_normal_qci1,a_erab_abnormal_qci1,a_lte_drop_r_qci1,a_lru_blind,a_lru_not_blind,
              a_succoutintraenb,a_attoutintraenb,
              a_rru_puschprbassn,a_rru_puschprbtot,a_rru_pdschprbassn,a_rru_pdschprbtot,a_effectiveconnmean,a_effectiveconnmax,a_pdcch_signal_occupy_ratio,a_rru_pdcchcceutil,a_rru_pdcchcceavail,a_succexecinc,
              a_succconnreestab_nonsrccell,a_rrc_reconn_rate,a_enb_handover_succ_rate,a_down_pdcch_ch_cce_occ_rate,a_down_pdcp_sdu_avg_delay,a_mr_sinrul_gt0_ratio,a_mr_sinrul_gt0_ratio_fz,a_mr_sinrul_gt0_ratio_fm,a_vendor,a_lte_wireless_drop_ratio_cell]
        return pm





def deal_with_file(fname):
    try:
        if 'LNBTS' in fname or 'MRBTS' in fname:
            # print(fname)
            with gzip.open(fname, 'rt') as ff:
                text = ff.read()
                #d={}
                # if 'UTF-8' in text:
                # tree = ET.parse(text)
                # root=tree.getroot()
                root = ET.fromstring(text)
                for i in root:
                    d={}
                    starttime = i.attrib['startTime']
                    interval = i.attrib['interval']
                    if not isinstance(i.find("PMMOResult"),type(None)) and int(interval)==15:
                        PMSetup = i.findall("PMMOResult")
                        for PMMOResult in PMSetup:
                            cell = PMMOResult.find('MO').find('DN').text
                            if not cell in d:
                                d[cell]={}
                            PMTarget = PMMOResult.findall("PMTarget")
                            for m in PMTarget:
                                for c in a_counters:
                                    x = c
                                    if not isinstance(m.find(x),type(None)):
                                        v = m.find(x).text
                                        # print(x+':'+v)
                                        d[cell][c] = int(v)

                        for cell in d:
                            if len(d[cell]) > 0:
                                # print(cell)
                                # print(d[cell])
                                pm = deal_with_kpi(cell,d[cell],starttime,interval)
                                if pm == 0:
                                    pass
                                else:
                                    # print(pm)
                                    kpi.append(pm)
                                    # print('已解析'+str(len(kpi))+'个小区')
                    # else:
                    #     print(fname)
    except Exception as e: #xml.etree.ElementTree.ParseError:
        # print(fname)
        # print(e.__class__.__name__,e)
        logger.warning("文件:%s,错误信息:%s,%s" % (fname,e.__class__.__name__,e))
        pass


if __name__ == '__main__':
    at = time.time()
    logger.info("开始处理%sPM指标" % d0)
    manager = Manager()
    kpi = manager.list()
    pool = Pool(20)
    for i in range(1,6):
        pm_path = '/data/esbftp/pm/4G/NOKIA/OMC'+str(i)+'/PM/'+d0+'/'
        print(pm_path)
        if os.path.isdir(pm_path):
            for root,dirs,files in os.walk(pm_path):
                pool.map(deal_with_file,[os.path.join(pm_path, f) for f in files])
    
    with open(output_name+'.temp', 'w',newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(kpi)
    os.rename(output_name+'.temp',output_name)
    bt = time.time()
    print(bt-at)
    logger.info("处理完成，共%d行数据" % len(kpi))
