# -*- coding: utf-8 -*-
"""
运行时加参数 格式为 202110201400
传到服务器时  修改 D: 为''
@author: qianda
"""
from hashlib import new
import logging
from time import sleep
import os
import sys
import glob
import gzip
import time
import shutil
import logging
import datetime
from logging.handlers import TimedRotatingFileHandler
import csv
at = time.time()
in_path = "/data/esbftp/dianxin/pm/4G/NOKIA"
out_path = "/data/output/pm/dianxin_nokia"
out_path_file = out_path+"/4g"
temp_path = out_path+".tmp_4g_pm"
log_path = '/home/nokia/shell/log/nk-4g-pm.log'
new_formatter = '[%(levelname)s]%(asctime)s:%(msecs)s.%(process)d,%(thread)d#>[%(funcName)s]:%(lineno)s  %(message)s'
#fmt = logging.Formatter(new_formatter)
#(log_path, when='D',backupCount=3)
#log_handel.setFormatter(fmt)

fileshandle = logging.handlers.TimedRotatingFileHandler(log_path, when='D', interval=1, backupCount=7,
                                                            encoding='utf-8')
fileshandle.suffix = "%Y%m%d.log"
fileshandle.setLevel(logging.INFO)
formatter = logging.Formatter(new_formatter)
fileshandle.setFormatter(formatter)
logger = logging.getLogger('')
logger.setLevel(logging.INFO)
logger.addHandler(fileshandle)

mins_time = []

# tar.gz文件解压
def un_gz(file_name, targetfile):
    # 获取文件的名称，去掉后缀名
    try:
        # 开始解压
        g_file = gzip.GzipFile(file_name)
        # 读取解压后的文件，并写入去掉后缀名的同名文件（即得到解压后的文件）
        open(targetfile, "wb+").write(g_file.read())
        return True

    except Exception as e:
        logging.info(e)
        return False

###时间判断
hourdelay = 1
if len(sys.argv) > 1:
    if len(sys.argv[1]) != 12:
        print('需要年月日时分12位数字，比如202111161115')
        sys.exit()
    else:
        print(sys.argv[1][-2:])
        if sys.argv[1][-2:] not in ['00','15','30','45']:
            print('最后两位需要15分钟的整数，比如00，15，30，45')
            sys.exit()
        else:
            d0 = sys.argv[1]
else:
    d1 = (datetime.datetime.now()-datetime.timedelta(hours=hourdelay)).strftime('%Y%m%d%H')
    d2 = (datetime.datetime.now()-datetime.timedelta(hours=hourdelay)).strftime('%M')
    d3 = ('00'+str(int(int(d2)/15)*15))[-2:]
    d0 = d1+d3
output_name = os.path.join(out_path_file,'nokia_4g_dx_'+d0+'.csv')    
#获取now-2 之前1小时的数据
def gettime():
    now_time = datetime.datetime.now().strftime('%Y-%m-%d %H')
    timeArray = time.strptime(now_time, "%Y-%m-%d %H")
    # 本地时间的时间戳，这里剪掉了3个小时
    now_time_stamp = int(time.mktime(timeArray)) - 3600 * 3
    #print(datetime.datetime.now()- datetime.timedelta(hours=2).strftime('%Y-%m-%d %H'))
    fmt = '%Y%m%d%H'
    return time.strftime(fmt, time.localtime(now_time_stamp))

def get_value(val):
    if len(val.strip())>0:
        return float(val.strip())
    else:
        return 0

def get_str(val):
    if len(val.strip()) > 0:
        return str(val.strip())
    else:
        return ''            
def get_devide(a,b,c=4):
    if b==0:
        return 0
    else:
        return round(a/b,c)   

def split_line(line):
    sep = '|'
    cols = line.split(sep)
    
    dn =  get_str(cols[0])
    related_enb_dn =  get_str(cols[1])
    related_enb_id =  get_value(cols[2])
    related_enb_userlabel =  get_str(cols[3])
    cel_id =  get_value(cols[4])
    cel_id_local =  get_str(cols[5])
    userlabel =  get_str(cols[6])
    freq_mode =  get_value(cols[7])
    RRC_AttConReestab_OtherCause =  get_value(cols[8])
    RRC_AttConReestab_HoFail =  get_value(cols[9])
    RRC_AttConReestab_reconFail =  get_value(cols[10])
    RRC_AttConnEstab_UE =  get_value(cols[11])
    RRC_AttConnEstab_Net =  get_value(cols[12])
    RRC_AttConnReestab =  get_value(cols[13])
    RRC_FailConnEstab_CellReject =  get_value(cols[14])
    RRC_FailConnEstab_UeNoReply =  get_value(cols[15])
    RRC_FailConnEstab_OtherCause =  get_value(cols[16])
    RRC_SuccConnEstab_UE =  get_value(cols[17])
    RRC_SuccConnEstab_Net =  get_value(cols[18])
    RRC_SuccConnReestab =  get_value(cols[19])
    RRC_UserConnMax =  get_value(cols[20])
    RRC_UserConnMean =  get_value(cols[21])
    RRC_ActiveMaxNbrPCellDl =  get_value(cols[22])
    RRC_ActiveMeanNbrPCellDl =  get_value(cols[23])
    RRC_MaxCACapUser =  get_value(cols[24])
    RRC_MeanCACapUser =  get_value(cols[25])
    RRC_MeanNbrCoMPUl =  get_value(cols[26])
    RRC_MaxNbrCoMPUl =  get_value(cols[27])
    HO_AttOutInterFreq =  get_value(cols[28])
    HO_AttOutIntraEnb_FT =  get_value(cols[29])
    HO_SuccOutInterFreq =  get_value(cols[30])
    HO_SuccOutIntraEnb_FT =  get_value(cols[31])
    HO_AttOutX2_FT =  get_value(cols[32])
    HO_SuccOutX2_FT =  get_value(cols[33])
    HO_AttOutS1_FT =  get_value(cols[34])
    HO_SuccOutS1_FT =  get_value(cols[35])
    HO_AttOutLTETo3G =  get_value(cols[36])
    HO_AttOutIntra =  get_value(cols[37])
    HO_AttOutIntraFreq =  get_value(cols[38])
    HO_SuccOutIntraEnb =  get_value(cols[39])
    HO_SuccOutIntraFreq =  get_value(cols[40])
    HO_AttOutX2 =  get_value(cols[41])
    HO_AvgDuration_X2 =  get_value(cols[42])
    HO_SuccOutX2 =  get_value(cols[43])
    HO_AttOutS1 =  get_value(cols[44])
    HO_AvgDuration_S1 =  get_value(cols[45])
    HO_Fail_TimeOut =  get_value(cols[46])
    HO_Fail_Prep =  get_value(cols[47])
    HO_SuccOutS1 =  get_value(cols[48])
    DtchPrbAssnMeanUl =  get_value(cols[49])
    DtchPrbAssnMeanUl_QCI1 =  get_value(cols[50])
    DtchPrbAssnMeanUl_QCI2 =  get_value(cols[51])
    DtchPrbAssnMeanUl_QCI3 =  get_value(cols[52])
    DtchPrbAssnMeanUl_QCI4 =  get_value(cols[53])
    DtchPrbAssnMeanUl_QCI5 =  get_value(cols[54])
    DtchPrbAssnMeanUl_QCI6 =  get_value(cols[55])
    DtchPrbAssnMeanUl_QCI7 =  get_value(cols[56])
    DtchPrbAssnMeanUl_QCI8 =  get_value(cols[57])
    DtchPrbAssnMeanUl_QCI9 =  get_value(cols[58])
    PRB89_InterfNoiseV =  get_value(cols[59])
    PRB88_InterfNoiseV =  get_value(cols[60])
    PRB96_InterfNoiseV =  get_value(cols[61])
    PRB95_InterfNoiseV =  get_value(cols[62])
    PRB10_InterfNoiseV =  get_value(cols[63])
    PRB98_InterfNoiseV =  get_value(cols[64])
    PRB97_InterfNoiseV =  get_value(cols[65])
    PRB92_InterfNoiseV =  get_value(cols[66])
    RRU_MeanCoMPPrbNbrUl =  get_value(cols[67])
    RRU_PCellPdschPrbNbrDl =  get_value(cols[68])
    RRU_SCellPdschPrbNbrDl =  get_value(cols[69])
    PRB91_InterfNoiseV =  get_value(cols[70])
    PRB94_InterfNoiseV =  get_value(cols[71])
    PRB93_InterfNoiseV =  get_value(cols[72])
    DC_SuccSgNBAddCount =  get_value(cols[73])
    DtchPrbAssnMeanDl =  get_value(cols[74])
    DtchPrbAssnMeanDl_QCI1 =  get_value(cols[75])
    DtchPrbAssnMeanDl_QCI2 =  get_value(cols[76])
    DtchPrbAssnMeanDl_QCI3 =  get_value(cols[77])
    DtchPrbAssnMeanDl_QCI4 =  get_value(cols[78])
    DtchPrbAssnMeanDl_QCI5 =  get_value(cols[79])
    DtchPrbAssnMeanDl_QCI6 =  get_value(cols[80])
    DtchPrbAssnMeanDl_QCI7 =  get_value(cols[81])
    DtchPrbAssnMeanDl_QCI8 =  get_value(cols[82])
    DtchPrbAssnMeanDl_QCI9 =  get_value(cols[83])
    PRB90_InterfNoiseV =  get_value(cols[84])
    S1Sig_AttConnEstab =  get_value(cols[85])
    S1Sig_SuccConnEstab =  get_value(cols[86])
    RANK2_DlRp_Num =  get_value(cols[87])
    PRB19_InterfNoiseV =  get_value(cols[88])
    RANK3_DlRp_Num =  get_value(cols[89])
    RANK1_DlRp_Num =  get_value(cols[90])
    CQI14 =  get_value(cols[91])
    PRB16_InterfNoiseV =  get_value(cols[92])
    CQI15 =  get_value(cols[93])
    PRB15_InterfNoiseV =  get_value(cols[94])
    CQI12 =  get_value(cols[95])
    PRB18_InterfNoiseV =  get_value(cols[96])
    RANK4_DlRp_Num =  get_value(cols[97])
    CQI13 =  get_value(cols[98])
    PRB17_InterfNoiseV =  get_value(cols[99])
    CQI10 =  get_value(cols[100])
    PRB12_InterfNoiseV =  get_value(cols[101])
    CQI11 =  get_value(cols[102])
    PRB11_InterfNoiseV =  get_value(cols[103])
    PRB99_InterfNoiseV =  get_value(cols[104])
    MAC_ActSCellAtt =  get_value(cols[105])
    MAC_ActSCellSucc =  get_value(cols[106])
    PRB14_InterfNoiseV =  get_value(cols[107])
    PRB13_InterfNoiseV =  get_value(cols[108])
    DlSCG_TxBytes =  get_value(cols[109])
    PRB21_InterfNoiseV =  get_value(cols[110])
    PRB20_InterfNoiseV =  get_value(cols[111])
    VlteVoice_time =  get_value(cols[112])
    VlteVoice_SduTotDelDl0_20 =  get_value(cols[113])
    VlteVoice_SduTotDelDl100_inf =  get_value(cols[114])
    VlteVoice_SduTotDelDl20_50 =  get_value(cols[115])
    VlteVoice_SduTotDelDl50_100 =  get_value(cols[116])
    VlteVoice_SduTotDelDlBf0_20 =  get_value(cols[117])
    VlteVoice_SduTotDelDlBf100_inf =  get_value(cols[118])
    VlteVoice_SduTotDelDlBf20_50 =  get_value(cols[119])
    VlteVoice_SduTotDelDlBf50_100 =  get_value(cols[120])
    VlteVoice_HOAttOutInterFrq =  get_value(cols[121])
    VlteVoice_HOAttOutIntraEnb =  get_value(cols[122])
    VlteVoice_HOAttOutIntraFrq =  get_value(cols[123])
    VlteVoice_HOAttOutS1 =  get_value(cols[124])
    VlteVoice_HOAttOutX2 =  get_value(cols[125])
    VlteVoice_HOSucOutInterFrq =  get_value(cols[126])
    VlteVoice_HOSucOutIntraFrq =  get_value(cols[127])
    VlteVoice_HOSuccOutIntraEnb =  get_value(cols[128])
    VlteVoice_HOSuccOutS1 =  get_value(cols[129])
    VlteVoice_HOSuccOutX2 =  get_value(cols[130])
    VlteVoice_MaxUser =  get_value(cols[131])
    VlteVoice_PrbTotDl =  get_value(cols[132])
    VlteVoice_PrbTotUl =  get_value(cols[133])
    VlteVoice_DispTotTbDl =  get_value(cols[134])
    VlteVoice_DispTotTbUl =  get_value(cols[135])
    VlteVoice_SemiContDispBegTbDl =  get_value(cols[136])
    VlteVoice_SemiContDispBegTbUl =  get_value(cols[137])
    UlSCG_RxBytes =  get_value(cols[138])
    DC_SgNBAddTimeMean =  get_value(cols[139])
    ARQ_RlcRecPktUl =  get_value(cols[140])
    ARQ_RlcRecRePktUl =  get_value(cols[141])
    ARQ_RlcSentPktDl =  get_value(cols[142])
    ARQ_RlcSentRePktDl =  get_value(cols[143])
    PDCP_SduTotDelayDl =  get_value(cols[144])
    PDCP_SduTotDelayDl_QCI8 =  get_value(cols[145])
    PDCP_SduTotDelayDl_QCI7 =  get_value(cols[146])
    PDCP_SduTotDelayDl_QCI9 =  get_value(cols[147])
    PDCP_SduTotDelayDl_Buf =  get_value(cols[148])
    PDCP_SduTotDelayDl_Buf_QCI2 =  get_value(cols[149])
    PDCP_SduTotDelayDl_Buf_QCI1 =  get_value(cols[150])
    PDCP_SduTotDelayDl_QCI4 =  get_value(cols[151])
    PDCP_SduTotDelayDl_Buf_QCI8 =  get_value(cols[152])
    PDCP_SduTotDelayDl_QCI3 =  get_value(cols[153])
    PDCP_SduTotDelayDl_Buf_QCI7 =  get_value(cols[154])
    PDCP_SduTotDelayDl_QCI6 =  get_value(cols[155])
    PDCP_SduTotDelayDl_QCI5 =  get_value(cols[156])
    PDCP_SduTotDelayDl_Buf_QCI9 =  get_value(cols[157])
    PDCP_SduTotDelayDl_Buf_QCI4 =  get_value(cols[158])
    PDCP_SduTotDelayDl_Buf_QCI3 =  get_value(cols[159])
    PDCP_SduTotDelayDl_QCI2 =  get_value(cols[160])
    PDCP_SduTotDelayDl_Buf_QCI6 =  get_value(cols[161])
    PDCP_SduTotDelayDl_QCI1 =  get_value(cols[162])
    PDCP_SduTotDelayDl_Buf_QCI5 =  get_value(cols[163])
    PDCP_CpOctDl =  get_value(cols[164])
    PDCP_CpOctUl =  get_value(cols[165])
    PDCP_CaThrpTimeDl =  get_value(cols[166])
    PDCP_CaTotalBitrateDl =  get_value(cols[167])
    PDCP_SduOctDl =  get_value(cols[168])
    PDCP_SduOctDl_QCI8 =  get_value(cols[169])
    PDCP_SduOctDl_QCI7 =  get_value(cols[170])
    PDCP_SduOctDl_QCI9 =  get_value(cols[171])
    PDCP_SduOctDl_QCI4 =  get_value(cols[172])
    PDCP_SduOctDl_QCI3 =  get_value(cols[173])
    PDCP_SduOctDl_QCI6 =  get_value(cols[174])
    PDCP_SduOctDl_QCI5 =  get_value(cols[175])
    PDCP_SduOctDl_QCI2 =  get_value(cols[176])
    PDCP_SduOctDl_QCI1 =  get_value(cols[177])
    PDCP_SduOctUl =  get_value(cols[178])
    PDCP_SduOctUl_QCI8 =  get_value(cols[179])
    PDCP_SduOctUl_QCI7 =  get_value(cols[180])
    PDCP_SduOctUl_QCI9 =  get_value(cols[181])
    PDCP_SduOctUl_QCI4 =  get_value(cols[182])
    PDCP_SduOctUl_QCI3 =  get_value(cols[183])
    PDCP_SduOctUl_QCI6 =  get_value(cols[184])
    PDCP_SduOctUl_QCI5 =  get_value(cols[185])
    PDCP_SduOctUl_QCI2 =  get_value(cols[186])
    PDCP_SduOctUl_QCI1 =  get_value(cols[187])
    PDCP_SduTailFlowDl =  get_value(cols[188])
    PDCP_SduTailFlowDl_QCI8 =  get_value(cols[189])
    PDCP_SduTailFlowDl_QCI7 =  get_value(cols[190])
    PDCP_SduTailFlowDl_QCI9 =  get_value(cols[191])
    PDCP_SduTailFlowDl_QCI4 =  get_value(cols[192])
    PDCP_SduTailFlowDl_QCI3 =  get_value(cols[193])
    PDCP_SduTailFlowDl_QCI6 =  get_value(cols[194])
    PDCP_SduTailFlowDl_QCI5 =  get_value(cols[195])
    PDCP_SduTailFlowDl_QCI2 =  get_value(cols[196])
    PDCP_SduTailFlowDl_QCI1 =  get_value(cols[197])
    PDCP_SduTailFlowUl =  get_value(cols[198])
    PDCP_SduTailFlowUl_QCI8 =  get_value(cols[199])
    PDCP_SduTailFlowUl_QCI7 =  get_value(cols[200])
    PDCP_SduTailFlowUl_QCI9 =  get_value(cols[201])
    PDCP_SduTailFlowUl_QCI4 =  get_value(cols[202])
    PDCP_SduTailFlowUl_QCI3 =  get_value(cols[203])
    PDCP_SduTailFlowUl_QCI6 =  get_value(cols[204])
    PDCP_SduTailFlowUl_QCI5 =  get_value(cols[205])
    PDCP_SduTailFlowUl_QCI2 =  get_value(cols[206])
    PDCP_SduTailFlowUl_QCI1 =  get_value(cols[207])
    PDCP_TailTimeDl =  get_value(cols[208])
    PDCP_TailTimeDl_QCI8 =  get_value(cols[209])
    PDCP_TailTimeDl_QCI7 =  get_value(cols[210])
    PDCP_TailTimeDl_QCI9 =  get_value(cols[211])
    PDCP_TailTimeDl_QCI4 =  get_value(cols[212])
    PDCP_TailTimeDl_QCI3 =  get_value(cols[213])
    PDCP_TailTimeDl_QCI6 =  get_value(cols[214])
    PDCP_TailTimeDl_QCI5 =  get_value(cols[215])
    PDCP_TailTimeDl_QCI2 =  get_value(cols[216])
    PDCP_TailTimeDl_QCI1 =  get_value(cols[217])
    PDCP_TailTimeUl =  get_value(cols[218])
    PDCP_TailTimeUl_QCI8 =  get_value(cols[219])
    PDCP_TailTimeUl_QCI7 =  get_value(cols[220])
    PDCP_TailTimeUl_QCI9 =  get_value(cols[221])
    PDCP_TailTimeUl_QCI4 =  get_value(cols[222])
    PDCP_TailTimeUl_QCI3 =  get_value(cols[223])
    PDCP_TailTimeUl_QCI6 =  get_value(cols[224])
    PDCP_TailTimeUl_QCI5 =  get_value(cols[225])
    PDCP_TailTimeUl_QCI2 =  get_value(cols[226])
    PDCP_TailTimeUl_QCI1 =  get_value(cols[227])
    PDCP_ThrpTimeDl =  get_value(cols[228])
    PDCP_ThrpTimeDl_QCI8 =  get_value(cols[229])
    PDCP_ThrpTimeDl_QCI7 =  get_value(cols[230])
    PDCP_ThrpTimeDl_QCI9 =  get_value(cols[231])
    PDCP_ThrpTimeDl_QCI4 =  get_value(cols[232])
    PDCP_ThrpTimeDl_QCI3 =  get_value(cols[233])
    PDCP_ThrpTimeDl_QCI6 =  get_value(cols[234])
    PDCP_ThrpTimeDl_QCI5 =  get_value(cols[235])
    PDCP_ThrpTimeDl_QCI2 =  get_value(cols[236])
    PDCP_ThrpTimeDl_QCI1 =  get_value(cols[237])
    PDCP_ThrpTimeUl =  get_value(cols[238])
    PDCP_ThrpTimeUl_QCI8 =  get_value(cols[239])
    PDCP_ThrpTimeUl_QCI7 =  get_value(cols[240])
    PDCP_ThrpTimeUl_QCI9 =  get_value(cols[241])
    PDCP_ThrpTimeUl_QCI4 =  get_value(cols[242])
    PDCP_ThrpTimeUl_QCI3 =  get_value(cols[243])
    PDCP_ThrpTimeUl_QCI6 =  get_value(cols[244])
    PDCP_ThrpTimeUl_QCI5 =  get_value(cols[245])
    PDCP_ThrpTimeUl_QCI2 =  get_value(cols[246])
    PDCP_ThrpTimeUl_QCI1 =  get_value(cols[247])
    PDCP_CpPktDl =  get_value(cols[248])
    PDCP_CpPktUl =  get_value(cols[249])
    PDCP_SduDiscardPktDl =  get_value(cols[250])
    PDCP_SduDiscardPktDl_QCI8 =  get_value(cols[251])
    PDCP_SduDiscardPktDl_QCI7 =  get_value(cols[252])
    PDCP_SduDiscardPktDl_QCI9 =  get_value(cols[253])
    PDCP_SduDiscardPktDl_QCI4 =  get_value(cols[254])
    PDCP_SduDiscardPktDl_QCI3 =  get_value(cols[255])
    PDCP_SduDiscardPktDl_QCI6 =  get_value(cols[256])
    PDCP_SduDiscardPktDl_QCI5 =  get_value(cols[257])
    PDCP_SduDiscardPktDl_QCI2 =  get_value(cols[258])
    PDCP_SduDiscardPktDl_QCI1 =  get_value(cols[259])
    PDCP_SduLossPktDl =  get_value(cols[260])
    PDCP_SduLossPktDl_QCI8 =  get_value(cols[261])
    PDCP_SduLossPktDl_QCI7 =  get_value(cols[262])
    PDCP_SduLossPktDl_QCI9 =  get_value(cols[263])
    PDCP_SduLossPktDl_QCI4 =  get_value(cols[264])
    PDCP_SduLossPktDl_QCI3 =  get_value(cols[265])
    PDCP_SduLossPktDl_QCI6 =  get_value(cols[266])
    PDCP_SduLossPktDl_QCI5 =  get_value(cols[267])
    PDCP_SduLossPktDl_QCI2 =  get_value(cols[268])
    PDCP_SduLossPktDl_QCI1 =  get_value(cols[269])
    PDCP_SduLossPktUl =  get_value(cols[270])
    PDCP_SduLossPktUl_QCI8 =  get_value(cols[271])
    PDCP_SduLossPktUl_QCI7 =  get_value(cols[272])
    PDCP_SduLossPktUl_QCI9 =  get_value(cols[273])
    PDCP_SduLossPktUl_QCI4 =  get_value(cols[274])
    PDCP_SduLossPktUl_QCI3 =  get_value(cols[275])
    PDCP_SduLossPktUl_QCI6 =  get_value(cols[276])
    PDCP_SduLossPktUl_QCI5 =  get_value(cols[277])
    PDCP_SduLossPktUl_QCI2 =  get_value(cols[278])
    PDCP_SduLossPktUl_QCI1 =  get_value(cols[279])
    PDCP_SduPktDl =  get_value(cols[280])
    PDCP_SduPktDl_QCI8 =  get_value(cols[281])
    PDCP_SduPktDl_QCI7 =  get_value(cols[282])
    PDCP_SduPktDl_QCI9 =  get_value(cols[283])
    PDCP_SduPktDl_QCI4 =  get_value(cols[284])
    PDCP_SduPktDl_QCI3 =  get_value(cols[285])
    PDCP_SduPktDl_QCI6 =  get_value(cols[286])
    PDCP_SduPktDl_QCI5 =  get_value(cols[287])
    PDCP_SduPktDl_QCI2 =  get_value(cols[288])
    PDCP_SduPktDl_QCI1 =  get_value(cols[289])
    PDCP_SduPktUl =  get_value(cols[290])
    PDCP_SduPktUl_QCI8 =  get_value(cols[291])
    PDCP_SduPktUl_QCI7 =  get_value(cols[292])
    PDCP_SduPktUl_QCI9 =  get_value(cols[293])
    PDCP_SduPktUl_QCI4 =  get_value(cols[294])
    PDCP_SduPktUl_QCI3 =  get_value(cols[295])
    PDCP_SduPktUl_QCI6 =  get_value(cols[296])
    PDCP_SduPktUl_QCI5 =  get_value(cols[297])
    PDCP_SduPktUl_QCI2 =  get_value(cols[298])
    PDCP_SduPktUl_QCI1 =  get_value(cols[299])
    Traffic_ActUserDl =  get_value(cols[300])
    Traffic_ActUserDl_QCI8 =  get_value(cols[301])
    Traffic_ActUserDl_QCI7 =  get_value(cols[302])
    Traffic_ActUserDl_QCI9 =  get_value(cols[303])
    Traffic_ActUserDl_QCI4 =  get_value(cols[304])
    Traffic_ActUserDl_QCI3 =  get_value(cols[305])
    Traffic_ActUserDl_QCI6 =  get_value(cols[306])
    Traffic_ActUserDl_QCI5 =  get_value(cols[307])
    Traffic_ActUserDl_QCI2 =  get_value(cols[308])
    Traffic_ActUserDl_QCI1 =  get_value(cols[309])
    Traffic_ActUserMean =  get_value(cols[310])
    Traffic_ActUserUl =  get_value(cols[311])
    Traffic_ActUserUl_QCI8 =  get_value(cols[312])
    Traffic_ActUserUl_QCI7 =  get_value(cols[313])
    Traffic_ActUserUl_QCI9 =  get_value(cols[314])
    Traffic_ActUserUl_QCI4 =  get_value(cols[315])
    Traffic_ActUserUl_QCI3 =  get_value(cols[316])
    Traffic_ActUserUl_QCI6 =  get_value(cols[317])
    Traffic_ActUserUl_QCI5 =  get_value(cols[318])
    Traffic_ActUserUl_QCI2 =  get_value(cols[319])
    Traffic_ActUserUl_QCI1 =  get_value(cols[320])
    Traffic_ActUserUlMax =  get_value(cols[321])
    PRB27_InterfNoiseV =  get_value(cols[322])
    UECNTX_AbnormRel =  get_value(cols[323])
    UECNTX_NormRel =  get_value(cols[324])
    PRB26_InterfNoiseV =  get_value(cols[325])
    PRB29_InterfNoiseV =  get_value(cols[326])
    PRB28_InterfNoiseV =  get_value(cols[327])
    PRB23_InterfNoiseV =  get_value(cols[328])
    PRB22_InterfNoiseV =  get_value(cols[329])
    VlteSignal_PrbTotDl =  get_value(cols[330])
    VlteSignal_PrbTotUl =  get_value(cols[331])
    PRB25_InterfNoiseV =  get_value(cols[332])
    PRB24_InterfNoiseV =  get_value(cols[333])
    PRB30_InterfNoiseV =  get_value(cols[334])
    PRB32_InterfNoiseV =  get_value(cols[335])
    PRB31_InterfNoiseV =  get_value(cols[336])
    PRB100_InterfNoiseV =  get_value(cols[337])
    UlMCG_RxBytes =  get_value(cols[338])
    RRC_DConnMax =  get_value(cols[339])
    DlMCG_TxBytes =  get_value(cols[340])
    PHY_RruRxRSSIMax_Chan2 =  get_value(cols[341])
    PHY_RruRxRSSIMax_Chan3 =  get_value(cols[342])
    PHY_RruRxRSSIMax_Chan4 =  get_value(cols[343])
    PHY_RruRxRSSIMax_Chan5 =  get_value(cols[344])
    PHY_RruRxRSSIMax_Chan6 =  get_value(cols[345])
    PHY_RruRxRSSIMax_Chan7 =  get_value(cols[346])
    PHY_RruRxRSSIMax_Chan8 =  get_value(cols[347])
    PHY_RruRxRSSIMax_Chan1 =  get_value(cols[348])
    PHY_RruRxRSSIMean_Chan2 =  get_value(cols[349])
    PHY_RruRxRSSIMean_Chan3 =  get_value(cols[350])
    PHY_RruRxRSSIMean_Chan4 =  get_value(cols[351])
    PHY_RruRxRSSIMean_Chan5 =  get_value(cols[352])
    PHY_RruRxRSSIMean_Chan6 =  get_value(cols[353])
    PHY_RruRxRSSIMean_Chan7 =  get_value(cols[354])
    PHY_RruRxRSSIMean_Chan8 =  get_value(cols[355])
    PHY_RruRxRSSIMean_Chan1 =  get_value(cols[356])
    PHY_RruRxRSSIMin_Chan2 =  get_value(cols[357])
    PHY_RruRxRSSIMin_Chan3 =  get_value(cols[358])
    PHY_RruRxRSSIMin_Chan4 =  get_value(cols[359])
    PHY_RruRxRSSIMin_Chan5 =  get_value(cols[360])
    PHY_RruRxRSSIMin_Chan6 =  get_value(cols[361])
    PHY_RruRxRSSIMin_Chan7 =  get_value(cols[362])
    PHY_RruRxRSSIMin_Chan8 =  get_value(cols[363])
    PHY_RruRxRSSIMin_Chan1 =  get_value(cols[364])
    PHY_RecvOctDl =  get_value(cols[365])
    PHY_RecvOctUl =  get_value(cols[366])
    VlteVideo_HOAttOutInterFrq =  get_value(cols[367])
    VlteVideo_HOAttOutIntraEnb =  get_value(cols[368])
    VlteVideo_HOAttOutIntraFrq =  get_value(cols[369])
    VlteVideo_HOAttOutS1 =  get_value(cols[370])
    VlteVideo_HOAttOutX2 =  get_value(cols[371])
    VlteVideo_HOSucOutInterFrq =  get_value(cols[372])
    VlteVideo_HOSucOutIntraFrq =  get_value(cols[373])
    VlteVideo_HOSuccOutIntraEnb =  get_value(cols[374])
    VlteVideo_HOSuccOutS1 =  get_value(cols[375])
    VlteVideo_HOSuccOutX2 =  get_value(cols[376])
    VlteVideo_MaxUser =  get_value(cols[377])
    VlteVideo_time =  get_value(cols[378])
    VlteVideo_PrbTotDl =  get_value(cols[379])
    VlteVideo_PrbTotUl =  get_value(cols[380])
    VlteVideo_DispTotTbDl =  get_value(cols[381])
    VlteVideo_DispTotTbUl =  get_value(cols[382])
    PRB38_InterfNoiseV =  get_value(cols[383])
    PRB37_InterfNoiseV =  get_value(cols[384])
    PRB39_InterfNoiseV =  get_value(cols[385])
    PRB34_InterfNoiseV =  get_value(cols[386])
    PRB33_InterfNoiseV =  get_value(cols[387])
    PRB36_InterfNoiseV =  get_value(cols[388])
    PRB35_InterfNoiseV =  get_value(cols[389])
    PRB41_InterfNoiseV =  get_value(cols[390])
    PRB40_InterfNoiseV =  get_value(cols[391])
    PRB43_InterfNoiseV =  get_value(cols[392])
    PRB42_InterfNoiseV =  get_value(cols[393])
    DtchPrbAssnDl_0_20 =  get_value(cols[394])
    DtchPrbAssnDl_20_40 =  get_value(cols[395])
    DtchPrbAssnDl_40_60 =  get_value(cols[396])
    DtchPrbAssnDl_60_80 =  get_value(cols[397])
    DtchPrbAssnDl_80_100 =  get_value(cols[398])
    DRB_DSLTimeCellDl =  get_value(cols[399])
    DRB_DSLTimeCellUl =  get_value(cols[400])
    DtchPrbAssnUl_0_20 =  get_value(cols[401])
    DtchPrbAssnUl_20_40 =  get_value(cols[402])
    DtchPrbAssnUl_40_60 =  get_value(cols[403])
    DtchPrbAssnUl_60_80 =  get_value(cols[404])
    DtchPrbAssnUl_80_100 =  get_value(cols[405])
    VlteUE_RTPLossPktN_5_10 =  get_value(cols[406])
    VlteUE_RTPLossPktN_1_2 =  get_value(cols[407])
    VlteUE_RTPLossPktN_2_5 =  get_value(cols[408])
    VlteUE_RTPLossPktN_10_inf =  get_value(cols[409])
    PRB_InterfNoiseV =  get_value(cols[410])
    PRB_CONInfMeanDl =  get_value(cols[411])
    PRB_CONInfMeanUl =  get_value(cols[412])
    PRB_Dl_Avail =  get_value(cols[413])
    PRB_Ul_Avail =  get_value(cols[414])
    PRB_DoubleModeTotDl =  get_value(cols[415])
    PRB49_InterfNoiseV =  get_value(cols[416])
    PRB48_InterfNoiseV =  get_value(cols[417])
    RRC_DConnMean =  get_value(cols[418])
    PRB45_InterfNoiseV =  get_value(cols[419])
    DC_AttSgNBAddCount =  get_value(cols[420])
    PRB44_InterfNoiseV =  get_value(cols[421])
    PRB47_InterfNoiseV =  get_value(cols[422])
    PRB46_InterfNoiseV =  get_value(cols[423])
    PRB5_InterfNoiseV =  get_value(cols[424])
    PRB52_InterfNoiseV =  get_value(cols[425])
    PRB4_InterfNoiseV =  get_value(cols[426])
    PRB51_InterfNoiseV =  get_value(cols[427])
    PRB54_InterfNoiseV =  get_value(cols[428])
    PRB7_InterfNoiseV =  get_value(cols[429])
    PRB53_InterfNoiseV =  get_value(cols[430])
    PRB6_InterfNoiseV =  get_value(cols[431])
    PRB1_InterfNoiseV =  get_value(cols[432])
    PRB3_InterfNoiseV =  get_value(cols[433])
    PRB50_InterfNoiseV =  get_value(cols[434])
    PRB2_InterfNoiseV =  get_value(cols[435])
    CQI =  get_value(cols[436])
    PRB9_InterfNoiseV =  get_value(cols[437])
    Cell_UnServ_Time =  get_value(cols[438])
    PRB8_InterfNoiseV =  get_value(cols[439])
    RRC_ConnMax =  get_value(cols[440])
    DC_SgNBAddTimeMax =  get_value(cols[441])
    VlteENB_RTPLossPktN_5_10 =  get_value(cols[442])
    VlteENB_RTPLossPktN_1_2 =  get_value(cols[443])
    VlteENB_RTPLossPktN_2_5 =  get_value(cols[444])
    VlteENB_RTPLossPktN_10_inf =  get_value(cols[445])
    Paging_Capacity =  get_value(cols[446])
    Paging_Discard_Num =  get_value(cols[447])
    Paging_S1_Rx =  get_value(cols[448])
    PRACH_NonPreDetected =  get_value(cols[449])
    PRACH_PreDetected =  get_value(cols[450])
    PRACH_UseNonPre =  get_value(cols[451])
    PRACH_UsePre =  get_value(cols[452])
    PRB59_InterfNoiseV =  get_value(cols[453])
    PRB56_InterfNoiseV =  get_value(cols[454])
    PRB55_InterfNoiseV =  get_value(cols[455])
    ERAB_AbnormRel =  get_value(cols[456])
    ERAB_AbnormRel_NetCong =  get_value(cols[457])
    ERAB_AbnormRel_NetCong_QCI8 =  get_value(cols[458])
    ERAB_AbnormRel_NetCong_QCI9 =  get_value(cols[459])
    ERAB_AbnormRel_NetCong_QCI6 =  get_value(cols[460])
    ERAB_AbnormRel_NetCong_QCI7 =  get_value(cols[461])
    ERAB_AbnormRel_QCI8 =  get_value(cols[462])
    ERAB_AbnormRel_NetCong_QCI4 =  get_value(cols[463])
    ERAB_AbnormRel_QCI7 =  get_value(cols[464])
    ERAB_AbnormRel_NetCong_QCI5 =  get_value(cols[465])
    ERAB_AbnormRel_NetCong_QCI2 =  get_value(cols[466])
    ERAB_AbnormRel_QCI9 =  get_value(cols[467])
    ERAB_AbnormRel_NetCong_QCI3 =  get_value(cols[468])
    ERAB_AbnormRel_NetCong_QCI1 =  get_value(cols[469])
    ERAB_AbnormRel_RNL =  get_value(cols[470])
    ERAB_AbnormRel_MME =  get_value(cols[471])
    ERAB_AbnormRel_RNL_QCI9 =  get_value(cols[472])
    ERAB_AbnormRel_RNL_QCI8 =  get_value(cols[473])
    ERAB_AbnormRel_HoFail_QCI9 =  get_value(cols[474])
    ERAB_AbnormRel_HoFail_QCI5 =  get_value(cols[475])
    ERAB_AbnormRel_HoFail_QCI6 =  get_value(cols[476])
    ERAB_AbnormRel_HoFail_QCI7 =  get_value(cols[477])
    ERAB_AbnormRel_HoFail_QCI8 =  get_value(cols[478])
    ERAB_AbnormRel_QCI4 =  get_value(cols[479])
    ERAB_AbnormRel_RNL_QCI7 =  get_value(cols[480])
    ERAB_AbnormRel_HoFail_QCI1 =  get_value(cols[481])
    ERAB_AbnormRel_QCI3 =  get_value(cols[482])
    ERAB_AbnormRel_RNL_QCI6 =  get_value(cols[483])
    ERAB_AbnormRel_HoFail_QCI2 =  get_value(cols[484])
    ERAB_AbnormRel_QCI6 =  get_value(cols[485])
    ERAB_AbnormRel_RNL_QCI5 =  get_value(cols[486])
    ERAB_AbnormRel_HoFail_QCI3 =  get_value(cols[487])
    ERAB_AbnormRel_QCI5 =  get_value(cols[488])
    ERAB_AbnormRel_RNL_QCI4 =  get_value(cols[489])
    ERAB_AbnormRel_HoFail_QCI4 =  get_value(cols[490])
    ERAB_AbnormRel_RNL_QCI3 =  get_value(cols[491])
    ERAB_AbnormRel_RNL_QCI2 =  get_value(cols[492])
    ERAB_AbnormRel_QCI2 =  get_value(cols[493])
    ERAB_AbnormRel_RNL_QCI1 =  get_value(cols[494])
    ERAB_AbnormRel_QCI1 =  get_value(cols[495])
    ERAB_AbnormRel_HoFail =  get_value(cols[496])
    ERAB_AddAttEstab =  get_value(cols[497])
    ERAB_AddAttEstab_QCI8 =  get_value(cols[498])
    ERAB_AddAttEstab_QCI7 =  get_value(cols[499])
    ERAB_AddAttEstab_QCI9 =  get_value(cols[500])
    ERAB_AddAttEstab_QCI4 =  get_value(cols[501])
    ERAB_AddAttEstab_QCI3 =  get_value(cols[502])
    ERAB_AddAttEstab_QCI6 =  get_value(cols[503])
    ERAB_AddAttEstab_QCI5 =  get_value(cols[504])
    ERAB_AddAttEstab_QCI2 =  get_value(cols[505])
    ERAB_AddAttEstab_QCI1 =  get_value(cols[506])
    ERAB_AddSuccEstab_QCI8 =  get_value(cols[507])
    ERAB_AddSuccEstab_QCI7 =  get_value(cols[508])
    ERAB_AddSuccEstab_QCI9 =  get_value(cols[509])
    ERAB_AddSuccEstab_QCI4 =  get_value(cols[510])
    ERAB_AddSuccEstab_QCI3 =  get_value(cols[511])
    ERAB_AddSuccEstab_QCI6 =  get_value(cols[512])
    ERAB_AddSuccEstab_QCI5 =  get_value(cols[513])
    ERAB_AddSuccEstab_QCI2 =  get_value(cols[514])
    ERAB_AddSuccEstab_QCI1 =  get_value(cols[515])
    ERAB_FailEstab_TNL =  get_value(cols[516])
    ERAB_InitAttEstab =  get_value(cols[517])
    ERAB_InitAttEstab_QCI8 =  get_value(cols[518])
    ERAB_InitAttEstab_QCI7 =  get_value(cols[519])
    ERAB_InitAttEstab_QCI9 =  get_value(cols[520])
    ERAB_InitAttEstab_QCI4 =  get_value(cols[521])
    ERAB_InitAttEstab_QCI3 =  get_value(cols[522])
    ERAB_InitAttEstab_QCI6 =  get_value(cols[523])
    ERAB_InitAttEstab_QCI5 =  get_value(cols[524])
    ERAB_InitAttEstab_QCI2 =  get_value(cols[525])
    ERAB_InitAttEstab_QCI1 =  get_value(cols[526])
    ERAB_InitSuccEstab_QCI8 =  get_value(cols[527])
    ERAB_InitSuccEstab_QCI7 =  get_value(cols[528])
    ERAB_InitSuccEstab_QCI9 =  get_value(cols[529])
    ERAB_InitSuccEstab_QCI4 =  get_value(cols[530])
    ERAB_InitSuccEstab_QCI3 =  get_value(cols[531])
    ERAB_InitSuccEstab_QCI6 =  get_value(cols[532])
    ERAB_InitSuccEstab_QCI5 =  get_value(cols[533])
    ERAB_InitSuccEstab_QCI2 =  get_value(cols[534])
    ERAB_InitSuccEstab_QCI1 =  get_value(cols[535])
    ERAB_NormRel =  get_value(cols[536])
    ERAB_NormRel_QCI8 =  get_value(cols[537])
    ERAB_NormRel_QCI7 =  get_value(cols[538])
    ERAB_NormRel_QCI9 =  get_value(cols[539])
    ERAB_NormRel_QCI4 =  get_value(cols[540])
    ERAB_NormRel_QCI3 =  get_value(cols[541])
    ERAB_NormRel_QCI6 =  get_value(cols[542])
    ERAB_NormRel_QCI5 =  get_value(cols[543])
    ERAB_NormRel_QCI2 =  get_value(cols[544])
    ERAB_NormRel_QCI1 =  get_value(cols[545])
    ERAB_AbnormRel_CaUser =  get_value(cols[546])
    ERAB_Mean =  get_value(cols[547])
    ERAB_Mean_QCI8 =  get_value(cols[548])
    ERAB_Mean_QCI7 =  get_value(cols[549])
    ERAB_Mean_QCI9 =  get_value(cols[550])
    ERAB_Mean_QCI4 =  get_value(cols[551])
    ERAB_Mean_QCI3 =  get_value(cols[552])
    ERAB_Mean_QCI6 =  get_value(cols[553])
    ERAB_Mean_QCI5 =  get_value(cols[554])
    ERAB_Mean_QCI2 =  get_value(cols[555])
    ERAB_Mean_QCI1 =  get_value(cols[556])
    ERAB_NormRel_CaUser =  get_value(cols[557])
    ERAB_AbnormRel_MME_QCI6 =  get_value(cols[558])
    ERAB_AbnormRel_MME_QCI5 =  get_value(cols[559])
    ERAB_AbnormRel_MME_QCI8 =  get_value(cols[560])
    ERAB_AbnormRel_MME_QCI7 =  get_value(cols[561])
    ERAB_AbnormRel_MME_QCI9 =  get_value(cols[562])
    ERAB_AbnormRel_MME_QCI2 =  get_value(cols[563])
    ERAB_AbnormRel_MME_QCI1 =  get_value(cols[564])
    ERAB_AbnormRel_MME_QCI4 =  get_value(cols[565])
    ERAB_AbnormRel_MME_QCI3 =  get_value(cols[566])
    ERAB_AbnormRel_TNL_QCI3 =  get_value(cols[567])
    ERAB_AbnormRel_TNL_QCI2 =  get_value(cols[568])
    ERAB_AbnormRel_TNL_QCI5 =  get_value(cols[569])
    ERAB_AbnormRel_TNL =  get_value(cols[570])
    ERAB_AbnormRel_TNL_QCI4 =  get_value(cols[571])
    ERAB_AbnormRel_TNL_QCI7 =  get_value(cols[572])
    ERAB_AbnormRel_TNL_QCI6 =  get_value(cols[573])
    ERAB_AbnormRel_TNL_QCI9 =  get_value(cols[574])
    ERAB_AbnormRel_TNL_QCI8 =  get_value(cols[575])
    ERAB_AbnormRel_TNL_QCI1 =  get_value(cols[576])
    ERAB_AddSuccEstab =  get_value(cols[577])
    ERAB_FailEstab_SecurModeFail =  get_value(cols[578])
    ERAB_FailEstab_RNL =  get_value(cols[579])
    ERAB_FailEstab_MME =  get_value(cols[580])
    ERAB_FailEstab_UeNoReply =  get_value(cols[581])
    ERAB_FailEstab_OtherCause =  get_value(cols[582])
    ERAB_FailEstab_NoRadioRes =  get_value(cols[583])
    ERAB_InitSuccEstab =  get_value(cols[584])
    ERAB_SessionTime =  get_value(cols[585])
    ERAB_SessionTime_QCI8 =  get_value(cols[586])
    ERAB_SessionTime_QCI7 =  get_value(cols[587])
    ERAB_SessionTime_QCI9 =  get_value(cols[588])
    ERAB_SessionTime_QCI4 =  get_value(cols[589])
    ERAB_SessionTime_QCI3 =  get_value(cols[590])
    ERAB_SessionTime_QCI6 =  get_value(cols[591])
    ERAB_SessionTime_QCI5 =  get_value(cols[592])
    ERAB_SessionTime_QCI2 =  get_value(cols[593])
    ERAB_SessionTime_QCI1 =  get_value(cols[594])
    PRB58_InterfNoiseV =  get_value(cols[595])
    PRB57_InterfNoiseV =  get_value(cols[596])
    PRB63_InterfNoiseV =  get_value(cols[597])
    PRB62_InterfNoiseV =  get_value(cols[598])
    PRB65_InterfNoiseV =  get_value(cols[599])
    PRB64_InterfNoiseV =  get_value(cols[600])
    PRB61_InterfNoiseV =  get_value(cols[601])
    PRB60_InterfNoiseV =  get_value(cols[602])
    PRB67_InterfNoiseV =  get_value(cols[603])
    PRB66_InterfNoiseV =  get_value(cols[604])
    PuschPrbTotMeanDl =  get_value(cols[605])
    PRB69_InterfNoiseV =  get_value(cols[606])
    PRB68_InterfNoiseV =  get_value(cols[607])
    PRB74_InterfNoiseV =  get_value(cols[608])
    PRB73_InterfNoiseV =  get_value(cols[609])
    PRB76_InterfNoiseV =  get_value(cols[610])
    PRB75_InterfNoiseV =  get_value(cols[611])
    PRB70_InterfNoiseV =  get_value(cols[612])
    PRB72_InterfNoiseV =  get_value(cols[613])
    PRB71_InterfNoiseV =  get_value(cols[614])
    eMTC_ERAB_SuccEstab =  get_value(cols[615])
    eMTC_ERAB_Mean =  get_value(cols[616])
    eMTC_ERAB_AbnormRel_MME =  get_value(cols[617])
    eMTC_ERAB_AttEstab =  get_value(cols[618])
    eMTC_ERAB_AbnormRel_TNL =  get_value(cols[619])
    eMTC_ERAB_AbnormRel_NetCong =  get_value(cols[620])
    eMTC_ERAB_AbnormRel_RNL =  get_value(cols[621])
    eMTC_HO_SuccOutX2 =  get_value(cols[622])
    eMTC_HO_AttOutIntraEnb =  get_value(cols[623])
    eMTC_HO_AttOutX2 =  get_value(cols[624])
    eMTC_HO_SuccOutS1 =  get_value(cols[625])
    eMTC_HO_SuccOutIntraEnb =  get_value(cols[626])
    eMTC_HO_AttOutS1 =  get_value(cols[627])
    eMTC_PDCP_SduTotDelayDl =  get_value(cols[628])
    eMTC_PDCP_SduOctDl =  get_value(cols[629])
    eMTC_PDCP_SduOctUl =  get_value(cols[630])
    eMTC_PDCP_SduPktDl =  get_value(cols[631])
    eMTC_PDCP_SduPktUl =  get_value(cols[632])
    eMTC_RRC_AttConnReestab =  get_value(cols[633])
    eMTC_RRC_UserConnMax =  get_value(cols[634])
    eMTC_RRC_SuccConn =  get_value(cols[635])
    eMTC_RRC_UserConnMean =  get_value(cols[636])
    eMTC_RRC_SuccConnReestab =  get_value(cols[637])
    eMTC_RRC_Conn_Req =  get_value(cols[638])
    eMTC_UECNTX_NormRel =  get_value(cols[639])
    eMTC_UECNTX_AbnormRel =  get_value(cols[640])
    eMTC_DRB_DSLTimeCellUl =  get_value(cols[641])
    eMTC_DRB_DSLTimeCellDl =  get_value(cols[642])
    eMTC_MAC_TBInitTransFailUl =  get_value(cols[643])
    eMTC_MAC_TBInitTransUl =  get_value(cols[644])
    eMTC_MAC_TBInitTransFailDl =  get_value(cols[645])
    eMTC_MAC_TBInitTransDl =  get_value(cols[646])
    eMTC_PrbAssnDl =  get_value(cols[647])
    eMTC_PrbAssnUl =  get_value(cols[648])
    eMTC_TBReMisPktDl =  get_value(cols[649])
    eMTC_TBReMisPktUl =  get_value(cols[650])
    eMTC_PDCP_ThrpTimeDl =  get_value(cols[651])
    eMTC_PDCP_ThrpTimeUl =  get_value(cols[652])
    eMTC_Paging_Discard_Num =  get_value(cols[653])
    eMTC_Paging_S1_Rx =  get_value(cols[654])
    eMTC_RRC_SuccConn_L3 =  get_value(cols[655])
    eMTC_RRC_Conn_Req_L3 =  get_value(cols[656])
    eMTC_RRC_SuccConn_L1 =  get_value(cols[657])
    eMTC_RRC_SuccConn_L2 =  get_value(cols[658])
    eMTC_RRC_SuccConn_L0 =  get_value(cols[659])
    eMTC_RRC_Conn_Req_L0 =  get_value(cols[660])
    eMTC_RRC_Conn_Req_L1 =  get_value(cols[661])
    eMTC_RRC_Conn_Req_L2 =  get_value(cols[662])
    CQI9 =  get_value(cols[663])
    CQI8 =  get_value(cols[664])
    CQI7 =  get_value(cols[665])
    CQI6 =  get_value(cols[666])
    CQI5 =  get_value(cols[667])
    VlteEPC_RTPLossPktN_5_10 =  get_value(cols[668])
    VlteEPC_RTPLossPktN_1_2 =  get_value(cols[669])
    VlteEPC_RTPLossPktN_2_5 =  get_value(cols[670])
    VlteEPC_RTPLossPktN_10_inf =  get_value(cols[671])
    CQI4 =  get_value(cols[672])
    CQI3 =  get_value(cols[673])
    CQI2 =  get_value(cols[674])
    CQI1 =  get_value(cols[675])
    CQI0 =  get_value(cols[676])
    RRC_ConnMean =  get_value(cols[677])
    PRB78_InterfNoiseV =  get_value(cols[678])
    PRB77_InterfNoiseV =  get_value(cols[679])
    PRB79_InterfNoiseV =  get_value(cols[680])
    PRB85_InterfNoiseV =  get_value(cols[681])
    PRB84_InterfNoiseV =  get_value(cols[682])
    PuschPrbTotDl =  get_value(cols[683])
    PRB87_InterfNoiseV =  get_value(cols[684])
    PDCCH_CCE_Assigned =  get_value(cols[685])
    PDCCH_CCE_Occupied =  get_value(cols[686])
    PRB86_InterfNoiseV =  get_value(cols[687])
    PRB81_InterfNoiseV =  get_value(cols[688])
    PRB80_InterfNoiseV =  get_value(cols[689])
    PRB83_InterfNoiseV =  get_value(cols[690])
    PRB82_InterfNoiseV =  get_value(cols[691])
    PuschPrbTotMeanUl =  get_value(cols[692])

    a_tuifu = Cell_UnServ_Time
    a_tongji = 900
    a_keyonglv = 1-get_devide(a_tuifu,a_tongji)
    a_ganrao = PRB_InterfNoiseV
    a_cqi0 = CQI0
    a_cqi1 = CQI1
    a_cqi2 = CQI2
    a_cqi3 = CQI3
    a_cqi4 = CQI4
    a_cqi5 = CQI5
    a_cqi6 = CQI6
    a_cqi7 = CQI7
    a_cqi8 = CQI8
    a_cqi9 = CQI9
    a_cqi10 = CQI10
    a_cqi11 = CQI11
    a_cqi12 = CQI12
    a_cqi13 = CQI13
    a_cqi14 = CQI14
    a_cqi15 = CQI15
    a_cqi = get_devide((1*CQI1+2*CQI2+3*CQI3+4*CQI4+5*CQI5+6*CQI6+7*CQI7+8*CQI8+9*CQI9+10*CQI10+11*CQI11+12*CQI12+13*CQI13+14*CQI14+15*CQI15),(CQI0+CQI1+CQI2+CQI3+CQI4+CQI5+CQI6+CQI7+CQI8+CQI9+CQI10+CQI11+CQI12+CQI13+CQI14+CQI15))
    a_cqigt7 = get_devide((CQI7+CQI8+CQI9+CQI10+CQI11+CQI12+CQI13+CQI14+CQI15),(CQI0+CQI1+CQI2+CQI3+CQI4+CQI5+CQI6+CQI7+CQI8+CQI9+CQI10+CQI11+CQI12+CQI13+CQI14+CQI15))
    a_cqilt7 = get_devide((CQI0+CQI1+CQI2+CQI3+CQI4+CQI5+CQI6),(CQI0+CQI1+CQI2+CQI3+CQI4+CQI5+CQI6+CQI7+CQI8+CQI9+CQI10+CQI11+CQI12+CQI13+CQI14+CQI15))
    a_ul_sdu_lost = PDCP_SduLossPktUl
    a_ul_sdu_total = PDCP_SduPktUl
    a_dl_sdu_lost = PDCP_SduLossPktDl
    a_dl_sdu_total = PDCP_SduPktDl
    a_ul_sdu_diubaolv = get_devide(PDCP_SduLossPktUl,PDCP_SduPktUl)
    a_ul_sdu_diubaolv_qci1 = get_devide(PDCP_SduLossPktUl_QCI1,PDCP_SduPktUl_QCI1)
    a_dl_sdu_diubaolv_qci1 = get_devide(PDCP_SduLossPktDl_QCI1,PDCP_SduPktDl_QCI1)
    a_dl_sdu_qibaolv = get_devide(PDCP_SduDiscardPktDl,PDCP_SduPktDl)
    a_ul_mbps = get_devide(PDCP_SduOctUl*8000,PDCP_ThrpTimeUl)
    a_dl_mbps = get_devide(PDCP_SduOctDl*8000,PDCP_ThrpTimeDl)
    a_rrc_req = RRC_AttConnEstab_UE+RRC_AttConnEstab_Net
    a_rrc_suc = RRC_SuccConnEstab_UE+RRC_SuccConnEstab_Net
    a_rrc_suc_r = get_devide(RRC_SuccConnEstab_UE+RRC_SuccConnEstab_Net,RRC_AttConnEstab_UE+RRC_AttConnEstab_Net)
    a_rrc_congest = ''
    a_rrc_avg = RRC_UserConnMean
    a_rrc_max = RRC_UserConnMax
    a_erab_req = ERAB_InitAttEstab+ERAB_AddAttEstab
    a_erab_req_qci3 = ERAB_InitAttEstab_QCI3+ERAB_AddAttEstab_QCI3
    a_erab_req_qci4 = ERAB_InitAttEstab_QCI4+ERAB_AddAttEstab_QCI4
    a_erab_req_qci6 = ERAB_InitAttEstab_QCI6+ERAB_AddAttEstab_QCI6
    a_erab_req_qci7 = ERAB_InitAttEstab_QCI7+ERAB_AddAttEstab_QCI7
    a_erab_req_qci8 = ERAB_InitAttEstab_QCI8+ERAB_AddAttEstab_QCI8
    a_erab_req_qci9 = ERAB_InitAttEstab_QCI9+ERAB_AddAttEstab_QCI9
    a_erab_suc = ERAB_InitSuccEstab+ERAB_AddSuccEstab
    a_erab_suc_qci3 = ERAB_InitSuccEstab_QCI3+ERAB_AddSuccEstab_QCI3
    a_erab_suc_qci4 = ERAB_InitSuccEstab_QCI4+ERAB_AddSuccEstab_QCI4
    a_erab_suc_qci6 = ERAB_InitSuccEstab_QCI6+ERAB_AddSuccEstab_QCI6
    a_erab_suc_qci7 = ERAB_InitSuccEstab_QCI7+ERAB_AddSuccEstab_QCI7
    a_erab_suc_qci8 = ERAB_InitSuccEstab_QCI8+ERAB_AddSuccEstab_QCI8
    a_erab_suc_qci9 = ERAB_InitSuccEstab_QCI9+ERAB_AddSuccEstab_QCI9
    a_erab_congest = ERAB_AbnormRel_NetCong
    a_s1_att = S1Sig_AttConnEstab
    a_s1_suc = S1Sig_SuccConnEstab
    a_s1_suc_r = get_devide(S1Sig_SuccConnEstab,S1Sig_AttConnEstab)
    a_erab_suc_r = get_devide(ERAB_InitSuccEstab+ERAB_AddSuccEstab,ERAB_InitAttEstab+ERAB_AddAttEstab)
    a_erab_suc_r_qci1 = get_devide(ERAB_InitSuccEstab_QCI1+ERAB_AddSuccEstab_QCI1,ERAB_InitAttEstab_QCI1+ERAB_AddAttEstab_QCI1)
    a_erab_suc_r_qci2 = get_devide(ERAB_InitSuccEstab_QCI2+ERAB_AddSuccEstab_QCI2,ERAB_InitAttEstab_QCI2+ERAB_AddAttEstab_QCI2)
    a_erab_suc_r_qci3 = get_devide(ERAB_InitSuccEstab_QCI3+ERAB_AddSuccEstab_QCI3,ERAB_InitAttEstab_QCI3+ERAB_AddAttEstab_QCI3)
    a_erab_suc_r_qci4 = get_devide(ERAB_InitSuccEstab_QCI4+ERAB_AddSuccEstab_QCI4,ERAB_InitAttEstab_QCI4+ERAB_AddAttEstab_QCI4)
    a_erab_suc_r_qci5 = get_devide(ERAB_InitSuccEstab_QCI5+ERAB_AddSuccEstab_QCI5,ERAB_InitAttEstab_QCI5+ERAB_AddAttEstab_QCI5)
    a_radio_suc_r = a_rrc_suc_r*a_erab_suc_r
    a_erab_abnormal = ERAB_AbnormRel
    a_context_release_abnormal = UECNTX_AbnormRel
    a_context_release_normal = UECNTX_NormRel
    a_lte_drop_r = get_devide(UECNTX_AbnormRel,UECNTX_AbnormRel+UECNTX_NormRel)
    a_lte_release = UECNTX_NormRel
    a_ho_pingpang = ''
    a_ho_out_suc_r = ''
    a_ul_tra_mb = PDCP_SduOctUl
    a_dl_tra_mb = PDCP_SduOctDl
    a_total_tra_mb = PDCP_SduOctUl+PDCP_SduOctDl
    a_rrc_fail_license = ''
    a_rrc_conn_suc = RRC_SuccConnEstab_UE+RRC_SuccConnEstab_Net
    a_ul_prb_utilization = get_devide(PuschPrbTotMeanUl,PRB_Ul_Avail)
    a_dl_prb_utilization = get_devide(PuschPrbTotMeanDl,PRB_Dl_Avail)
    a_rrc_congest_r_license = ''
    a_shuangliubi = get_devide(PRB_DoubleModeTotDl,PuschPrbTotDl)
    a_radio_utilization = ''
    a_sgnb_add_req = DC_AttSgNBAddCount
    a_sgnb_add_suc = DC_SuccSgNBAddCount
    a_sgnb_add_r = get_devide(DC_SuccSgNBAddCount,DC_AttSgNBAddCount)
    a_dl_16qam_utilization = ''
    a_dl_64qam_utilization = ''
    a_erab_congest_radio = ''
    a_erab_congest_trans = ''
    a_erab_fail_ue = ERAB_FailEstab_UeNoReply
    a_erab_fail_core = ERAB_FailEstab_MME
    a_erab_fail_trans = ERAB_FailEstab_TNL
    a_erab_fail_radio = ERAB_FailEstab_RNL
    a_erab_fail_resource = ERAB_FailEstab_NoRadioRes
    a_rrc_release_csfb = ''
    a_dl_highmsc_r = ''
    a_ul_highmsc_r = ''
    a_csfb_suc = ''
    a_ho_s1_out_req = ''
    a_ho_s1_out_suc = ''
    a_ho_x2_out_req = ''
    a_ho_x2_out_suc = ''
    a_erab_req_qci1 = ERAB_InitAttEstab_QCI1
    a_erab_req_qci5 = ERAB_InitAttEstab_QCI5
    a_erab_suc_qci1 = ERAB_InitSuccEstab_QCI1
    a_erab_suc_qci5 = ERAB_InitSuccEstab_QCI5
    a_erab_normal_qci1 = ERAB_NormRel_QCI1
    a_erab_abnormal_qci1 = ERAB_AbnormRel_QCI1
    a_lte_drop_r_qci1 = get_devide(ERAB_AbnormRel_QCI1,ERAB_AbnormRel_QCI1+ERAB_NormRel_QCI1)
    a_lru_blind = ''
    a_lru_not_blind = ''
    starttime = d0[0:4]+'-'+d0[4:6]+'-'+d0[6:8]+' '+d0[8:10]+':'+d0[10:]+':00'
    cell = '127.'+str(int(related_enb_id))+'.'+str(int(cel_id))
    ##第二批增加
    a_lru_not_blind = ''    #LTE-UTRAN系统间重定向请求次数(非盲重定向)(次)
    a_succoutintraenb = HO_SuccOutIntraEnb    #eNB内切换出成功次数
    a_attoutintraenb = ''    #eNB内切换出请求次数
    a_rru_puschprbassn = PuschPrbTotMeanUl    #上行PUSCH_PRB占用数
    a_rru_puschprbtot = PRB_Ul_Avail    #上行PUSCH_PRB可用数
    a_rru_pdschprbassn = PuschPrbTotMeanDl    #下行PUSCH_PRB占用数
    a_rru_pdschprbtot = PRB_Dl_Avail    #下行PUSCH_PRB可用数
    a_effectiveconnmean = RRC_UserConnMean    #有效RRC连接平均数
    a_effectiveconnmax = RRC_UserConnMax    #有效RRC连接最大数
    a_pdcch_signal_occupy_ratio = get_devide(PDCCH_CCE_Occupied,PDCCH_CCE_Assigned)    #PDCCH信道CCE占用率(%)(下行利用率PDCCH)
    a_rru_pdcchcceutil = PDCCH_CCE_Occupied    #PDCCH信道CCE占用个数
    a_rru_pdcchcceavail = PDCCH_CCE_Assigned    #PDCCH信道CCE可用个数
    a_succexecinc = ''    #切换入成功次数
    a_succconnreestab_nonsrccell = RRC_SuccConnReestab    #RRC连接重建成功次数(非源侧小区)
    a_rrc_reconn_rate = get_devide(RRC_AttConnReestab,RRC_AttConnEstab_UE+RRC_AttConnEstab_Net)    #RRC连接重建比例(%)
    a_enb_handover_succ_rate = ''    #eNB内切换出成功率(%)
    a_down_pdcch_ch_cce_occ_rate = get_devide(PDCCH_CCE_Occupied,PDCCH_CCE_Assigned)    #下行PDCCH信道CCE占用率(%)
    a_down_pdcp_sdu_avg_delay = ''    #下行PDCP SDU平均时延(ms)
    a_mr_sinrul_gt0_ratio = ''    #上行SINR大于0占比
    a_mr_sinrul_gt0_ratio_fz = ''    #上行SINR大于0占比分子
    a_mr_sinrul_gt0_ratio_fm = ''    #上行SINR大于0占比分母
    a_vendor = 'NOKIA'    #厂商
    a_lte_wireless_drop_ratio_cell = get_devide(UECNTX_AbnormRel,UECNTX_AbnormRel+UECNTX_NormRel)    #小区无线掉线率
    ##第三批增加
    a_pdcp_sdu_vol_ul_plmn1 = ''    #空口上行业务流量（PDCP）(联通46001)(MByte)
    a_pdcp_sdu_vol_dl_plmn1 = ''    #空口下行业务流量（PDCP）(联通46001)(MByte)
    a_effectiveconnmean_plmn1 = ''    #RRC连接平均数(联通46001)
    a_erab_abnormal_plmn1 = ''    #E-RAB异常释放总次数(联通46001)
    a_erab_normal_plmn1 = ''    #E-RAB正常释放次数(联通46001)
    a_pdcp_sdu_vol_ul_plmn2 = ''    #空口上行业务流量（PDCP）(电信46011)(MByte)
    a_pdcp_sdu_vol_dl_plmn2 = ''    #空口下行业务流量（PDCP）(电信46011)(MByte)
    a_effectiveconnmean_plmn2 = ''    #RRC连接平均数(电信46011)
    a_erab_abnormal_plmn2 = ''    #E-RAB异常释放总次数(电信46011)
    a_erab_normal_plmn2 = ''    #E-RAB正常释放次数(电信46011)
    a_erab_ini_setup_att_plmn1_qci1 = ''    #初始E-RAB建立请求次数_联通_QCI1
    a_erab_ini_setup_att_plmn1_qci2 = ''    #初始E-RAB建立请求次数_联通_QCI2
    a_erab_ini_setup_att_plmn1_qci3 = ''    #初始E-RAB建立请求次数_联通_QCI3
    a_erab_ini_setup_att_plmn1_qci4 = ''    #初始E-RAB建立请求次数_联通_QCI4
    a_erab_ini_setup_att_plmn1_qci5 = ''    #初始E-RAB建立请求次数_联通_QCI5
    a_erab_ini_setup_att_plmn1_qci6 = ''    #初始E-RAB建立请求次数_联通_QCI6
    a_erab_ini_setup_att_plmn1_qci7 = ''    #初始E-RAB建立请求次数_联通_QCI7
    a_erab_ini_setup_att_plmn1_qci8 = ''    #初始E-RAB建立请求次数_联通_QCI8
    a_erab_ini_setup_att_plmn1_qci9 = ''    #初始E-RAB建立请求次数_联通_QCI9
    a_erab_ini_setup_succ_plmn1_qci1 = ''    #初始E-RAB建立成功次数_联通_QCI1
    a_erab_ini_setup_succ_plmn1_qci2 = ''    #初始E-RAB建立成功次数_联通_QCI2
    a_erab_ini_setup_succ_plmn1_qci3 = ''    #初始E-RAB建立成功次数_联通_QCI3
    a_erab_ini_setup_succ_plmn1_qci4 = ''    #初始E-RAB建立成功次数_联通_QCI4
    a_erab_ini_setup_succ_plmn1_qci5 = ''    #初始E-RAB建立成功次数_联通_QCI5
    a_erab_ini_setup_succ_plmn1_qci6 = ''    #初始E-RAB建立成功次数_联通_QCI6
    a_erab_ini_setup_succ_plmn1_qci7 = ''    #初始E-RAB建立成功次数_联通_QCI7
    a_erab_ini_setup_succ_plmn1_qci8 = ''    #初始E-RAB建立成功次数_联通_QCI8
    a_erab_ini_setup_succ_plmn1_qci9 = ''    #初始E-RAB建立成功次数_联通_QCI9
    a_erab_add_setup_att_plmn1_qci1 = ''    #附加E-RAB建立请求次数_联通_QCI1
    a_erab_add_setup_att_plmn1_qci2 = ''    #附加E-RAB建立请求次数_联通_QCI2
    a_erab_add_setup_att_plmn1_qci3 = ''    #附加E-RAB建立请求次数_联通_QCI3
    a_erab_add_setup_att_plmn1_qci4 = ''    #附加E-RAB建立请求次数_联通_QCI4
    a_erab_add_setup_att_plmn1_qci5 = ''    #附加E-RAB建立请求次数_联通_QCI5
    a_erab_add_setup_att_plmn1_qci6 = ''    #附加E-RAB建立请求次数_联通_QCI6
    a_erab_add_setup_att_plmn1_qci7 = ''    #附加E-RAB建立请求次数_联通_QCI7
    a_erab_add_setup_att_plmn1_qci8 = ''    #附加E-RAB建立请求次数_联通_QCI8
    a_erab_add_setup_att_plmn1_qci9 = ''    #附加E-RAB建立请求次数_联通_QCI9
    a_erab_add_setup_succ_plmn1_qci1 = ''    #附加E-RAB建立成功次数_联通_QCI1
    a_erab_add_setup_succ_plmn1_qci2 = ''    #附加E-RAB建立成功次数_联通_QCI2
    a_erab_add_setup_succ_plmn1_qci3 = ''    #附加E-RAB建立成功次数_联通_QCI3
    a_erab_add_setup_succ_plmn1_qci4 = ''    #附加E-RAB建立成功次数_联通_QCI4
    a_erab_add_setup_succ_plmn1_qci5 = ''    #附加E-RAB建立成功次数_联通_QCI5
    a_erab_add_setup_succ_plmn1_qci6 = ''    #附加E-RAB建立成功次数_联通_QCI6
    a_erab_add_setup_succ_plmn1_qci7 = ''    #附加E-RAB建立成功次数_联通_QCI7
    a_erab_add_setup_succ_plmn1_qci8 = ''    #附加E-RAB建立成功次数_联通_QCI8
    a_erab_add_setup_succ_plmn1_qci9 = ''    #附加E-RAB建立成功次数_联通_QCI9
    a_erab_abnormal_plmn1_qci1 = ''    #E-RAB异常释放总次数_联通_QCI1
    a_erab_abnormal_plmn1_qci2 = ''    #E-RAB异常释放总次数_联通_QCI2
    a_erab_abnormal_plmn1_qci3 = ''    #E-RAB异常释放总次数_联通_QCI3
    a_erab_abnormal_plmn1_qci4 = ''    #E-RAB异常释放总次数_联通_QCI4
    a_erab_abnormal_plmn1_qci5 = ''    #E-RAB异常释放总次数_联通_QCI5
    a_erab_abnormal_plmn1_qci6 = ''    #E-RAB异常释放总次数_联通_QCI6
    a_erab_abnormal_plmn1_qci7 = ''    #E-RAB异常释放总次数_联通_QCI7
    a_erab_abnormal_plmn1_qci8 = ''    #E-RAB异常释放总次数_联通_QCI8
    a_erab_abnormal_plmn1_qci9 = ''    #E-RAB异常释放总次数_联通_QCI9
    a_erab_normal_plmn1_qci1 = ''    #E-RAB正常释放次数_联通_QCI1
    a_erab_normal_plmn1_qci2 = ''    #E-RAB正常释放次数_联通_QCI2
    a_erab_normal_plmn1_qci3 = ''    #E-RAB正常释放次数_联通_QCI3
    a_erab_normal_plmn1_qci4 = ''    #E-RAB正常释放次数_联通_QCI4
    a_erab_normal_plmn1_qci5 = ''    #E-RAB正常释放次数_联通_QCI5
    a_erab_normal_plmn1_qci6 = ''    #E-RAB正常释放次数_联通_QCI6
    a_erab_normal_plmn1_qci7 = ''    #E-RAB正常释放次数_联通_QCI7
    a_erab_normal_plmn1_qci8 = ''    #E-RAB正常释放次数_联通_QCI8
    a_erab_normal_plmn1_qci9 = ''    #E-RAB正常释放次数_联通_QCI9
    a_erab_ini_setup_att_plmn2_qci1 = ''    #初始E-RAB建立请求次数_电信_QCI1
    a_erab_ini_setup_att_plmn2_qci2 = ''    #初始E-RAB建立请求次数_电信_QCI2
    a_erab_ini_setup_att_plmn2_qci3 = ''    #初始E-RAB建立请求次数_电信_QCI3
    a_erab_ini_setup_att_plmn2_qci4 = ''    #初始E-RAB建立请求次数_电信_QCI4
    a_erab_ini_setup_att_plmn2_qci5 = ''    #初始E-RAB建立请求次数_电信_QCI5
    a_erab_ini_setup_att_plmn2_qci6 = ''    #初始E-RAB建立请求次数_电信_QCI6
    a_erab_ini_setup_att_plmn2_qci7 = ''    #初始E-RAB建立请求次数_电信_QCI7
    a_erab_ini_setup_att_plmn2_qci8 = ''    #初始E-RAB建立请求次数_电信_QCI8
    a_erab_ini_setup_att_plmn2_qci9 = ''    #初始E-RAB建立请求次数_电信_QCI9
    a_erab_ini_setup_succ_plmn2_qci1 = ''    #初始E-RAB建立成功次数_电信_QCI1
    a_erab_ini_setup_succ_plmn2_qci2 = ''    #初始E-RAB建立成功次数_电信_QCI2
    a_erab_ini_setup_succ_plmn2_qci3 = ''    #初始E-RAB建立成功次数_电信_QCI3
    a_erab_ini_setup_succ_plmn2_qci4 = ''    #初始E-RAB建立成功次数_电信_QCI4
    a_erab_ini_setup_succ_plmn2_qci5 = ''    #初始E-RAB建立成功次数_电信_QCI5
    a_erab_ini_setup_succ_plmn2_qci6 = ''    #初始E-RAB建立成功次数_电信_QCI6
    a_erab_ini_setup_succ_plmn2_qci7 = ''    #初始E-RAB建立成功次数_电信_QCI7
    a_erab_ini_setup_succ_plmn2_qci8 = ''    #初始E-RAB建立成功次数_电信_QCI8
    a_erab_ini_setup_succ_plmn2_qci9 = ''    #初始E-RAB建立成功次数_电信_QCI9
    a_erab_add_setup_att_plmn2_qci1 = ''    #附加E-RAB建立请求次数_电信_QCI1
    a_erab_add_setup_att_plmn2_qci2 = ''    #附加E-RAB建立请求次数_电信_QCI2
    a_erab_add_setup_att_plmn2_qci3 = ''    #附加E-RAB建立请求次数_电信_QCI3
    a_erab_add_setup_att_plmn2_qci4 = ''    #附加E-RAB建立请求次数_电信_QCI4
    a_erab_add_setup_att_plmn2_qci5 = ''    #附加E-RAB建立请求次数_电信_QCI5
    a_erab_add_setup_att_plmn2_qci6 = ''    #附加E-RAB建立请求次数_电信_QCI6
    a_erab_add_setup_att_plmn2_qci7 = ''    #附加E-RAB建立请求次数_电信_QCI7
    a_erab_add_setup_att_plmn2_qci8 = ''    #附加E-RAB建立请求次数_电信_QCI8
    a_erab_add_setup_att_plmn2_qci9 = ''    #附加E-RAB建立请求次数_电信_QCI9
    a_erab_add_setup_succ_plmn2_qci1 = ''    #附加E-RAB建立成功次数_电信_QCI1
    a_erab_add_setup_succ_plmn2_qci2 = ''    #附加E-RAB建立成功次数_电信_QCI2
    a_erab_add_setup_succ_plmn2_qci3 = ''    #附加E-RAB建立成功次数_电信_QCI3
    a_erab_add_setup_succ_plmn2_qci4 = ''    #附加E-RAB建立成功次数_电信_QCI4
    a_erab_add_setup_succ_plmn2_qci5 = ''    #附加E-RAB建立成功次数_电信_QCI5
    a_erab_add_setup_succ_plmn2_qci6 = ''    #附加E-RAB建立成功次数_电信_QCI6
    a_erab_add_setup_succ_plmn2_qci7 = ''    #附加E-RAB建立成功次数_电信_QCI7
    a_erab_add_setup_succ_plmn2_qci8 = ''    #附加E-RAB建立成功次数_电信_QCI8
    a_erab_add_setup_succ_plmn2_qci9 = ''    #附加E-RAB建立成功次数_电信_QCI9
    a_erab_abnormal_plmn2_qci1 = ''    #E-RAB异常释放总次数_电信_QCI1
    a_erab_abnormal_plmn2_qci2 = ''    #E-RAB异常释放总次数_电信_QCI2
    a_erab_abnormal_plmn2_qci3 = ''    #E-RAB异常释放总次数_电信_QCI3
    a_erab_abnormal_plmn2_qci4 = ''    #E-RAB异常释放总次数_电信_QCI4
    a_erab_abnormal_plmn2_qci5 = ''    #E-RAB异常释放总次数_电信_QCI5
    a_erab_abnormal_plmn2_qci6 = ''    #E-RAB异常释放总次数_电信_QCI6
    a_erab_abnormal_plmn2_qci7 = ''    #E-RAB异常释放总次数_电信_QCI7
    a_erab_abnormal_plmn2_qci8 = ''    #E-RAB异常释放总次数_电信_QCI8
    a_erab_abnormal_plmn2_qci9 = ''    #E-RAB异常释放总次数_电信_QCI9
    a_erab_normal_plmn2_qci1 = ''    #E-RAB正常释放次数_电信_QCI1
    a_erab_normal_plmn2_qci2 = ''    #E-RAB正常释放次数_电信_QCI2
    a_erab_normal_plmn2_qci3 = ''    #E-RAB正常释放次数_电信_QCI3
    a_erab_normal_plmn2_qci4 = ''    #E-RAB正常释放次数_电信_QCI4
    a_erab_normal_plmn2_qci5 = ''    #E-RAB正常释放次数_电信_QCI5
    a_erab_normal_plmn2_qci6 = ''    #E-RAB正常释放次数_电信_QCI6
    a_erab_normal_plmn2_qci7 = ''    #E-RAB正常释放次数_电信_QCI7
    a_erab_normal_plmn2_qci8 = ''    #E-RAB正常释放次数_电信_QCI8
    a_erab_normal_plmn2_qci9 = ''    #E-RAB正常释放次数_电信_QCI9
    #2022-5-23添加
    a_spi='电信'                     #承建运营商:联通/电信    
    a_share='是'                    #是否共享:是/否
    #2022-5-27添加
    a_rrc_max_plmn1 = ''			#RRC最大连接数(联通46001)
    a_rrc_max_plmn2 = ''			#RRC最大连接数(电信46011)
    #2022-7-29添加
    a_dl_prb_used=PuschPrbTotMeanDl    #下行PRB平均占用数
    a_dl_prb_total=PRB_Dl_Avail   #下行PRB可用数
    pm = [starttime,cell,a_tuifu,a_tongji,a_keyonglv,a_ganrao,a_cqi0,a_cqi1,a_cqi2,a_cqi3,a_cqi4,a_cqi5,a_cqi6,a_cqi7,a_cqi8,a_cqi9,a_cqi10,a_cqi11,a_cqi12,a_cqi13,a_cqi14,a_cqi15,a_cqi,a_cqigt7,a_cqilt7,a_ul_sdu_lost,a_ul_sdu_total,a_dl_sdu_lost,a_dl_sdu_total,a_ul_sdu_diubaolv,a_ul_sdu_diubaolv_qci1,a_dl_sdu_diubaolv_qci1,a_dl_sdu_qibaolv,a_ul_mbps,a_dl_mbps,a_rrc_req,a_rrc_suc,a_rrc_congest,a_rrc_avg,a_rrc_max,a_erab_req,a_erab_req_qci3,a_erab_req_qci4,a_erab_req_qci6,a_erab_req_qci7,a_erab_req_qci8,a_erab_req_qci9,a_erab_suc,a_erab_suc_qci3,a_erab_suc_qci4,a_erab_suc_qci6,a_erab_suc_qci7,a_erab_suc_qci8,a_erab_suc_qci9,a_erab_congest,a_s1_att,a_s1_suc,a_s1_suc_r,a_erab_suc_r,a_erab_suc_r_qci1,a_erab_suc_r_qci2,a_erab_suc_r_qci3,a_erab_suc_r_qci4,a_erab_suc_r_qci5,a_radio_suc_r,a_erab_abnormal,a_context_release_abnormal,a_context_release_normal,a_lte_drop_r,a_lte_release,a_ho_pingpang,a_ho_out_suc_r,a_ul_tra_mb,a_dl_tra_mb,a_total_tra_mb,a_rrc_fail_license,a_rrc_conn_suc,a_ul_prb_utilization,a_dl_prb_utilization,a_rrc_congest_r_license,a_shuangliubi,a_radio_utilization,a_sgnb_add_req,a_sgnb_add_suc,a_sgnb_add_r,a_dl_16qam_utilization,a_dl_64qam_utilization,a_erab_congest_radio,a_erab_congest_trans,a_erab_fail_ue,a_erab_fail_core,a_erab_fail_trans,a_erab_fail_radio,a_erab_fail_resource,a_rrc_release_csfb,a_dl_highmsc_r,a_ul_highmsc_r,a_csfb_suc,a_ho_s1_out_req,a_ho_s1_out_suc,a_ho_x2_out_req,a_ho_x2_out_suc,a_erab_req_qci1,a_erab_req_qci5,a_erab_suc_qci1,a_erab_suc_qci5,a_erab_normal_qci1,a_erab_abnormal_qci1,a_lte_drop_r_qci1,a_lru_blind,a_lru_not_blind,
            #第二批新加
            a_succoutintraenb,a_attoutintraenb,a_rru_puschprbassn,a_rru_puschprbtot,a_rru_pdschprbassn,a_rru_pdschprbtot,a_effectiveconnmean,a_effectiveconnmax,a_pdcch_signal_occupy_ratio,a_rru_pdcchcceutil,a_rru_pdcchcceavail,a_succexecinc,a_succconnreestab_nonsrccell,a_rrc_reconn_rate,a_enb_handover_succ_rate,a_down_pdcch_ch_cce_occ_rate,a_down_pdcp_sdu_avg_delay,a_mr_sinrul_gt0_ratio,a_mr_sinrul_gt0_ratio_fz,a_mr_sinrul_gt0_ratio_fm,a_vendor,a_lte_wireless_drop_ratio_cell,
            ##以下是2022-4-15第三批新增
            a_pdcp_sdu_vol_ul_plmn1,a_pdcp_sdu_vol_dl_plmn1,a_effectiveconnmean_plmn1,a_erab_abnormal_plmn1,a_erab_normal_plmn1,a_pdcp_sdu_vol_ul_plmn2,a_pdcp_sdu_vol_dl_plmn2,a_effectiveconnmean_plmn2,a_erab_abnormal_plmn2,a_erab_normal_plmn2,a_erab_ini_setup_att_plmn1_qci1,a_erab_ini_setup_att_plmn1_qci2,a_erab_ini_setup_att_plmn1_qci3,a_erab_ini_setup_att_plmn1_qci4,a_erab_ini_setup_att_plmn1_qci5,a_erab_ini_setup_att_plmn1_qci6,a_erab_ini_setup_att_plmn1_qci7,a_erab_ini_setup_att_plmn1_qci8,a_erab_ini_setup_att_plmn1_qci9,a_erab_ini_setup_succ_plmn1_qci1,a_erab_ini_setup_succ_plmn1_qci2,a_erab_ini_setup_succ_plmn1_qci3,a_erab_ini_setup_succ_plmn1_qci4,a_erab_ini_setup_succ_plmn1_qci5,a_erab_ini_setup_succ_plmn1_qci6,a_erab_ini_setup_succ_plmn1_qci7,a_erab_ini_setup_succ_plmn1_qci8,a_erab_ini_setup_succ_plmn1_qci9,a_erab_add_setup_att_plmn1_qci1,a_erab_add_setup_att_plmn1_qci2,a_erab_add_setup_att_plmn1_qci3,a_erab_add_setup_att_plmn1_qci4,a_erab_add_setup_att_plmn1_qci5,a_erab_add_setup_att_plmn1_qci6,a_erab_add_setup_att_plmn1_qci7,a_erab_add_setup_att_plmn1_qci8,a_erab_add_setup_att_plmn1_qci9,a_erab_add_setup_succ_plmn1_qci1,a_erab_add_setup_succ_plmn1_qci2,a_erab_add_setup_succ_plmn1_qci3,a_erab_add_setup_succ_plmn1_qci4,a_erab_add_setup_succ_plmn1_qci5,a_erab_add_setup_succ_plmn1_qci6,a_erab_add_setup_succ_plmn1_qci7,a_erab_add_setup_succ_plmn1_qci8,a_erab_add_setup_succ_plmn1_qci9,a_erab_abnormal_plmn1_qci1,a_erab_abnormal_plmn1_qci2,a_erab_abnormal_plmn1_qci3,a_erab_abnormal_plmn1_qci4,a_erab_abnormal_plmn1_qci5,a_erab_abnormal_plmn1_qci6,a_erab_abnormal_plmn1_qci7,a_erab_abnormal_plmn1_qci8,a_erab_abnormal_plmn1_qci9,a_erab_normal_plmn1_qci1,a_erab_normal_plmn1_qci2,a_erab_normal_plmn1_qci3,a_erab_normal_plmn1_qci4,a_erab_normal_plmn1_qci5,a_erab_normal_plmn1_qci6,a_erab_normal_plmn1_qci7,a_erab_normal_plmn1_qci8,a_erab_normal_plmn1_qci9,a_erab_ini_setup_att_plmn2_qci1,a_erab_ini_setup_att_plmn2_qci2,a_erab_ini_setup_att_plmn2_qci3,a_erab_ini_setup_att_plmn2_qci4,a_erab_ini_setup_att_plmn2_qci5,a_erab_ini_setup_att_plmn2_qci6,a_erab_ini_setup_att_plmn2_qci7,a_erab_ini_setup_att_plmn2_qci8,a_erab_ini_setup_att_plmn2_qci9,a_erab_ini_setup_succ_plmn2_qci1,a_erab_ini_setup_succ_plmn2_qci2,a_erab_ini_setup_succ_plmn2_qci3,a_erab_ini_setup_succ_plmn2_qci4,a_erab_ini_setup_succ_plmn2_qci5,a_erab_ini_setup_succ_plmn2_qci6,a_erab_ini_setup_succ_plmn2_qci7,a_erab_ini_setup_succ_plmn2_qci8,a_erab_ini_setup_succ_plmn2_qci9,a_erab_add_setup_att_plmn2_qci1,a_erab_add_setup_att_plmn2_qci2,a_erab_add_setup_att_plmn2_qci3,a_erab_add_setup_att_plmn2_qci4,a_erab_add_setup_att_plmn2_qci5,a_erab_add_setup_att_plmn2_qci6,a_erab_add_setup_att_plmn2_qci7,a_erab_add_setup_att_plmn2_qci8,a_erab_add_setup_att_plmn2_qci9,a_erab_add_setup_succ_plmn2_qci1,a_erab_add_setup_succ_plmn2_qci2,a_erab_add_setup_succ_plmn2_qci3,a_erab_add_setup_succ_plmn2_qci4,a_erab_add_setup_succ_plmn2_qci5,a_erab_add_setup_succ_plmn2_qci6,a_erab_add_setup_succ_plmn2_qci7,a_erab_add_setup_succ_plmn2_qci8,a_erab_add_setup_succ_plmn2_qci9,a_erab_abnormal_plmn2_qci1,a_erab_abnormal_plmn2_qci2,a_erab_abnormal_plmn2_qci3,a_erab_abnormal_plmn2_qci4,a_erab_abnormal_plmn2_qci5,a_erab_abnormal_plmn2_qci6,a_erab_abnormal_plmn2_qci7,a_erab_abnormal_plmn2_qci8,a_erab_abnormal_plmn2_qci9,a_erab_normal_plmn2_qci1,a_erab_normal_plmn2_qci2,a_erab_normal_plmn2_qci3,a_erab_normal_plmn2_qci4,a_erab_normal_plmn2_qci5,a_erab_normal_plmn2_qci6,a_erab_normal_plmn2_qci7,a_erab_normal_plmn2_qci8,a_erab_normal_plmn2_qci9,
            #2022-5-23
            a_spi,a_share,
            #2022-5-27添加
            a_rrc_max_plmn1,a_rrc_max_plmn2,
            #2022-7-29添加
            a_dl_prb_used,a_dl_prb_total
            ]
    return pm

    



def getData():
    new_list = []
    # 删除临时文件
    if os.path.exists(temp_path):
        shutil.rmtree(temp_path)
        os.mkdir(temp_path)
    if not os.path.exists(out_path_file):
        os.mkdir(out_path_file)   
    #fileList = []
    usetime = gettime()
    fileList = glob.glob(in_path+'/OMC*/'+usetime+'/*')
    
    if len(fileList) == 0:
        return False
    fileCOntent = ''    
    for file in fileList:
        with gzip.open(file, 'rt') as f:   
            l = 0  
            for line in f:
                l =l+1
                if l != 1:
                    new_list.append(split_line(line))
    if len(new_list) > 0:
        #print(new_list)
        with open(output_name+'.temp', 'w',newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(new_list)
            os.rename(output_name+'.temp',output_name)
    bt = time.time()
    print(bt-at)
    logger.info("处理完成，共%d行数据" % len(new_list))            



def main():
    getData()

if __name__ == '__main__':
    main()     









