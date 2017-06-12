#coding:utf-8
#Actor:Tyson
import requests
import pymongo
from config import *
from multiprocessing import Pool
import random
client = pymongo.MongoClient(MONGO_URL, connect=False)
db = client[MONGO_DB]
proxies=None
proxy=None
global erro
erro=1
#获取代理
def get_proxy():
    try:
        response = requests.get(PROXY_POOL_URL)
        if response.status_code == 200:
            return response.text
        return None
    except ConnectionError:
        return None

#获取返回结果
def get_html(url, count=1):
    global  erro
    erro=1
    # print('Crawling', url)
    # print('Trying Count', count)
    public_headers =random.choice(public_headerses)
    global proxy
    if count >= MAX_COUNT:
        print('Tried Too Many Counts for:',url)
        erro=0
        return None
    try:
        if proxy:
            proxies = {
                'http': 'http://' + proxy
            }
            response = requests.get(url, allow_redirects=False, headers=public_headers, proxies=proxies)
        else:
            response = requests.get(url, allow_redirects=False, headers=public_headers)
        if response.status_code == 200 or response.status_code==304:
            return response.json()
        if response.status_code is not 200:
            # Need Proxy
            print(response.status_code)
            proxy = get_proxy()
            if proxy:
                print('Using Proxy', proxy)
                count += 1
                return get_html(url,count)
            else:
                print('Get Proxy Failed')
                erro=0
                return None
    except ConnectionError as e:
        print('Error Occurred', e.args)
        proxy = get_proxy()
        count += 1
        return get_html(url, count)

#给到id之后，从网页返回url_datas（包含假的id和year）
def get_nianbao(id):
    try:
        global w
        global erro

        url1='http://www.tianyancha.com/annualreport/newReport.json?id='+str(id)+'&year=2016'
        url3='http://www.tianyancha.com/annualreport/newReport.json?id='+str(id)+'&year=2015'
        response1=get_html(url1)
        response3=get_html(url3)
        if erro==1:
            if (response1 is not None and 'data' in response1) or (response3 is not None and 'data' in response3):
                if response1.get('data').get('baseInfo') is not None:
                    nianbaos={str(2016):response1.get('data')}.copy()
                    i=5
                    while i>=0:
                        url2='http://www.tianyancha.com/annualreport/newReport.json?id='+str(id)+'&year='+str(2010+i)
                        response2=get_html(url2)
                        if response2 is not None and 'data' in response2:
                            baseInfo=response2.get('data').get('baseInfo')
                            if  baseInfo is not None and 'reportYear' in baseInfo:
                                a={str(2010+i):response2.get('data')}
                                nianbaos.update(a)
                                i=i-1
                            else:
                                return nianbaos
                        else:
                            return nianbaos
                    return nianbaos
                elif response3.get('data').get('baseInfo') is not None:
                    nianbaos={str(2015):response3.get('data')}.copy()
                    i=4
                    while i>=0:
                        url2='http://www.tianyancha.com/annualreport/newReport.json?id='+str(id)+'&year='+str(2010+i)
                        response2=get_html(url2)
                        if response2 is not None and 'data' in response2:
                            baseInfo=response2.get('data').get('baseInfo')
                            if  baseInfo is not None and 'reportYear' in baseInfo:
                                a={str(2010+i):response2.get('data')}
                                nianbaos.update(a)
                                i=i-1
                            else:
                                return nianbaos
                        else:
                            return nianbaos
                    return nianbaos
                return None
            return None
        else:
            print('获取年报信息失败')
            w=0
            return None
    except:
        print('获取年报信息失败,异常跳出')
        w=0
        return None

#获取公司人员信息
def get_staff_informaton(id):
    try:
        global w
        global erro

        url='http://www.tianyancha.com/expanse/staff.json?id='+str(id)+'&ps=20&pn=1'
        response=get_html(url)
        if erro==1:
            if response is not None and  'data' in response:
                staff_information=response.get('data')
                if staff_information is not None:
                    return staff_information
            return None
        else:
            print('获取公司人员信息失败')
            w=0
            return None
    except:
        print('获取公司人员信息失败,异常跳出')
        w=0
        return None

#获取公司股东信息
def get_holder_informaton(id):
    try:
        global w

        global erro

        url='http://www.tianyancha.com/expanse/holder.json?id='+str(id)+'&ps=20&pn=1'
        response=get_html(url)
        if erro==1:
            if response is not None and  'data' in response:
                holder_information=response.get('data')
                if holder_information is not None:
                    return holder_information
            return None
        else:
            print('获取公司股东信息失败')
            w=0
            return None
    except:
        print('获取公司股东信息失败,异常跳出')
        w=0
        return None


#获取公司对外投资信息
def get_inverst_informaton(id):
    try:
        global w
        global erro
        url='http://www.tianyancha.com/expanse/inverst.json?id='+str(id)+'&ps=20&pn=1'
        response=get_html(url)
        if erro==1:
            if response is not None and  'data' in response:
                inverst_information=response.get('data')
                if inverst_information is not None:
                    return inverst_information
            return None
        else:
            print('获取公司对外投资信息失败')
            w=0
            return None
    except:
        print('获取公司对外投资信息失败,异常跳出')
        w=0
        return None


#获取公司变更记录
def get_changeinfo_informaton(id):
    try:
        global w

        global erro

        url='http://www.tianyancha.com/expanse/changeinfo.json?id='+str(id)+'&ps=5&pn=1'
        response=get_html(url)
        if erro==1:
            if response is not None and  'data' in response:
                changeinfo_informaton=response.get('data')
                if changeinfo_informaton is not None:
                    return changeinfo_informaton
            return None
        else:
            print('获取公司变更记录失败')
            w=0
            return None
    except:
        print('获取公司变更记录失败,异常跳出')
        w=0
        return None


#获取公司分支机构
def get_branch_informaton(id):
    try:
        global w

        global erro

        url='http://www.tianyancha.com/expanse/branch.json?id='+str(id)+'&ps=10&pn=1'
        response=get_html(url)
        if erro==1:
            if response is not None and  'data' in response:
                branch_informaton=response.get('data')
                if branch_informaton is not None:
                    return branch_informaton
            return None
        else:
            print('请求公司股东信息失败')
            w=0
            return None
    except:
        print('请求公司股东信息失败,异常跳出')
        w=0
        return None
#获取公司融资历史
def get_findHistoryRongzi(company_name):
    try:
        global w

        global erro

        url='http://www.tianyancha.com/expanse/findHistoryRongzi.json?name='+company_name+'&ps=10&pn=1'
        response=get_html(url)
        if erro==1:
            if response is not None and  'data' in response:
                findHistoryRongzi=response.get('data')
                if findHistoryRongzi is not None:
                    return findHistoryRongzi
            return None
        else:
            print('获取公司融资历史失败')
            w=0
            return None
    except:
        print('获取公司融资历史失败,异常跳出')
        w=0
        return None

#获取公司核心团队
def get_findTeamMember(company_name):
    try:
        global w

        global erro

        url='http://www.tianyancha.com/expanse/findTeamMember.json?name='+company_name+'&ps=5&pn=1'
        response=get_html(url)
        if erro==1:
            if response is not None and  'data' in response:
                findTeamMember=response.get('data')
                if findTeamMember is not None:
                    return findTeamMember
            return None
        else:
            print('获取公司核心团队失败')
            w=0
            return None
    except:
        print('获取公司核心团队失败,异常跳出')
        w=0
        return None



#获取公司企业业务
def get_findProduct(company_name):
    try:
        global w

        global erro

        url='http://www.tianyancha.com/expanse/findProduct.json?name='+company_name+'&ps=15&pn=1'
        response=get_html(url)
        if erro==1:
            if response is not None and  'data' in response:
                findProduct=response.get('data')
                if findProduct is not None:
                    return findProduct
            return None
        else:
            print('获取公司企业业务失败')
            w=0
            return None
    except:
        print('获取公司企业业务失败,异常跳出')
        w=0
        return None

#获取公司投资事件
def get_findTzanli(company_name):
    try:
        global w

        global erro

        url='http://www.tianyancha.com/expanse/findTzanli.json?name='+company_name+'&ps=10&pn=1'
        response=get_html(url)
        if erro==1:
            if response is not None and  'data' in response:
                findTzanli=response.get('data')
                if findTzanli is not None:
                    return findTzanli
            return None
        else:
            print('获取公司投资事件失败')
            w=0
            return None
    except:
        print('获取公司投资事件失败,异常跳出')
        w=0
        return None

#获取竞品信息
def get_findJingpin(company_name):
    try:
        global w

        global erro

        url='http://www.tianyancha.com/expanse/findJingpin.json?name='+company_name+'&ps=10&pn=1'
        response=get_html(url)
        if erro==1:
            if response is not None and  'data' in response:
                findJingpin=response.get('data')
                if findJingpin is not None:
                    return findJingpin
            return None
        else:
            print('获取竞品信息')
            w=0
            return None
    except:
        print('获取竞品信息,异常跳出')
        w=0
        return None

#获取法律诉讼信息
def get_getlawsuit(company_name):
    try:
        global w

        global erro

        url='http://www.tianyancha.com/v2/getlawsuit/'+company_name+'.json?page=1&ps=10'
        response=get_html(url)
        if erro==1:
            if response is not None and  'data' in response:
                getlawsuit=response.get('data')
                if getlawsuit is not None:
                    return getlawsuit
            return None
        else:
            print('获取法律诉讼信息失败')
            w=0
            return None
    except:
        print('获取法律诉讼信息失败,异常跳出')
        w=0
        return None

#获取法院公告
def get_court(company_name):
    try:
        global w

        global erro

        url='http://www.tianyancha.com/v2/court/'+company_name+'.json?'
        response=get_html(url)
        if erro==1:
            if response is not None and  'courtAnnouncements' in response:
                court=response.get('courtAnnouncements')
                if court is not None:
                    return court
            return None
        else:
            print('获取法院公告失败')
            w=0
            return None
    except:
        print('获取法院公告失败,异常跳出')
        w=0
        return None

#获取公司司法中的被执行人
def get_zhixing(id):
    try:
        global w

        global erro

        url='http://www.tianyancha.com/expanse/zhixing.json?id='+str(id)+'&pn=1&ps=5'
        response=get_html(url)
        if erro==1:
            if response is not None and  'data' in response:
                zhixing=response.get('data')
                if zhixing is not None:
                    return zhixing
            return None
        else:
            print('获取法院公告失败')
            w=0
            return None
    except:
        print('获取法院公告失败,异常跳出')
        w=0
        return None
#获取公司经营异常
def get_abnormal(id):
    try:
        global w

        global erro

        url='http://www.tianyancha.com/expanse/abnormal.json?id='+str(id)+'&ps=5&pn=1'
        response=get_html(url)
        if erro==1:
            if response is not None and  'data' in response:
                abnormal=response.get('data')
                if abnormal is not None:
                    return abnormal
            return None
        else:
            print('获取公司经营异常失败')
            w=0
            return None
    except:
        print('获取公司经营异常失败,异常跳出')
        w=0
        return None
#获取公司行政处罚
def get_punishment(company_name):
    try:
        global w

        global erro

        url='http://www.tianyancha.com/expanse/punishment.json?name='+company_name+'&ps=5&pn=1'
        response1=get_html(url)
        if erro==1:
            if response1 is not None and  'data' in response1:
                if response1.get('data') is not None:
                    punishment={str(1):response1.get('data')}.copy()
                    rawcount=response1.get('data').get('count')//5
                    for i in range(rawcount):
                        url2='http://www.tianyancha.com/expanse/punishment.json?name='+company_name+'&ps=5&pn'+str(i+2)
                        response2=requests.get(url2)
                        if response2 is not None and  'data' in response2:
                            a={str(i+2):response2.get('data')}
                            punishment.update(a)
                    return punishment
                return None
            return None
        else:
            print('获取公司行政处罚失败')
            w=0
            return None
    except:
        print('获取公司行政处罚失败,异常跳出')
        w=0
        return None
#获取公司股权出质
def get_companyEquity(company_name):
    try:
        global w

        global erro

        url='http://www.tianyancha.com/expanse/companyEquity.json?name='+company_name+'&ps=5&pn=1'
        response=get_html(url)
        if erro==1:
            if response is not None and  'data' in response:
                companyEquity=response.get('data')
                if companyEquity is not None:
                    return companyEquity
            return None
        else:
            print('获取公司股权出质失败')
            w=0
            return None
    except:
        print('获取公司股权出质失败,异常跳出')
        w=0
        return None
#获取公司招投标
def get_bid(id):
    try:
        global w

        global erro

        url='http://www.tianyancha.com/expanse/bid.json?id='+str(id)+'&pn=1&ps=10'
        response=get_html(url)
        if erro==1:
            if response is not None and  'data' in response:
                bid=response.get('data')
                if bid is not None:
                    return bid
            return None
        else:
            print('获取公司招投标失败')
            w=0
            return None
    except:
        print('获取公司招投标失败,异常跳出')
        w=0
        return None
#获取公司招聘信息
def get_getEmploymentList(company_name):
    try:
        global w

        global erro

        url='http://www.tianyancha.com/extend/getEmploymentList.json?companyName='+company_name+'&pn=1&ps=10'
        response=get_html(url)
        if erro==1:
            if response is not None and  'data' in response:
                getEmploymentList=response.get('data')
                if getEmploymentList is not None:
                    return getEmploymentList
            return None
        else:
            print('获取公司招聘信息')
            w=0
            return None
    except:
        print('获取公司招聘信息,异常跳出')
        w=0
        return None
#获取公司产品信息
def get_appbkinfo(id):
    try:
        global w

        global erro

        url1='http://www.tianyancha.com/expanse/appbkinfo.json?id='+str(id)+'&ps=5&pn=1'
        response1=get_html(url1)
        if erro==1:
            if response1 is not None and  'data' in response1:
                if response1.get('data') is not None:
                    appbkinfo={str(1):response1.get('data')}.copy()
                    rawcount=response1.get('data').get('count')//5
                    for i in range(rawcount):
                        url2='http://www.tianyancha.com/expanse/appbkinfo.json?id='+str(id)+'&ps=5&pn='+str(i+2)
                        response2=get_html(url2)
                        if response2 is not None and  'data' in response1:
                            a={str(i+2):response2.get('data')}
                            appbkinfo.update(a)
                    return appbkinfo
                return None
            return None
        else:
            print('获取公司招聘信息失败')
            w=0
            return None
    except:
        print('获取公司招聘信息失败,异常跳出')
        w=0
        return None

#获取公司商标信息
def get_getTmList(id):
    try:
        global w

        global erro

        url1='http://www.tianyancha.com/tm/getTmList.json?id='+str(id)+'&pageNum=2&ps=5'
        response1=get_html(url1)
        if erro==1:
            if response1 is not None and  'data' in response1:
                if response1.get('data') is not None:
                    getTmList={str(1):response1.get('data')}.copy()
                    rawviewtotal=int(response1.get('data').get('viewtotal'))//5
                    for i in range(rawviewtotal):
                        url2='http://www.tianyancha.com/tm/getTmList.json?id='+str(id)+'&pageNum='+str(i+2)+'&ps=5'
                        response2=get_html(url2)
                        if response2 is not None and  'data' in response1:
                            a={str(i+2):response2.get('data')}
                            getTmList.update(a)
                    return getTmList
                return None
            return None
        else:
            print('获取公司商标信息失败')
            w=0
            return None
    except:
        print('获取公司商标信息失败,异常跳出')
        w=0
        return None
#获取公司专利信息
def get_patent(id):
    try:
        global w
        global erro
        url1='http://www.tianyancha.com/expanse/patent.json?id='+str(id)+'&pn=1&ps=5'
        response1=get_html(url1)
        if erro==1:
            if response1 is not None and  'data' in response1:
                if response1.get('data') is not None:
                    patent={str(1):response1.get('data')}.copy()
                    rawviewtotal=int(response1.get('data').get('viewtotal'))//5
                    for i in range(rawviewtotal):
                        url2='http://www.tianyancha.com/expanse/patent.json?id='+str(id)+'&pn='+str(i+2)+'&ps=5'
                        response2=get_html(url2)
                        if response2 is not None and  'data' in response2:
                            a={str(i+2):response2.get('data')}
                            patent.update(a)
                    return patent
                return None
            return None
        else:
            print('获取公司专利信息失败')
            w=0
            return None
    except:
        print('获取公司专利信息失败,异常跳出')
        w=0
        return None

def save_to_mongo_complete_information(all_information):
    if db[MONGO_TABLE1].insert(all_information):
        print('Waiting save data.............')
        print('保存信息数据中。。。。。。')
        print('Successfully Saved to Mongo')
        return True
    return False

def save_to_mongo_uncomplete_information(all_information):
    if db[MONGO_TABLE2].insert(all_information):
        print('Waiting save data.............')
        print('保存非全面信息数据中。。。。。。')
        print('Successfully Saved to Mongo')
        return True
    return False

def save_to_mongo_unnormal_information(all_information):
    if db[MONGO_TABLE3].insert(all_information):
        print('Waiting save data.............')
        print('保存未能爬去公司信息数据中。。。。。。')
        print('Successfully Saved to Mongo')
        return True
    return False


def main(keyword):
    global w
    global erro
    w=1
    erro=1
    company_name=keyword[0]
    id=keyword[1]
    line=keyword[2]
    try:
        print(id)
        print(company_name)
        #由id获取公司信息
        # print('获取公司信息中')
        nianbaos=get_nianbao(id)
        staff_informaton=get_staff_informaton(id)
        holder_informaton=get_holder_informaton(id)
        inverst_information=get_inverst_informaton(id)
        changeinfo_informaton=get_changeinfo_informaton(id)
        branch_informaton=get_branch_informaton(id)


        #由name获取公司发展信息
        # print('获取公司发展信息中')
        findHistoryRongzi=get_findHistoryRongzi(company_name)
        findTeamMember=get_findTeamMember(company_name)
        findProduct=get_findProduct(company_name)
        findTzanli=get_findTzanli(company_name)
        findJingpin=get_findJingpin(company_name)

        #由name获取公司司法风险信息
        # print('获取公司司法风险信息中')
        getlawsuit=get_getlawsuit(company_name)
        court=get_court(company_name)
        zhixing=get_zhixing(id)

        #由name获取公司经营风险信息
        # print('获取公司经营风险信息中')
        abnormal=get_abnormal(id)
        punishment=get_punishment(company_name)
        companyEquity=get_companyEquity(company_name)

        #由id获取公司经营状况
        # print('获取公司经营状况信息中')
        bid=get_bid(id)
        getEmploymentList=get_getEmploymentList(company_name)
        appbkinfo=get_appbkinfo(id)

        #获取公司商标信息
        # print('获取公司商标信息中')
        getTmList=get_getTmList(id)
        patent=get_patent(id)

        if company_name is not None:
            all_information={'公司名称':company_name,
                             'id':id,
                             # '公司基本信息':company_information,
                             '公司基本信息':line,
                             '公司主要人员':staff_informaton,
                             '公司股东信息':holder_informaton,
                             '公司对外投资信息':inverst_information,
                             '公司变更记录':changeinfo_informaton,
                             '公司年报':nianbaos,
                             '公司分支机构':branch_informaton,
                             '公司融资历史':findHistoryRongzi,
                             '公司核心团队':findTeamMember,
                             '公司企业业务':findProduct,
                             '公司投资事件':findTzanli,
                             '竞品信息':findJingpin,
                             '法律诉讼信息':getlawsuit,
                             '法院公告':court,
                             '公司法律被执行人':zhixing,
                             '公司经营异常':abnormal,
                             '公司行政处罚':punishment,
                             '公司股权出质':companyEquity,
                             '公司招投标':bid,
                             '公司招聘信息':getEmploymentList,
                             '公司产品信息':appbkinfo,
                             '公司商标信息':getTmList,
                             '公司专利信息':patent
                             }
            if w==0:
                save_to_mongo_uncomplete_information(all_information)
                w=1

            else:
                save_to_mongo_complete_information(all_information)
    except:
        w=1
        if company_name is not None:

            all_information={'公司名称':company_name,
                             '公司基本信息':line
                             }
            save_to_mongo_unnormal_information(all_information)
            print('获取此公司信息失败')


if __name__ == "__main__":
    keyword=[]
    for line in open('C:\\MongoDB\\a.txt',encoding='utf-8'):
        o=json.loads(line)
        # print(o)
        if 'Name' in o and 'Tid' in o:
            company_name=o.get('Name')
            id=o.get('Tid')
            key=[company_name,id,line]
            keyword.append(key)
    pool = Pool(20)
    pool.map(main, keyword)
    pool.close()
    pool.join()

    # keyword=['北京百度网讯科技有限公司',22822,{'data':'abc'}]
    # main(keyword)
    # keyword=['济南仁杰医疗器械有限公司',1333906548,{'data':'abc'}]
    # main(keyword)


