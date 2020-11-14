import time
import json

global tag
global last

# 读取原始数据
with open(
        'C:\\codebase\\log\\274189_2020_05_09_19_00_00_2020_05_09_21_00_00_Motion.json',
        encoding='utf-8') as f:
    rawdata = f.readlines()[0]
    rawdata = rawdata.replace(']', '],')
    rawdata = '[' + rawdata
    rawdata = rawdata[:-1] + ']'
    rawdata = json.loads(rawdata)
    print(len(rawdata))
    f.close()

# 分别为处理原始数据用的字典，化成占用数据的字典，结果字典
countervalue = {}
freeoccupy = {}
percentage = {}

# 起始时间的文本形式和毫秒级时间戳形式
STARTTIME = '2020-05-09 19:00:00'
start = int(time.mktime(time.strptime(STARTTIME,
                                      '%Y-%m-%d %H:%M:%S'))) * 1000  #
TIMESTEP = 600000  #计算间隔

# 处理原始数据
for persensordata in rawdata:
    for motionrecord in persensordata:
        grid = (motionrecord['sampleTime'] - start) // TIMESTEP
        if motionrecord['DID'] not in countervalue.keys():
            countervalue[motionrecord['DID']] = {}
            freeoccupy[motionrecord['DID']] = {}
            percentage[motionrecord['DID']] = {}
            countervalue[motionrecord['DID']][grid] = [[
                motionrecord['sampleTime']
            ], [motionrecord['value']]]
        else:
            if grid not in countervalue[motionrecord['DID']].keys():
                countervalue[motionrecord['DID']][grid] = [[
                    motionrecord['sampleTime']
                ], [motionrecord['value']]]
            else:
                countervalue[motionrecord['DID']][grid][0].append(
                    motionrecord['sampleTime'])
                countervalue[motionrecord['DID']][grid][1].append(
                    motionrecord['value'])
'''建立计数器map
countervalue = 
{
    'did-1':{
        t1:[[T1,T2,T3,T4...],[c1,c2,c3,c4...]],
        t2:[[T1,T2,T3,T4...],[c1,c2,c3,c4...]],
        ...
        ...
        tn:[[T1,T2,T3,T4...],[c1,c2,c3,c4...]]
    },
    'did-2':{
        t1:[[T1,T2,T3,T4...],[c1,c2,c3,c4...]],
        t2:[[T1,T2,T3,T4...],[c1,c2,c3,c4...]],
        ...
        ...
        tn:[[T1,T2,T3,T4...],[c1,c2,c3,c4...]]
    },
    ...
    ...
    ...
    'did-n':{
        t1:[[T1,T2,T3,T4...],[c1,c2,c3,c4...]],
        t2:[[T1,T2,T3,T4...],[c1,c2,c3,c4...]],
        ...
        ...
        tn:[[T1,T2,T3,T4...],[c1,c2,c3,c4...]]
    }
}
'''

# print(countervalue)

for uid in countervalue.keys():
    tag = 'free'
    lastvalue = None
    for timegrid in countervalue[uid].keys():
        # 一个用来记录时间，一个用来记录状态变化
        timelist = []
        datalist = []
        for iValues in range(len(countervalue[uid][timegrid][1])):
            if lastvalue == None:  # 记录前一个value,已进行比较,得出free还是occupy
                lastvalue = countervalue[uid][timegrid][1][iValues]
            elif lastvalue != countervalue[uid][timegrid][1][
                    iValues]:  # 说明检测到了移动
                # 当从free变为occupied时，记录状态和对应的时间
                if tag == 'free':
                    tag = 'occupied'
                    lastvalue = countervalue[uid][timegrid][1][iValues]
                    timelist.append(countervalue[uid][timegrid][0][iValues])
                    datalist.append('occupied')
                else:
                    # 更新values,但是状态没有变化
                    lastvalue = countervalue[uid][timegrid][1][iValues]
            else:  # value没有变化,变为free
                # 当从occupied变为free时，记录状态和对应的时间
                if tag == 'occupied':
                    tag = 'free'
                    timelist.append(countervalue[uid][timegrid][0][iValues])
                    datalist.append('free')
        if (len(datalist)) == 0:
            timelist.append(tag)
        freeoccupy[uid][timegrid] = [timelist, datalist]
# print(res)
'''
freeoccupy = 
{
    'did-1':{
        t1:[[T1,T2,T3,T4...],[free,occupied,free,occupied...]],  ===>正常格式
        t2:[[occupied],[]],                                      ===>若整个十分钟内状态无变化，则状态list为空，时间list内为当前时段内的状态，此处表示这10分钟一直处于occupied状态
        ...
        ...
        tn:[[T1],[free]]                                         ===>10分钟内只有一次状态变化
    },
    'did-2':{
        t1:[[T1,T2,T3,T4...],[free,occupied,free,occupied...]],
        t2:[[T1,T2,T3,T4...],[free,occupied,free,occupied...]],
        ...
        ...
        tn:[[T1,T2,T3,T4...],[free,occupied,free,occupied...]]
    },
    ...
    ...
    ...
    'did-n':{
        t1:[[T1,T2,T3,T4...],[free,occupied,free,occupied...]],
        t2:[[T1,T2,T3,T4...],[free,occupied,free,occupied...]],
        ...
        ...
        tn:[[T1,T2,T3,T4...],[free,occupied,free,occupied...]]
    }
}
'''

# 循环res字典
for uid in freeoccupy.keys():
    for timegrid in freeoccupy[uid].keys():
        occupied_time = 0.0
        first_record = ''
        last_record = ''
        util = 0.0
        begin = start + timegrid * TIMESTEP
        end = start + (timegrid + 1) * TIMESTEP

        # 如果当前数据list长度为0，则代表10分钟内状态没变，直接根据状态名称获取0或100的占用值
        if len(freeoccupy[uid][timegrid][1]) == 0:
            if freeoccupy[uid][timegrid][0] == 'free':
                util = format(0.0 * 100.0, '.3f')
            elif freeoccupy[uid][timegrid][0] == 'occupied':
                util = format(1.0 * 100.0, '.3f')

        # 如果当前数据list长度为1，则代表10分钟内状态只变化过1次。若状态变化为占用，则选取变化后的时间计算。若状态变化为空闲，则选取变化前的时间进行计算。
        elif len(freeoccupy[uid][timegrid][1]) == 1:
            first_gap = freeoccupy[uid][timegrid][0][0] - begin
            last_gap = end - freeoccupy[uid][timegrid][0][0]

            if freeoccupy[uid][timegrid][1][0] == 'occupied':
                util = format(last_gap / TIMESTEP * 100.0, '.3f')
            elif freeoccupy[uid][timegrid][1][0] == 'free':
                util = format(first_gap / TIMESTEP * 100.0, '.3f')

        # 若数据长度超过1，则先选取开始到第一个数据和最后一个数据到结尾的时间。
        # 然后根据第一个以及最后一个状态决定是否删除开头及结尾的状态。
        # 处理完成后，剩下的依次计算占用时间
        # 最后根据原本开头和结尾的状态决定占用时间需不需要加上最开始选取的开头/结尾时间
        else:
            first_gap = freeoccupy[uid][timegrid][0][0] - begin
            last_gap = end - freeoccupy[uid][timegrid][0][-1]

            if freeoccupy[uid][timegrid][1][0] == 'free':
                first_record = 'free'
                del freeoccupy[uid][timegrid][0][0], freeoccupy[uid][timegrid][
                    1][0]

            if freeoccupy[uid][timegrid][1][-1] == 'occupied':
                last_record = 'occupied'
                del freeoccupy[uid][timegrid][0][-1], freeoccupy[uid][
                    timegrid][1][-1]

            indexList = [
                iValues for iValues in range(len(freeoccupy[uid][timegrid][1]))
                if freeoccupy[uid][timegrid][1][iValues] == 'occupied'
            ]

            for j in indexList:
                occupied_time += ((freeoccupy[uid][timegrid][0][j + 1] -
                                   freeoccupy[uid][timegrid][0][j]) * 1.0)

            if first_record:
                occupied_time += first_gap

            if last_record:
                occupied_time += last_gap

            util = format(occupied_time / TIMESTEP * 100, '.3f')

        percentage[uid][timegrid] = util

with open('C:\\codebase\\log\\test.csv', 'a+', encoding='utf-8',
          newline='') as f:
    writer = csv.writer(f)
    for did in percentage.keys():
        for timegrid in percentage[did].keys():
            epoch_time = (int(timegrid) * TIMESTEP + start) / 1000.0
            recordTime = time.strftime("%Y-%m-%d %H:%M:%S",
                                       time.localtime(epoch_time))
            util = percentage[did][timegrid]
            writer.writerow([did, recordTime, util])
    f.close()

# print(occ)
print("Job done!!!!!!!")
