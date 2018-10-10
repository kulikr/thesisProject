import pickle


path ="./sessions_8.pickle"

# Reads the events from a given path starting from position
def read_sessions_from_file(path, numOfSessionsToRead, position=0):
    sessionsList = []
    with(open(path, mode='rb')) as f:
        f.seek(position)
        i = 0
        while i < numOfSessionsToRead:
            try:
                tmp=pickle.load(f)
                if isinstance(tmp, (list,)):
                    sessionsList.append(tmp[0])
                else:
                    sessionsList.append(tmp)
            except EOFError:
                print("file read to end")
                return sessionsList, -1
            i += 1
        position = f.tell()
    return sessionsList, position


def writeDictToCsv(dict,fileName):
        for sub_dict_name,sub_dict in dict.items():
            with open("BasketClicksDistResults/"+fileName+"_"+str(sub_dict_name), mode="w+") as f:
                for key, value in sub_dict.items():
                    f.write(str(key)+","+str(value)+"\n")


def writeDictWithListsToCsv(dict, fileName):
    for sub_dict_name, sub_dict in dict.items():
        with open("BasketClicksDistResults/" + fileName + "_" + str(sub_dict_name)+".csv", mode="w+") as f:
            for i in range(len(sub_dict)):
                f.write(str(i) + "," + str(sub_dict[i]) + "\n")


month = "8"
# reg_bef= {}
# reg_aft= {}
# tmp_bef= {}
# tmp_aft= {}
#
# for dict_name in ["buy", "noBuy"]:
#     reg_bef[dict_name] = {}
#     reg_aft[dict_name] = {}
#     tmp_bef[dict_name] = {}
#     tmp_aft[dict_name] = {}
#
# for i in range(400):
#     reg_bef["buy"][str(i)] = 0
#     reg_bef["noBuy"][str(i)] = 0
#     reg_aft["buy"][str(i)] = 0
#     reg_aft["noBuy"][str(i)] = 0
#     tmp_bef["buy"][str(i)] = 0
#     tmp_bef["noBuy"][str(i)] = 0
#     tmp_aft["buy"][str(i)] = 0
#     tmp_aft["noBuy"][str(i)] = 0
#
# sessions_batch, position = read_sessions_from_file(path, 1000)
#
# j=0
#
#
# while 1:
#     for session in sessions_batch:
#         if not session["isBasketSession"]:
#             continue
#
#         len_bef = 0
#         len_aft = 0
#         is_tmp = not session["isLoggedIn"]
#         hadBasket = False
#         for i in range(len(session['events'])):
#             if session['events'][i]["eventType"] == "basket":
#                 hadBasket=True
#                 continue
#             if session['events'][i]["eventType"] == "buy":
#                 continue
#
#             if not hadBasket:
#                 len_bef+=1
#             else:
#                 len_aft+=1
#         try:
#             if is_tmp:
#                 if session["isBuySession"]:
#                     tmp_bef["buy"][str(len_bef)]+=1
#                     tmp_aft["buy"][str(len_aft)]+=1
#                 else:
#                     tmp_bef["noBuy"][str(len_bef)]+=1
#                     tmp_aft["noBuy"][str(len_aft)]+=1
#             else:
#                 if session["isBuySession"]:
#                     reg_bef["buy"][str(len_bef)]+=1
#                     reg_aft["buy"][str(len_aft)]+=1
#                 else:
#                     reg_bef["noBuy"][str(len_bef)]+=1
#                     reg_aft["noBuy"][str(len_aft)]+=1
#         except:
#             print("too long session")
#
#
#     if position == -1:
#         break
#     j+=1
#     print("num of sessions:" + str(j*1000))
#     sessions_batch, position = read_sessions_from_file(path, 1000, position)
#
#
# writeDictToCsv(reg_bef,"reg_bef"+"_"+month)
# writeDictToCsv(reg_aft,"reg_aft"+"_"+month)
# writeDictToCsv(tmp_bef,"tmp_bef"+"_"+month)
# writeDictToCsv(tmp_aft,"tmp_aft"+"_"+month)


reg_bef= {}
reg_aft= {}
tmp_bef= {}
tmp_aft= {}
time_to_buy = {}
time_to_buy["tmp"] = []
time_to_buy["reg"] = []

for dict_name in ["buy", "noBuy"]:
    reg_bef[dict_name] = []
    reg_aft[dict_name] = []
    tmp_bef[dict_name] = []
    tmp_aft[dict_name] = []


sessions_batch, position = read_sessions_from_file(path, 1000)
j = 0
tmp_sessions = 0
reg_sessions = 0

while 1:
    for session in sessions_batch:
        if not session["isBasketSession"]:
            continue
        len_bef = 0
        len_aft = 0
        hadBasket = False
        for i in range(len(session['events'])):
            if session['events'][i]["eventType"] == "basket":
                hadBasket=True
            if session['events'][i]["eventType"] == "buy":
                if session["isTmp"]:
                    time_to_buy["tmp"].append(len_aft)
                else:
                    time_to_buy["reg"].append(len_aft)
            if not hadBasket:
                len_bef+=session['events'][i]["dwellTime"]
            else:
                len_aft+=session['events'][i]["dwellTime"]
        try:
            if session["isTmp"]:
                tmp_sessions += 1
                if session["isBuySession"]:
                    tmp_bef["buy"].append(len_bef)
                    tmp_aft["buy"].append(len_aft)
                else:
                    tmp_bef["noBuy"].append(len_bef)
                    tmp_aft["noBuy"].append(len_aft)
            else:
                reg_sessions += 1
                if session["isBuySession"]:
                    reg_bef["buy"].append(len_bef)
                    reg_aft["buy"].append(len_aft)
                else:
                    reg_bef["noBuy"].append(len_bef)
                    reg_aft["noBuy"].append(len_aft)
        except:
            print("too long session")

    if position == -1:
        break
    j+=1
    print("num of sessions:" + str(j*1000))
    sessions_batch, position = read_sessions_from_file(path, 1000, position)

print("num of tmp sessions "+ str(tmp_sessions))
print("num of reg sessions "+ str(reg_sessions))

writeDictWithListsToCsv(reg_bef,"reg_bef_time"+"_"+month)
writeDictWithListsToCsv(reg_aft,"reg_aft_time"+"_"+month)
writeDictWithListsToCsv(tmp_bef,"tmp_bef_time"+"_"+month)
writeDictWithListsToCsv(tmp_aft,"tmp_aft_time"+"_"+month)
writeDictWithListsToCsv(time_to_buy,"time_from_basket_to_buy_"+month)