from pyspark import SparkContext


sc = SparkContext("local","Simple App")
# 转换成invert list
inverted_list = []
f = open("random.dat", "r")
# 记录行数
cnt = 0
for line in f:
    cnt += 1
    line = line[:-1]
    str_arr = line.split(' ')
    for e in str_arr:
        inverted_list.append((e, [(cnt, len(str_arr))]))
#print(inverted_list)

# 生成invert list set
textfile = sc.parallelize(inverted_list)
inverted_list_set = textfile.reduceByKey(lambda a, b: a+b).collect()

# 遍历list，生成所有的pair
pair_list = []
for item in inverted_list_set:
    length = len(item[1])
    for i in range(length):
        for j in range(i+1, length):
            if item[1][i][0] < item[1][j][0]:
                pair = (item[1][i], item[1][j])
            else:
                pair = (item[1][j], item[1][i])
            pair_list.append(pair)

textfile = sc.parallelize(pair_list)
count = textfile.map(lambda pairs: (pairs, 1)).reduceByKey(lambda a, b: a+b)

# 得到count list
count_list = count.collect()

roll = 0.6

# filter
res_list = []
for item in count_list:
    pair = item[0]
    alpha = roll/(1+roll) * (pair[0][1]+pair[1][1])
    if item[1] > alpha:
        res_list.append(pair)
print(res_list)

# output result
res_file = open("res.txt", "w")
for item in res_list:
    s = "R"+str(item[0][0])+", R"+str(item[1][0])+"\n"
    res_file.writelines(s)
res_file.close()