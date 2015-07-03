#!/bin/sh

#初始化W矩阵,三个参数：三元组矩阵、输出的W矩阵、分类数目
#hadoop jar /data2/liaoyida/pnmf/pnmf_init_w.jar /user/hive/warehouse/analyse.db/songyida_one7 /user/liaoyida/pnmf/input_w 200 
 
#初始化H矩阵,三个参数：三元组矩阵、输出的H矩阵、分类数目
#hadoop jar /data2/liaoyida/pnmf/pnmf_init_h.jar /user/hive/warehouse/analyse.db/songyida_one7 /user/liaoyida/pnmf/input_h 200 

for((i=0;i<5;i++))
do 

echo '第'${i}'次迭代开始' >>iterator.txt	
sendbegin=`date '+%Y-%m-%d %H:%M:%S'`
echo ${sendbegin} >>iterator.txt


#更新H矩阵
#step 1，四个参数：三元组矩阵、W矩阵、输出路径output_1、W矩阵父目录
hadoop jar /data2/liaoyida/pnmf/pnmf_step1.jar /user/hive/warehouse/analyse.db/songyida_one7/*.gz /user/liaoyida/pnmf/input_w/*.gz /user/liaoyida/pnmf/output_1 songyida_one7 input_w

#step 2，两个参数：输入output_1、输出output_2
hadoop jar /data2/liaoyida/pnmf/pnmf_step2.jar /user/liaoyida/pnmf/output_1/*.gz  /user/liaoyida/pnmf/output_2

#step 3，两个参数：输入W矩阵、输出output_3
hadoop jar /data2/liaoyida/pnmf/pnmf_step3_1.jar /user/liaoyida/pnmf/input_w/*.gz /user/liaoyida/pnmf/output_3 

#step 3.1,两个参数，合并output_3为一个文件，
hadoop jar pnmf_step3_2.jar /user/liaoyida/pnmf/output_3/*.gz /user/liaoyida/pnmf/output_3_1

#step 4，三个参数：输入output_3、H矩阵、输出output_4
hadoop jar /data2/liaoyida/pnmf/pnmf_step4.jar /user/liaoyida/pnmf/output_3_1/part-r-00000 /user/liaoyida/pnmf/input_h/*.gz /user/liaoyida/pnmf/output_4

#step 5，七个参数：H矩阵、output_2、output_4、输出output_5、H矩阵父目录、output_2、output_4
hadoop jar /data2/liaoyida/pnmf/pnmf_step5.jar /user/liaoyida/pnmf/input_h/*.gz /user/liaoyida/pnmf/output_2/*.gz /user/liaoyida/pnmf/output_4/*.gz /user/liaoyida/pnmf/output_5 input_h output_2 output_4

#更新W矩阵
#step 6，四个参数：三元组矩阵、H矩阵、输出路径output_new_1、H矩阵父目录
hadoop jar /data2/liaoyida/pnmf/pnmf_step6.jar /user/hive/warehouse/analyse.db/songyida_one7/*.gz /user/liaoyida/pnmf/input_h/*.gz /user/liaoyida/pnmf/output_new_1 songyida_one7 input_h

#step 7，两个参数：输入output_new_1、输出output_new_2
hadoop jar /data2/liaoyida/pnmf/pnmf_step2.jar /user/liaoyida/pnmf/output_new_1/*.gz  /user/liaoyida/pnmf/output_new_2 

#setp 8，两个参数：输入H矩阵、输出output_new_3
hadoop jar /data2/liaoyida/pnmf/pnmf_step3_1.jar /user/liaoyida/pnmf/input_h/*.gz /user/liaoyida/pnmf/output_new_3

#step 8.1,两个参数，合并output_3为一个文件，
hadoop jar pnmf_step3_2.jar /user/liaoyida/pnmf/output_new_3/*.gz /user/liaoyida/pnmf/output_new_3_1

#step 9，三个参数：输入output_new_3、W矩阵、输出output_new_4
hadoop jar /data2/liaoyida/pnmf/pnmf_step8.jar /user/liaoyida/pnmf/output_new_3_1/part-r-00000 /user/liaoyida/pnmf/input_w/*.gz /user/liaoyida/pnmf/output_new_4

#step 10，七个参数：W矩阵、output_new_2、output_new_4、输出output_new_5、W矩阵父目录、output_new_2、output_new_4
hadoop jar /data2/liaoyida/pnmf/pnmf_step5.jar /user/liaoyida/pnmf/input_w/*.gz /user/liaoyida/pnmf/output_new_2/*.gz /user/liaoyida/pnmf/output_new_4/*.gz /user/liaoyida/pnmf/output_new_5 input_w output_new_2 output_new_4

hadoop fs -rmr  /user/liaoyida/pnmf/input_w
hadoop fs -rmr  /user/liaoyida/pnmf/input_h
hadoop fs -mkdir /user/liaoyida/pnmf/input_w
hadoop fs -mkdir /user/liaoyida/pnmf/input_h
hadoop fs -mv /user/liaoyida/pnmf/output_5/* /user/liaoyida/pnmf/input_h
hadoop fs -mv /user/liaoyida/pnmf/output_new_5/* /user/liaoyida/pnmf/input_w
hadoop fs -rmr /user/liaoyida/pnmf/output_1
hadoop fs -rmr /user/liaoyida/pnmf/output_2
hadoop fs -rmr /user/liaoyida/pnmf/output_3
hadoop fs -rmr /user/liaoyida/pnmf/output_3_1
hadoop fs -rmr /user/liaoyida/pnmf/output_4
hadoop fs -rmr /user/liaoyida/pnmf/output_5
hadoop fs -rmr /user/liaoyida/pnmf/output_new_1
hadoop fs -rmr /user/liaoyida/pnmf/output_new_2
hadoop fs -rmr /user/liaoyida/pnmf/output_new_3
hadoop fs -rmr /user/liaoyida/pnmf/output_new_3_1
hadoop fs -rmr /user/liaoyida/pnmf/output_new_4
hadoop fs -rmr /user/liaoyida/pnmf/output_new_5

sendend=`date '+%Y-%m-%d %H:%M:%S'`
begin=$(date -d "$sendbegin" +%s)
end=$(date -d "$sendend" +%s)
diff=$((($end - $begin)/60))
echo '第'${i}'次迭代所花时间为：'${diff}'分钟' >>iterator.txt

done



#注释，更新的W矩阵文件存放形式跟W矩阵一样，更新的H矩阵文件是按列存储
#H的矩阵形式比如：                W的矩阵形式比如：
#	1  2  3                           1  2  3
#       4  5  6                           4  5  6
#H的文件矩阵则是                  W的文件矩阵则是
#       1|1|4                             1|1|2|3
#       2|2|5                             2|4|5|6
#       3|3|6








