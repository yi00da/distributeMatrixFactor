#!/bin/sh



hadoop jar /data2/liaoyida/pnmf/Matrix_step1.jar /user/liaoyida/input_1/output1.txt /user/liaoyida/input_2/item_out.txt /user/liaoyida/output  input_1 input_2
hadoop jar /data2/liaoyida/pnmf/Matrix_step2.jar /user/liaoyida/output/*.gz  /user/liaoyida/output_2


#output_1.txt是三元组，用户ID、分类数(从0开始)、分解后user矩阵的值
#item_out.txt是分解后的item矩阵，有200行(分类数)，第一列的值是0-200，后面每一列是itemid
#矩阵是1个用户:1*200   乘以  200*100000得到每个用户对item的评分


