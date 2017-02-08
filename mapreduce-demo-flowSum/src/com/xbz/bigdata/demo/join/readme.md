这个Demo是使用MapReduce实现SQL的join
--------------
假设我们有两张表：部门表和员工表。他们分别放于department.data和employee.data文件

department.data文件
主键(id)			部门名称(d_name)		
1					研发部
2					测试部
3					人力资源部

employee.data文件
主键(id)			姓名(e_name)	部门(d_id)
1					张三				1
2					李四				1
3					王五				2
4					赵六				3
5					许七				3


最终的执行结果应该为
员工主键			员工姓名		部门名称
1					张三			研发部
2					李四			研发部
3					王五			测试部
4					赵六			人力资源部
5					许七			人力资源部

定义文件的分隔符为","

测试数据
部门
1,研发部
2,测试部
3,人力资源部


员工
1,张三,1
2,李四,1
3,王五,2
4,赵六,3
5,许七,3



hadoop -jar join.jar com.xbz.bigdata.demo.join.JoinJob /test/join/in /test/join/out/0