# RealtimeCalculate
SparkStreaming实时计算Demo

程序功能说明：从kafka接收数据，以5秒的间隔不断进行微批处理，统计以空格为分隔的单词数量。

运行时在命令行输入 `nc -lk 9999 `，即可模拟流式输入