#!/bin/bash
# resolve links - $0 may be a softlink
THIS="$0"
# copy mr config file

print_usage () 
{
    echo "Usage: Shell/run COMMOND"
    echo "where COMMAND is one of the follows:"
    echo "triple -i <input file path> -domain <domain's name> -o <output file path> 三元组抽取"
    echo "runWeiboData -i <input file path> -domain <domain's name>  在线跑微博数据，抽取三元组追加导入到bee数据库"
    echo "reloadMysqlDataCli -i <input file path> -domain <domain's name>  根据csv文件，清空DB，将csv数据导入到bee数据库t_triple表"
    echo "tripleRpc 三元组抽取Rpc服务"
    echo "tripleRpcClient -domain <domain:汽车、化妆品、母婴、数码> -i <input_testset_filename> 三元组抽取RPC服务测试客户端"
    echo "trainValidPair [-s <start line> -e <end line>] -o <output path> -type [fs|ss]  使用HBase中的数据训练FS Candidate or SS Pair"
    echo "trainFSDistance [-s <start line> -e <end line>] -o <output path> 使用HBase中的微博数据训练Feature和Sentiment的位置关系模型"
    echo "extr -i <input file> -o <output file> -domain <product domain> -func [pair|feature] 从文件中抽取待选的搭配或者特征"
    echo "genWord2VecData -s <start line> -e <end line> -o <output path> 从HBase中产生word2vec的训练数据"
    echo "word2vec -i <input file> -o <output file> -mdl <model path> -gen 查询给定词语的 vector 输出到文件中"
    exit 1
}

if [ $# = 0 ] || [ $1 = "help" ]; then
  print_usage
fi


# get arguments
COMMAND=$1
shift

# some directories
THIS_DIR=`dirname "$THIS"`
CLASSIFIER_HOME=`cd "$THIS_DIR/.." ; pwd`

JAVA=$JAVA_HOME/bin/java
JAVA_HEAP_MAX=-Xmx3000m

CLASSPATH=${CLASSPATH}:$JAVA_HOME/lib/tools.jar

if [ -d "$CLASSIFIER_HOME/target/classes" ]; then
  CLASSPATH=${CLASSPATH}:$CLASSIFIER_HOME/target/classes
fi

for f in $CLASSIFIER_HOME/target/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

# add libs to CLASSPATH
for f in $CLASSIFIER_HOME/target/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

# figure out which class to run
if [ "$COMMAND" = "trainGlobalModel" ]; then
    CLASS=com.yeezhao.commons.triple.tools.TrainGlobalModel
elif [ "$COMMAND" = "trainValidPair" ]; then
    CLASS=com.yeezhao.commons.triple.tools.GenValidFeatureSentimentPair
elif [ "$COMMAND" = "triple" ]; then
    CLASS=com.yeezhao.commons.triple.cli.TripCli
elif [ "$COMMAND" = "tripleRpc" ]; then
    CLASS=com.yeezhao.commons.triple.cli.TripleRPCServerCli
elif [ "$COMMAND" = "tripleRpcClient" ]; then
    CLASS=com.yeezhao.commons.triple.rpc.TripleRPCClient
elif [ "$COMMAND" = "trainFSDistance" ]; then
    CLASS=com.yeezhao.commons.triple.tools.TrainFSDistanceModel
elif [ "$COMMAND" = "extr" ]; then
    CLASS=com.yeezhao.commons.triple.tools.ExtractProductInfo
elif [ "$COMMAND" = "runWeiboData" ]; then
    CLASS=com.yeezhao.commons.triple.cli.RunWeiboDataCli
elif [ "$COMMAND" = "reloadMysqlDataCli" ]; then
    CLASS=com.yeezhao.commons.triple.cli.ReloadMysqlDataCli
elif [ "$COMMAND" = "genWord2VecData" ]; then
    CLASS=com.yeezhao.commons.triple.tools.GenWord2VecTrainData
elif [ "$COMMAND" = "word2vec" ]; then
    CLASS=com.yeezhao.commons.triple.tools.Word2VecTools
else
	print_usage
fi


# run it
params=$@
"$JAVA" -Dfile.encoding=UTF-8 $JAVA_HEAP_MAX -classpath "$CLASSPATH" $CLASS $params
