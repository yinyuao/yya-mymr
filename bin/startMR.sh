#!/bin/bash

# 函数用于获取配置文件内容并过滤掉注释
get_config_lines() {
    grep -v '^[[:space:]]*#' "$1"
}

# 配置参数
current_directory="$PWD"
local_jar_path="mymr-1.0.jar"  # 本地Jar包路径

# 上传Jar包到远程服务器
upload_jar_to_remote() {
    remote_server="$1"
    scp "$local_jar_path" "$remote_server:$current_directory"
}

# SSH远程连接并执行部署操作
remote_deploy() {
    remote_server="$1"
    ssh "$remote_server" <<EOF
    cd "$current_directory"  # 切换到远程服务器上的目标路径
    $2  # 执行传入的命令
EOF
}

# 读取配置文件内容
master_lines=$(get_config_lines master.conf)
urltopn_lines=$(get_config_lines urltopn.conf)

# 启动 Executor
while IFS= read -r slave_line; do
    data2=($slave_line)
    remote_server2="${data2[0]}"
    if [[ "$remote_server2" == "127.0.0.1" ]]; then
      nohup java -cp mymr-1.0.jar com.ksc.wordcount.worker.Executor $slave_line $master_lines > slave.log 2>&1 &
    else
      upload_jar_to_remote "$remote_server2"
      remote_deploy "$remote_server2" "nohup java -cp mymr-1.0.jar com.ksc.wordcount.worker.Executor $slave_line $master_lines > slave.log 2>&1 &"
    fi
done <<< "$(get_config_lines slave.conf)"

# 启动 WordCountDriver
data=($master_lines)
remote_server="${data[0]}"
if [[ "$remote_server" == "127.0.0.1" ]]; then
    nohup java -cp mymr-1.0.jar com.ksc.wordcount.driver.WordCountDriver $master_lines $urltopn_lines > master.log 2>&1 &
else
    upload_jar_to_remote "$remote_server"
    remote_deploy "$remote_server" "nohup java -cp mymr-1.0.jar com.ksc.wordcount.driver.WordCountDriver $master_lines $urltopn_lines > master.log 2>&1 &"
fi