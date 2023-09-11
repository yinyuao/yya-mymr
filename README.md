# 分布式URL分析计算框架

## 背景

随着互联网的迅速发展，大型企业、广告公司、社交媒体平台和新闻门户网站需要了解用户的访问习惯和最常访问的URL。传统的单机计算方法已经无法满足处理如此庞大的访问日志数据的需求。因此，开发了一个分布式计算框架来解决这个问题，该框架具备足够的灵活性，以支持各种计算任务。

## 概述

要求实现一个分布式计算框架，其中包括支持“计算URL访问频率TOP N”的计算服务。该框架可以启动一个master进程和多个slave进程，并根据配置文件自动完成URL访问频率计算。此外，master进程还提供了Thrift服务接口，以便外部调用。

## 启动指南

1. 在工程根目录的 `bin/` 目录下，包含一个 `startMR.sh` 脚本，用于一键启动整个分布式计算框架及其相关服务。

## 打包规范

- 执行 `mvn clean package` 命令能够正常打包，生成的 JAR 包位于 `target/` 目录下，并命名为 `mymr-1.0.jar`，且为胖JAR，包含所有项目依赖。

## Shuffle 的目录

Shuffle的基础目录由 `System.getProperty("java.io.tmpdir")+"/shuffle"` 决定。Shuffle路径格式为：`base目录 + "/" + application_id`。例如，如果 `java.io.tmpdir` 值为 `/tmp` 且 `application_id` 为 `application_1234`，Shuffle目录则为 `/tmp/shuffle/application_1234`。

## 配置文件

配置文件需要放置在 `bin/` 目录下，包括以下文件：

- `master.conf`：配置master节点的IP、Akka端口、Thrift端口、内存等参数。
- `slave.conf`：配置slave节点的IP、Akka端口、RPC端口、内存、CPU等参数。
- `urltopn.conf`：配置URL访问频率TOP N计算的参数，包括`applicationId`、输入目录、输出目录、topN、过程reduceTask数、分片大小等。

## Thrift 服务IDL 文件

`urltopn.thrift` 文件包含了URL访问频率计算的Thrift服务接口，定义了请求和响应的结构体以及服务接口，具体内容见作业要求中的示例。

## 输入样例

包括多个`inputFileX.log`文件，每个文件包含了不同的访问日志。

## 输出结果样例

输出结果，按照访问频率排列URL，并显示其访问次数。

## 补充说明

- 执行 `startMR.sh` 脚本可以一键启动服务。
- 完成打包后，批改脚本会自动替换`thrift.executable`变量并重新打包。
- 作业要求不仅包括了基本功能的实现，还包括了加分项和扣分项的评分标准，可根据实际情况获得额外加分或扣分。

## 联系方式
如有任何问题或疑虑，请联系：

殷钰奥：[yinyuao@kingsoft.com]
项目仓库：[yuao yin / wk3-exam · GitLab
