# 使用此项目更新骨架

***

flink版本：1.13.6 scala 2.12

#### 当前pom:

```shell
    <groupId>com.sqh</groupId>
    <artifactId>flink-quickstart-archetype</artifactId>
    <version>2.0.0</version>
```

#### 更多连接器:

1. [apache-flink-connector子项目,包含多个连接器实现](https://github.com/dahai1996/bahir-flink)
2. [flink-cdc](https://github.com/ververica/flink-cdc-connectors)
3. [flink-connector-redis](https://github.com/dahai1996/flink-connector-lettuce-redis)
4. [doris-flink-connector](https://github.com/apache/doris-flink-connector)
5. 其他

***

#### 1.用idea打开此项目

#### 2.在项目中执行

    mvn archetype:create-from-project

#### 3.进入项目目录下\target\generated-sources\archetype，执行如下命令

    mvn install

#### 4.新建项目，在maven选项中添加新的骨架，注意各个属性对应填写，其中repository填写local

```shell
新版本idea中不需要填写 repository ,需要填写目录或者url.
对此,我们执行了步骤3得 mvn install 成功,可以不填写目录或者url这一栏.
```

#### 5.测试：使用新骨架创建新项目查看是否生效

#### 6.未生效及解决方案：

- 未生效，多次创建后即可成功
- 手动到相关文件中添加自己的骨架

       该文件参考路径：
       C:\Users\sqh\AppData\Local\JetBrains\IntelliJIdea2021.1\Maven\Indices

       参考内容：
       <archetypes>
       <archetype groupId="com.sqh" artifactId="flink-quickstart-archetype" version="1.0-SNAPSHOT" />
       <archetype groupId="com.sqh" artifactId="flink-quickstart-archetype" version="1.1.1" repository="local" />
       </archetypes>
   