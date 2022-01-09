#使用此项目更新骨架
***
#### 1.用idea打开此项目
#### 2.在项目中执行
    mvn archetype:create-from-project

#### 3.进入项目目录下\target\generated-sources\archetype，执行如下命令
    mvn install
  
#### 4.新建项目，在maven选项中添加新的骨架，注意各个属性对应填写，其中repository填写local
#### 5.测试：使用新骨架创建新项目查看是否生效
#### 6.未生效及解决方案： 
 - 未生效，多次创建后即可成功 
 - 手动到相关文件中添加自己的骨架
   
        该文件参考路径：
        C:\Users\sqh\AppData\Local\JetBrains\IntelliJIdea2021.1\Maven\Indices
   
        参考内容：
        <archetypes>
        <archetype groupId="com.mydataway" artifactId="mdw-flink-quickstart-archetype" version="1.0-SNAPSHOT" />
        <archetype groupId="com.mydataway" artifactId="mdw-flink-quickstart-archetype" version="1.1.1" repository="local" />
        </archetypes>
   