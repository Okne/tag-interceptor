# tag-interceptor

## how to user
* mvn clean package
* copy tag-interceptor-1.0.jar from target dir to cluster under $FLUME_HOME/lib
* add interceptor configuration to flume agent conf file
```java 
agent.sources.source.interceptor = tag-interceptor
agent.sources.source.interceptor.tag-interceptor.type=com.epam.flume.TagInterceptor$Builder
agent.sources.source.interceptor.tag-interceptor.tagsHeader=withRealTags
agent.sources.source.interceptor.tag-interceptor.tagsFilePath=$PATH_TO_DICT_FILE
```
* split into separate channels base on **withRealTags** value
```java
agent.sources.source.selector.type = multiplexing
agent.sources.source.selector.header = withRealTags
agent.sources.source.selector.mapping.true = channel-1
agent.sources.source.selector.mapping.false = channel-2
agent.sources.source.selector.mapping.default = channel-2
```
