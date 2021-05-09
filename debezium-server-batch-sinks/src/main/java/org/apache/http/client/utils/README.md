# WORKAROUND FIX

WORKAROUND OF DEPENDENCY CONFLICT COPIED FROM HTTPCLIENT-4.5.13 FOLLOWING two dependencies are having conflict, hudi
bundle loads the class, but it's bundled with older version of httpclient library and
missing `formatSegments(java.lang.String[])` method

`for i in *.jar; do jar -tvf "$i" | grep -Hsi URLEncodedUtils && echo "$i"; done`

(standard input): 16562 Fri Jun 07 15:24:26 CEST 2019 org/apache/http/client/utils/URLEncodedUtils.class
httpclient-4.5.9.jar
(standard input): 13664 Tue Apr 06 09:09:52 CEST 2021 org/apache/http/client/utils/URLEncodedUtils.class
hudi-spark-bundle_2.12-0.8.0.jar

exception

```java
java.lang.RuntimeException:java.lang.NoSuchMethodError:'java.lang.String org.apache.http.client.utils.URLEncodedUtils.formatSegments(java.lang.String[])'
    Caused by:java.lang.NoSuchMethodError:'java.lang.String org.apache.http.client.utils.URLEncodedUtils.formatSegments(java.lang.String[])'
```