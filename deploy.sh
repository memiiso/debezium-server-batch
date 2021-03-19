#
# /*
#  * Copyright memiiso Authors.
#  *
#  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#  */
#

JAVA_HOME=$(/usr/libexec/java_home -v 11) || exit
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)" || exit
cd $DIR || exit
mvn clean install package -Passembly -Dquarkus.version=1.12.2.Final || exit
cd ./debezium-server-dist/target || exit
rm -rf debezium-server-batch
unzip debezium-server-dist-1.0.1-SNAPSHOT.zip
cd debezium-server-batch || exit
mv ./conf/application.properties.example ./conf/application.properties
bash ./run.sh
