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
mvn verify -DskipITs=true -Dquarkus.version=1.12.2.Final
