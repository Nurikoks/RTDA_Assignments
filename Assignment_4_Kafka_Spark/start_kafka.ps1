# Start a single-node Kafka 4.3.2 broker (KRaft mode) on Windows.
#   .\start_kafka.ps1          start the broker with the existing data
#   .\start_kafka.ps1 -Reset   empty the data folder and format it again (clean run)
# The .bat scripts of Kafka fail with "The input line is too long", so java is called directly.
param([switch]$Reset)

$KafkaHome = "C:\Users\kazie\kafka\kafka_2.13-4.3.2"
$Config = "$KafkaHome\config\aitu-server.properties"   # copy of server.properties with
                                                         # log.dirs=C:/Users/kazie/kafka/data
                                                         # auto.create.topics.enable=false
$Data = "C:\Users\kazie\kafka\data"
$Cp = "$KafkaHome\libs\*"

if ($Reset -or -not (Test-Path "$Data\meta.properties")) {
    if (Test-Path $Data) { Remove-Item -Recurse -Force $Data }
    $id = (java -cp $Cp kafka.tools.StorageTool random-uuid | Select-Object -Last 1)
    java -cp $Cp "-Dlog4j2.configurationFile=$KafkaHome\config\tools-log4j2.yaml" `
        kafka.tools.StorageTool format --standalone -t $id -c $Config
}

java -Xmx1G -Xms1G -cp $Cp "-Dlog4j2.configurationFile=$KafkaHome\config\log4j2.yaml" `
    "-Dkafka.logs.dir=C:\Users\kazie\kafka\logs" kafka.Kafka $Config
