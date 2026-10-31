param([switch]$Reset)

$KafkaHome = "C:\Users\kazie\kafka\kafka_2.13-4.3.2"
$Config = "$KafkaHome\config\aitu-server.properties"
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
