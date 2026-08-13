# Installs Kafka HTTP Gateway as a Windows service using Apache Procrun.
# Requires prunsrv.exe from the Apache Commons Daemon binaries:
# https://downloads.apache.org/commons/daemon/binaries/windows/

param(
    [string]$ServiceName = "kafka-gateway",
    [Parameter(Mandatory = $true)]
    [string]$InstallDir,
    [string]$WorkDir = "",
    [string]$LogDir = "",
    [string]$Prunsrv = "prunsrv.exe",
    [string]$JvmOpts = "-Xms256M;-Xmx2G",
    [string]$KafkaGatewayOpts = "-Dhost=127.0.0.1;-Dport=8086",
    [int]$StopTimeout = 120
)

if (-not $WorkDir) {
    $WorkDir = $InstallDir
}
if (-not $LogDir) {
    $LogDir = "$WorkDir\logs"
}
# PowerShell does not search the current directory for executables, so check the install directory explicitly.
if ($Prunsrv -eq "prunsrv.exe" -and (Test-Path (Join-Path $InstallDir "prunsrv.exe"))) {
    $Prunsrv = Join-Path $InstallDir "prunsrv.exe"
}
if (-not (Get-Command $Prunsrv -ErrorAction SilentlyContinue)) {
    Write-Error "'$Prunsrv' not found in '$InstallDir' or on PATH - specify -Prunsrv explicitly."
    exit 1
}

# Application log files go to the same directory as the Procrun service logs;
# an explicit -Dlog.dir in $KafkaGatewayOpts overrides this.
$JvmOpts = "$JvmOpts;-Dlog.dir=$LogDir"
if ($KafkaGatewayOpts) {
    # Procrun separates options with ';' or '#'.
    $JvmOpts = "$JvmOpts;$KafkaGatewayOpts"
}

$classPath = (Get-ChildItem "$InstallDir\lib\*.jar" -ErrorAction SilentlyContinue).FullName -join ';'
if (-not $classPath) {
    Write-Error "No jars found in $InstallDir\lib - install the distribution first."
    exit 1
}

& $Prunsrv "//IS//$ServiceName" `
    --DisplayName="Kafka Gateway" `
    --Description="Kafka HTTP Gateway" `
    --Startup=auto `
    --Jvm=auto `
    --Classpath="$classPath" `
    --StartMode=jvm --StartClass=pro.savel.kafka.Application --StartMethod=main `
    --StopMode=jvm --StopClass=pro.savel.kafka.Application --StopMethod=stop `
    --StopTimeout=$StopTimeout `
    --StartPath="$WorkDir" `
    "++JvmOptions=$JvmOpts" `
    --StdOutput=auto --StdError=auto `
    --LogPath="$LogDir"

if ($LASTEXITCODE -eq 0) {
    Write-Host "Service '$ServiceName' installed. Start it with: $Prunsrv //ES//$ServiceName"
}
else {
    exit $LASTEXITCODE
}
