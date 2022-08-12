#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

#echo "--- Generate RiseDev CI config"
#cp ci/risedev-components.ci.benchmark.env risedev-components.user.env
#
#echo "--- Download necessary tools"
#apt-get -y install golang-go librdkafka-dev
#cargo make pre-start-benchmark
#
#echo "--- Start a full risingwave cluster"
#./risedev clean-data && ./risedev d full-benchmark
#
#echo "--- Cluter label:"
#cat .risingwave/config/prometheus.yml |grep rw_cluster
#
#echo "--- Clone tpch-bench repo"
#git clone https://"$GITHUB_TOKEN"@github.com/singularity-data/tpch-bench.git
#
#echo "--- Run tpc-h bench"
#cd tpch-bench/
#./scripts/build.sh
#./scripts/launch.sh

function polling() {
    set +e
    try_times=30
    while :; do
        echo "tenant end: $@"
        if [ $try_times == 0 ]; then
            echo "❌ ERROR: polling timeout"
            exit 1
        fi
        psql "$@" -c '\q'
        if [ $? == 0 ]; then
            echo "✅ Instance Ready"
            break
        fi
        sleep 10
        try_times=$((try_times - 1))
    done
    set -euo pipefail
}

function cleanup {
  echo "--- delete tenant"
  rwc t delete -tenant ${TENANT_NAME}
}

trap cleanup EXIT

echo "--- echo info"
echo ${TENANT_NAME}
echo ${HOST_IP}
psql --version

mkdir ~/risingwave-deploy

echo "--- Generate RiseDev CI config"
cp ci/risedev-components.ci.benchmark.env risedev-components.user.env

echo "--- Download necessary tools"
apt-get -y install golang-go librdkafka-dev

echo "--- Download cloud cli tool"
curl -L https://rwc-cli-internal-release.s3.ap-southeast-1.amazonaws.com/download.sh | bash && mv rwc /usr/local/bin

echo "--- rwc config -region"
rwc config -region ap-southeast-1
rwc config ls

echo "--- rwc login -account"
rwc login -account benchmark -password "$BENCH_TOKEN"

echo "--- rwc create a sing node risingwave instance"
rwc t create -tenant ${TENANT_NAME} -sku SingleNodeBench

sleep 2

echo "--- wait instance ready "
endpoint=$(rwc t get-endpoint -tenant ${TENANT_NAME})
polling ${endpoint}

echo "--- rwc get endpoint"
echo "--frontend-url ${endpoint}" > ~/risingwave-deploy/tpch-bench-args-frontend
echo "--kafka-addr ${HOST_IP}:29092" >  ~/risingwave-deploy/tpch-bench-args-kafka
cat ~/risingwave-deploy/tpch-bench-args-frontend
cat ~/risingwave-deploy/tpch-bench-args-kafka

exit 1

echo "--- Clone tpch-bench repo"
git clone https://"$GITHUB_TOKEN"@github.com/singularity-data/tpch-bench.git

echo "--- Run tpc-h bench"
cd tpch-bench/
./scripts/build.sh
./scripts/launch_risedev_bench.sh

#sleep 2

#echo "--- delete tenant"
#rwc t delete -tenant ${tenantname}