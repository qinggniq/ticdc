#!/bin/bash

set -e

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run() {
    rm -rf $WORK_DIR && mkdir -p $WORK_DIR

    start_tidb_cluster --workdir $WORK_DIR
    cd $WORK_DIR
    
    #sleep 10000
    # record tso before we create tables to skip the system table DDLs
    start_ts=$(run_cdc_cli tso query --pd=http://$UP_PD_HOST:$UP_PD_PORT)

    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
    TOPIC_NAME="canal"
    case $SINK_TYPE in
        kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4";;
        mysql) ;&
        *) SINK_URI="mysql://root@127.0.0.1:3333/";;
    esac

cat - >"$WORK_DIR/changefeed.toml" <<EOF
case-sensitive = false
[mounter]
worker-num = 4
[sink]
protocol = "canal"
dispatchers = [
	{matcher = ['*.*'], dispatcher = "table"},
]
EOF
    run_cdc_cli changefeed create --start-ts=$start_ts --config="$WORK_DIR/changefeed.toml" --sink-uri="$SINK_URI"
    run_sql_file $CUR/data/use.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql_file $CUR/data/use.sql ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    run_canal_adapter $TOPIC_NAME row_format
    sleep 100000
    run_sql_file $CUR/data/prepare.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    # sync_diff can't check non-exist table, so we check expected tables are created in downstream first
    check_table_exists row_format.multi_data_type ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"