#!/bin/bash

run_dir=`dirname "$0"`
run_dir=`cd "$run_dir"; cd ".."; pwd`

conf_dir="${run_dir}/conf"
dist_dir="${run_dir}/dist"

params="-Dtalos.shell.conf=${conf_dir}/talosShell.conf -Dtalos.shell.dist.path=${dist_dir}/ com.xiaomi.infra.galaxy.talos.shell.TalosAdminShell"
exec java -cp $run_dir/lib/galaxy-talos-shell-2.4-SNAPSHOT-jar-with-dependencies.jar ${params} $@
