#!/usr/bin/env bash

# set -e

if [ $# -ne 1 ]; then
    echo "You must pass the bench type as parameters"
    exit 1
fi

BENCH_TYPE=$1

NUMBER_OF_RUNS=2
TIMEOUT=120
CALVIN_CMD="./bin/deployment/cluster  -c dist-deploy.conf -p src/deployment/portfile -d bin/deployment/db $BENCH_TYPE 0"

# declare -a means
means=(8.2 8.0)

# for i in $(seq 1 $NUMBER_OF_RUNS); do
    # timeout --foreground $TIMEOUT $CALVIN_CMD

    # partition_count=0
    # latency_sum=0
    # for file in *output.txt; do
        # if [[ -s $file ]]; then
            # partition_count=$(($partition_count + 1))
            # latency=$(grep -A 1 '^LATENCY$' $file | tail -n 1 | grep -oP '^([^,]+)')
            # latency_sum=$(($latency_sum + $latency))
        # fi
    # done

    # mean=`python3 -c "print($latency_sum/$partition_count)"`
    # echo ""
    # echo "run $i mean: $mean"
    # echo ""
    # means[$i]=$mean
# done


total_mean=0
for mean in ${means[@]}; do
    total_mean=`python -c "print($total_mean+$mean)"`
done

total_mean=$(python -c "print($total_mean/$NUMBER_OF_RUNS)")

# stdev=$(echo "${means[@]}" | awk 'NF {sum=0;ssq=0;for (i=1;i<=NF;i++){sum+=$i;ssq+=$i**2}; print (ssq/NF-(sum/NF)**2)**0.5}')
stdev=`python <<END
import statistics
import sys

data = [ float(x) for x in "${means[@]}".split(' ') ]
print(statistics.stdev(data))
END`

echo ""
echo "means: ${means[@]}"
echo "total mean: $total_mean, standard deviation: $stdev"
